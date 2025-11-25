#!/usr/bin/env python3
"""
생명공학육성시행계획 데이터 처리 시스템 - 메인 프로그램
PDF → JSON → 정규화 → Oracle DB 적재 파이프라인

사용법:
    python main.py --batch            # 전체 파이프라인 실행 (권장)
    python main.py document.pdf       # 특정 PDF 파일만 처리
    python main.py --skip-db          # DB 적재 건너뛰기
    python main.py --workers 8        # 병렬 처리 워커 수 지정
"""

import sys
import io
import json
from pathlib import Path
import logging
from datetime import datetime
from typing import List
import argparse

# UTF-8 출력 설정 (Windows cp949 에러 방지)
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# 핵심 모듈
from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from load_oracle_direct import OracleDirectLoader
from config import (
    ORACLE_CONFIG,
    ORACLE_CONFIG_DEV,
    INPUT_DIR,
    OUTPUT_DIR,
    NORMALIZED_OUTPUT_GOVERNMENT_DIR
)

# 배치 처리
try:
    from batch_processor import BatchPDFProcessor, create_pdf_processor_func
    BATCH_AVAILABLE = True
except ImportError:
    BATCH_AVAILABLE = False
    BatchPDFProcessor = None
    create_pdf_processor_func = None

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PDFtoDBPipeline:
    
    def __init__(self,
                 skip_db: bool = False,
                 batch_mode: bool = False,
                 batch_size: int = 10,
                 max_workers: int = 5
                 ):
        """
        Args:
            skip_db: DB 적재 건너뛰기
            batch_mode: 배치 처리 모드
            batch_size: 배치당 파일 수
            max_workers: 병렬 작업자 수
        """
        self.skip_db = skip_db
        self.batch_mode = batch_mode
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # 디렉토리 설정 (config에서 가져옴)
        self.input_dir = INPUT_DIR
        self.output_dir = OUTPUT_DIR
        self.normalized_dir = NORMALIZED_OUTPUT_GOVERNMENT_DIR

        # 통계
        self.stats = {
            'start_time': datetime.now(),
            'pdf_files': [],
            'processed': 0,
            'failed': 0,
            'total_records': 0,
            'db_loaded': False,
            'matched': 0,
            'unmatched': 0,
            'diff_found': 0
        }
    
    def clean_previous_data(self):
        """이전 실행 데이터 정리 (JSON, CSV)"""
        logger.info("\n" + "="*80)
        logger.info("🧹 이전 데이터 정리 중...")
        logger.info("="*80)

        cleaned_items = []

        # 1. Output JSON 파일 삭제
        json_files = list(self.output_dir.glob("*.json"))
        if json_files:
            for file in json_files:
                try:
                    file.unlink()
                    cleaned_items.append(f"JSON: {file.name}")
                except Exception as e:
                    logger.warning(f"파일 삭제 실패 {file}: {e}")

        # 2. 정규화된 CSV 파일 삭제
        csv_files = list(self.normalized_dir.glob("*.csv"))
        if csv_files:
            for file in csv_files:
                try:
                    file.unlink()
                    cleaned_items.append(f"CSV: {file.name}")
                except Exception as e:
                    logger.warning(f"파일 삭제 실패 {file}: {e}")

        # 결과 출력
        if cleaned_items:
            logger.info(f"✅ 총 {len(cleaned_items)}개 항목 정리 완료:")
            for item in cleaned_items[:10]:  # 처음 10개만 출력
                logger.info(f"   - {item}")
            if len(cleaned_items) > 10:
                logger.info(f"   ... 외 {len(cleaned_items) - 10}개")
        else:
            logger.info("✅ 삭제할 이전 데이터가 없습니다")

        logger.info("")

    def process_pdf(self, pdf_path: Path) -> bool:
        """단일 PDF 처리"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"📄 처리 중: {pdf_path.name}")
            logger.info(f"{'='*60}")
            
            # 1. PDF → JSON
            logger.info("1️⃣ PDF → JSON 변환")
            json_data = extract_pdf_to_json(str(pdf_path), str(self.output_dir))
            
            if not json_data:
                logger.error("JSON 변환 실패")
                return False
            
            # JSON 파일 저장
            json_file = self.output_dir / f"{pdf_path.stem}.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"   ✅ JSON 생성: {json_file.name}")
            
            # 2. JSON → 정규화 (DB 연결 전달하여 PLAN_ID 매칭)
            logger.info("2️⃣ 데이터 정규화")

            # Oracle DB 연결 (PLAN_ID 매칭용)
            from oracle_db_manager import OracleDBManager
            db_manager = None
            if not self.skip_db:
                try:
                    db_manager = OracleDBManager(ORACLE_CONFIG)
                    db_manager.connect()
                    logger.info("   🔗 DB 연결 (PLAN_ID 매칭용)")
                except Exception as e:
                    logger.warning(f"   ⚠️ DB 연결 실패 (신규 PLAN_ID로 생성): {e}")
                    db_manager = None

            normalizer = GovernmentStandardNormalizer(
                str(json_file),
                str(self.normalized_dir),
                db_manager=db_manager
            )

            if not normalizer.normalize(json_data):
                logger.error("정규화 실패")
                if db_manager:
                    db_manager.close()
                return False
            
            normalizer.save_to_csv()
            normalizer.print_statistics()
            
            # DB 연결 종료
            if db_manager:
                db_manager.close()

            # 통계 업데이트
            for table_name, records in normalizer.data.items():
                if isinstance(records, list):
                    self.stats['total_records'] += len(records)
            
            logger.info(f"   ✅ 정규화 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"처리 실패: {e}")
            return False
    
    def process_batch_mode(self, pdf_files: List[str] = None) -> bool:
        """대량 배치 처리 모드"""
        if not BATCH_AVAILABLE:
            logger.error("배치 처리 모듈을 사용할 수 없습니다. batch_processor.py를 확인하세요.")
            return False
        
        try:
            logger.info("\n" + "="*60)
            logger.info("🚀 대량 배치 처리 모드")
            logger.info("="*60)
            
            # 1. PDF → JSON (배치 처리)
            logger.info("1️⃣ PDF → JSON 변환 (병렬 처리)")
            
            processor = BatchPDFProcessor(
                input_dir=str(self.input_dir),
                output_dir=str(self.output_dir),
                batch_size=self.batch_size,
                max_workers=self.max_workers,
                use_multiprocessing=False  # 멀티스레딩 사용
            )
            
            pdf_processor_func = create_pdf_processor_func(str(self.output_dir))
            
            summary = processor.process_all(
                pdf_processor_func,
                recursive=False,
                save_results=True
            )
            
            processor.print_summary()
            
            if summary['processed'] == 0:
                logger.error("처리된 파일이 없습니다.")
                return False
            
            # 통계 업데이트
            self.stats['processed'] = summary['processed']
            self.stats['failed'] = summary['failed']
            
            # 2. JSON → 정규화 (모든 파일 누적)
            logger.info("\n2️⃣ 데이터 정규화")
            
            json_files = list(self.output_dir.glob("*.json"))
            json_files = [f for f in json_files if not f.name.startswith('batch_')]
            
            logger.info(f"정규화할 JSON 파일: {len(json_files)}개")
            
            # 전체 JSON 데이터 수집
            all_json_data = []
            for i, json_file in enumerate(json_files, 1):
                if i % 50 == 0:
                    logger.info(f"  JSON 로드 중: [{i}/{len(json_files)}]")

                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                        all_json_data.append(json_data)
                except Exception as e:
                    logger.error(f"JSON 로드 실패 {json_file.name}: {e}")

            logger.info(f"✅ {len(all_json_data)}개 JSON 로드 완료")

            # 모든 데이터를 한 번에 정규화 (파일별로 연도 추출)
            if all_json_data:
                logger.info("모든 데이터 통합 정규화 시작...")

                # Oracle DB 연결 (PLAN_ID 매칭용)
                from oracle_db_manager import OracleDBManager
                db_manager = None
                if not self.skip_db:
                    try:
                        db_manager = OracleDBManager(ORACLE_CONFIG)
                        db_manager.connect()
                        logger.info("   🔗 DB 연결 (PLAN_ID 매칭용)")
                    except Exception as e:
                        logger.warning(f"   ⚠️ DB 연결 실패 (신규 PLAN_ID로 생성): {e}")
                        db_manager = None

                # 첫 번째 파일로 normalizer 초기화
                normalizer = GovernmentStandardNormalizer(
                    str(json_files[0]),
                    str(self.normalized_dir),
                    db_manager=db_manager
                )

                # 각 JSON 파일별로 처리 (연도 추출 포함)
                for json_file, json_data in zip(json_files, all_json_data):
                    # 파일마다 연도를 추출하여 컨텍스트 업데이트
                    import re
                    filename = json_file.stem
                    year_match = re.search(r'(20\d{2})', filename)

                    if year_match:
                        doc_year = int(year_match.group(1))
                        logger.info(f"📅 {filename} -> {doc_year}년도 데이터 처리 중...")

                        # 연도별로 컨텍스트 업데이트
                        normalizer.current_context['document_year'] = doc_year
                        normalizer.current_context['performance_year'] = doc_year - 1
                        normalizer.current_context['plan_year'] = doc_year

                    normalizer.normalize(json_data)

                # 한 번에 CSV 저장
                normalizer.save_to_csv()
                normalizer.print_statistics()

                # DB 연결 종료
                if db_manager:
                    db_manager.close()
                    logger.info("   🔌 DB 연결 종료")

                # 통계
                for table_name, records in normalizer.data.items():
                    if isinstance(records, list):
                        self.stats['total_records'] += len(records)

                logger.info(f"✅ 정규화 완료: 총 {self.stats['total_records']:,}건")
            else:
                logger.error("정규화할 데이터가 없습니다.")
                return False

            return True
            
        except Exception as e:
            logger.error(f"배치 처리 실패: {e}")
            return False
    
    def load_to_database(self) -> bool:
        """3단계: Oracle 데이터베이스 적재 (매칭 기반)"""
        if self.skip_db:
            logger.info("\n⏭️ DB 적재 건너뜀")
            return True

        try:
            logger.info("\n" + "="*80)
            logger.info("3️⃣ Oracle 데이터베이스 적재")
            logger.info("="*80)

            # Oracle 적재 (매칭 기반)
            # ORACLE_CONFIG: TB_PLAN_DATA 읽기 (매칭용)
            # ORACLE_CONFIG_DEV: 하위 테이블 쓰기 (적재용)
            oracle_loader = OracleDirectLoader(
                db_config_read=ORACLE_CONFIG,
                db_config_write=ORACLE_CONFIG_DEV,
                csv_dir=str(self.normalized_dir)
            )
            oracle_loader.connect()

            logger.info("\n📋 파이프라인 흐름:")
            logger.info("   1️⃣ BICS.TB_PLAN_DATA 조회 (기존 레코드 - 매칭용)")
            logger.info("   2️⃣ CSV와 매칭 (YEAR + BIZ_NM + DETAIL_BIZ_NM 기준)")
            logger.info("   3️⃣ 매칭 성공 → 기존 PLAN_ID 재사용")
            logger.info("   4️⃣ 매칭 실패 → 신규 레코드로 표시")
            logger.info("   5️⃣ 하위 테이블 적재 → BICS_DEV 스키마 (TB_PLAN_BUDGET, SCHEDULE, PERFORMANCE, ACHIEVEMENTS)")

            # 매칭 기반 적재 실행
            oracle_loader.load_with_matching()

            # 통계 출력
            stats = oracle_loader.load_stats

            logger.info("\n" + "="*80)
            logger.info("📊 적재 완료 통계")
            logger.info("="*80)
            logger.info(f"✅ 총 적재 레코드: {stats['total_records']:,}건")
            logger.info(f"\n📌 매칭 결과:")
            logger.info(f"   • 매칭 성공: {stats['matched']}건 (기존 PLAN_ID 재사용)")
            logger.info(f"   • 매칭 실패: {stats['unmatched']}건 (신규 레코드)")
            logger.info(f"   • 차이점 발견: {stats['diff_found']}건 (내용 불일치)")

            if stats['unmatched'] > 0:
                logger.warning(f"\n⚠️  매칭 실패 {stats['unmatched']}건은 신규 내역사업으로 추정됩니다.")
                logger.warning("   → 매칭 리포트를 확인하여 수동 처리가 필요할 수 있습니다.")

            if stats['diff_found'] > 0:
                logger.warning(f"\n⚠️  차이점 발견 {stats['diff_found']}건은 내용이 변경된 사업입니다.")
                logger.warning("   → 업데이트 여부를 검토해주세요.")

            oracle_loader.close()

            self.stats['db_loaded'] = True
            self.stats['matched'] = stats['matched']
            self.stats['unmatched'] = stats['unmatched']
            self.stats['diff_found'] = stats['diff_found']

            return True

        except Exception as e:
            logger.error(f"❌ Oracle DB 적재 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            logger.warning("⚠️ Oracle 적재 실패했지만 계속 진행합니다.")
            return False

    def run(self, pdf_files: List[str] = None):
        """파이프라인 실행"""
        logger.info("\n" + "="*80)
        logger.info("🚀 PDF to Database 파이프라인 시작")
        logger.info("="*80)
        
        # 이전 데이터 정리
        self.clean_previous_data()

        success = False
        
        try:
            # 배치 처리 모드
            if self.batch_mode:
                success = self.process_batch_mode(pdf_files)
            
            # 일반 PDF 처리 모드
            else:
                # PDF 파일 찾기
                if pdf_files:
                    pdf_list = [Path(f) for f in pdf_files if Path(f).exists()]
                else:
                    # input 폴더에서 모든 PDF 찾기
                    pdf_list = list(self.input_dir.glob("*.pdf"))
                
                if not pdf_list:
                    logger.error("❌ 처리할 PDF 파일이 없습니다!")
                    logger.error(f"   '{self.input_dir}' 폴더에 PDF 파일을 넣어주세요.")
                    return False

                # 각 PDF 처리
                for pdf_path in pdf_list:
                    self.stats['pdf_files'].append(pdf_path.name)

                    if self.process_pdf(pdf_path):
                        self.stats['processed'] += 1
                    else:
                        self.stats['failed'] += 1

                success = self.stats['processed'] > 0

            # DB 적재
            if success and not self.skip_db:
                self.load_to_database()

            # 최종 통계 출력
            logger.info("\n" + "="*80)
            logger.info("📊 처리 결과")
            logger.info("="*80)
            logger.info(f"성공: {self.stats['processed']}개")
            logger.info(f"실패: {self.stats['failed']}개")
            logger.info(f"총 레코드: {self.stats['total_records']:,}건")
            logger.info(f"DB 적재: {'✅' if self.stats['db_loaded'] else '⏭️ 건너뜀'}")

            if self.stats['db_loaded']:
                logger.info(f"\n📌 매칭 결과:")
                logger.info(f"   • 매칭 성공: {self.stats.get('matched', 0)}건")
                logger.info(f"   • 매칭 실패: {self.stats.get('unmatched', 0)}건")
                logger.info(f"   • 차이점 발견: {self.stats.get('diff_found', 0)}건")

        except Exception as e:
            logger.error(f"파이프라인 오류: {e}")
            success = False
        
        # 완료 메시지
        if success:
            print("\n" + "="*80)
            print("✅ 파이프라인 성공적으로 완료!")
            print("="*80)
        else:
            print("\n" + "="*80)
            print("⚠️ 파이프라인 일부 실패")
            print("="*80)
        
        return success


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='PDF 처리 시스템',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python main.py --batch            # 전체 파이프라인 실행
  python main.py doc1.pdf           # 특정 PDF 파일 처리
  python main.py --skip-db          # DB 적재 건너뛰기
  python main.py --workers 8        # 병렬 처리 워커 수 지정
        """
    )
    
    parser.add_argument(
        'pdf_files',
        nargs='*',
        help='처리할 PDF 파일 경로 (생략하면 input 폴더 검색)'
    )
    
    parser.add_argument(
        '--skip-db',
        action='store_true',
        help='데이터베이스 적재 건너뛰기'
    )
    
    parser.add_argument(
        '--batch',
        action='store_true',
        help='배치 처리 모드 (병렬 처리)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='배치당 파일 수 (기본값: 10)'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=5,
        help='병렬 작업자 수 (기본값: 5)'
    )
    
    args = parser.parse_args()
    
    # 배치 모드 체크
    if args.batch and not BATCH_AVAILABLE:
        print("⚠️ 배치 처리 모듈을 사용할 수 없습니다.")
        print("다음 패키지를 설치하세요: pip install tqdm")
        return 1
    
    # 파이프라인 실행
    pipeline = PDFtoDBPipeline(
        skip_db=args.skip_db,
        batch_mode=args.batch,
        batch_size=args.batch_size,
        max_workers=args.workers
    )
    
    success = pipeline.run(args.pdf_files)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
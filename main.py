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
import json
from pathlib import Path
import logging
from datetime import datetime
from typing import List
import argparse

# 핵심 모듈
from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from load_oracle_db import OracleDBLoader
from config import ORACLE_CONFIG

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
                 max_workers: int = 4
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
        
        # 디렉토리 설정
        self.input_dir = Path("input")
        self.output_dir = Path("output")
        self.normalized_dir = Path("normalized_output_government")
        self.report_dir = Path("reports")
        
        # 디렉토리 생성
        for dir_path in [self.input_dir, self.output_dir, self.normalized_dir, self.report_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 통계
        self.stats = {
            'start_time': datetime.now(),
            'pdf_files': [],
            'processed': 0,
            'failed': 0,
            'total_records': 0,
            'db_loaded': False
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
            
            # 2. JSON → 정규화
            logger.info("2️⃣ 데이터 정규화")
            normalizer = GovernmentStandardNormalizer(str(json_file), str(self.normalized_dir))
            
            if not normalizer.normalize(json_data):
                logger.error("정규화 실패")
                return False
            
            normalizer.save_to_csv()
            normalizer.print_statistics()
            
            # 통계 업데이트
            for table_name, records in normalizer.data.items():
                if isinstance(records, list):
                    self.stats['total_records'] += len(records)
            
            logger.info(f"   ✅ 정규화 완료")
            
            return True
            
        except Exception as e:
            logger.error(f"처리 실패: {e}")
            return False
    
    def process_sample(self) -> bool:
        """샘플 데이터 처리"""
        try:
            logger.info("\n" + "="*60)
            logger.info("🧪 샘플 데이터 모드")
            logger.info("="*60)
            
            # 1. 샘플 JSON 생성
            logger.info("1️⃣ 샘플 데이터 생성")
            json_data = extract_pdf_to_json(None, str(self.output_dir))
            
            # JSON 저장
            json_file = self.output_dir / "sample_data.json"
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            # 2. 정규화
            logger.info("2️⃣ 데이터 정규화")
            normalizer = GovernmentStandardNormalizer(str(json_file), str(self.normalized_dir))
            normalizer.normalize(json_data)
            normalizer.save_to_csv()
            normalizer.print_statistics()
            
            # 통계
            for table_name, records in normalizer.data.items():
                if isinstance(records, list):
                    self.stats['total_records'] += len(records)
            
            return True
            
        except Exception as e:
            logger.error(f"샘플 처리 실패: {e}")
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
                use_multiprocessing=False  # 멀티스레딩 사용 (안정성)
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

                # 첫 번째 파일로 normalizer 초기화
                normalizer = GovernmentStandardNormalizer(
                    str(json_files[0]),
                    str(self.normalized_dir)
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
        """3단계: Oracle 데이터베이스 적재"""
        if self.skip_db:
            logger.info("\n⏭️ DB 적재 건너뜀")
            return True

        try:
            logger.info("\n" + "="*60)
            logger.info("3️⃣ Oracle 데이터베이스 적재")
            logger.info("="*60)

            # Oracle 적재
            oracle_loader = OracleDBLoader(ORACLE_CONFIG, str(self.normalized_dir))
            oracle_loader.connect()

            # 테이블 생성 (존재하지 않을 경우)
            oracle_loader.create_tables()

            # 데이터 적재
            oracle_loader.load_all_tables()

            oracle_loader.close()

            self.stats['db_loaded'] = True
            logger.info(f"   ✅ Oracle DB 적재 완료: {oracle_loader.load_stats['total_records']:,}건")

            return True

        except Exception as e:
            logger.error(f"Oracle DB 적재 실패: {e}")
            logger.warning("⚠️ Oracle 적재 실패했지만 계속 진행합니다.")
            return False

    def generate_report(self):
        """최종 보고서 생성"""
        report = []
        report.append("="*80)
        report.append("📊 PDF to Database 처리 보고서")
        report.append("="*80)
        report.append(f"실행 시간: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"소요 시간: {(datetime.now() - self.stats['start_time']).total_seconds():.1f}초")
        report.append("")
        
        if self.stats['pdf_files']:
            report.append("📄 처리된 파일:")
            for pdf in self.stats['pdf_files']:
                report.append(f"  - {pdf}")
        
        report.append("")
        report.append("📊 처리 결과:")
        report.append(f"  • 성공: {self.stats['processed']}개")
        report.append(f"  • 실패: {self.stats['failed']}개")
        report.append(f"  • 총 레코드: {self.stats['total_records']:,}건")
        report.append(f"  • DB 적재: {'✅' if self.stats['db_loaded'] else '⏭️ 건너뜀'}")
        report.append("")
        
        # 생성된 파일
        report.append("📁 생성된 파일:")
        report.append(f"  • JSON: {self.output_dir}/*.json")
        report.append(f"  • CSV: {self.normalized_dir}/*.csv")
        if self.stats['db_loaded']:
            report.append(f"  • DB: government_standard database")
        
        report.append("")
        report.append("="*80)
        
        # 보고서 저장
        report_text = "\n".join(report)
        report_file = self.report_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        # 콘솔 출력
        print("\n" + report_text)
        
        return report_file
    
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
                    logger.warning("PDF 파일이 없습니다. 샘플 데이터 모드로 전환...")
                    success = self.process_sample()
                    self.stats['processed'] = 1 if success else 0
                else:
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

            # 보고서 생성
            report_file = self.generate_report()
            logger.info(f"\n📄 보고서 생성: {report_file}")
            
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
        description='생명공학육성시행계획 PDF 처리 시스템',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python main.py --batch            # 전체 파이프라인 실행 (권장)
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
        default=4,
        help='병렬 작업자 수 (기본값: 4)'
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
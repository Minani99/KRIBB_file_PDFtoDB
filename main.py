#!/usr/bin/env python3
"""
생명공학육성시행계획 데이터 처리 시스템 - 메인 프로그램
PDF → JSON → 정규화 → Oracle DB 적재 파이프라인

사용법:
    python main.py                    # input 폴더의 모든 PDF 처리
    python main.py document.pdf       # 특정 PDF 파일만 처리
    python main.py --skip-db          # DB 적재 건너뛰기 (CSV만 생성)
"""

import sys
import io
import json
import re
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
from config import (
    INPUT_DIR,
    OUTPUT_DIR,
    NORMALIZED_OUTPUT_GOVERNMENT_DIR
)

# DB 모듈 (선택적)
try:
    from load_oracle_direct import OracleDirectLoader
    from oracle_db_manager import OracleDBManager
    from config import ORACLE_CONFIG, ORACLE_CONFIG_DEV
    DB_AVAILABLE = True
except ImportError as e:
    DB_AVAILABLE = False
    print(f"⚠️ DB 모듈 로드 실패 (CSV만 생성): {e}")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PDFtoDBPipeline:
    """PDF → DB 파이프라인"""
    
    def __init__(self, skip_db: bool = False):
        """
        Args:
            skip_db: DB 적재 건너뛰기
        """
        self.skip_db = skip_db or not DB_AVAILABLE
        
        # 디렉토리 설정
        self.input_dir = Path(INPUT_DIR)
        self.output_dir = Path(OUTPUT_DIR)
        self.normalized_dir = Path(NORMALIZED_OUTPUT_GOVERNMENT_DIR)

        # 디렉토리 생성
        self.input_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        self.normalized_dir.mkdir(exist_ok=True)

        # 통계
        self.stats = {
            'start_time': datetime.now(),
            'pdf_files': [],
            'processed': 0,
            'failed': 0,
            'total_records': 0,
            'db_loaded': False,
            'matched': 0,
            'unmatched': 0
        }
    
    def clean_previous_data(self):
        """이전 실행 데이터 정리"""
        logger.info("=" * 80)
        logger.info("🧹 이전 데이터 정리 중...")
        logger.info("=" * 80)

        cleaned = 0

        # JSON 파일 삭제
        for file in self.output_dir.glob("*.json"):
            try:
                file.unlink()
                cleaned += 1
            except Exception as e:
                logger.warning(f"삭제 실패 {file}: {e}")

        # CSV 파일 삭제
        for file in self.normalized_dir.glob("*.csv"):
            try:
                file.unlink()
                cleaned += 1
            except Exception as e:
                logger.warning(f"삭제 실패 {file}: {e}")

        logger.info(f"✅ {cleaned}개 파일 정리 완료\n")

    def process_pdf(self, pdf_path: Path) -> bool:
        """단일 PDF 처리"""
        try:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"📄 처리 중: {pdf_path.name}")
            logger.info(f"{'=' * 60}")
            
            # 1. PDF → JSON
            logger.info("1️⃣ PDF → JSON 변환")
            json_data = extract_pdf_to_json(str(pdf_path), str(self.output_dir))
            
            if not json_data:
                logger.error("JSON 변환 실패")
                return False
            
            json_file = self.output_dir / f"{pdf_path.stem}.json"
            logger.info(f"   ✅ JSON 생성: {json_file.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"처리 실패: {e}")
            return False

    def normalize_all(self) -> bool:
        """모든 JSON 정규화"""
        logger.info("\n" + "=" * 60)
        logger.info("2️⃣ 데이터 정규화")
        logger.info("=" * 60)

        json_files = list(self.output_dir.glob("*.json"))
        json_files = [f for f in json_files if not f.name.startswith('batch_')]

        if not json_files:
            logger.error(f"❌ {self.output_dir}에 JSON 파일이 없습니다.")
            return False

        logger.info(f"📂 {len(json_files)}개 JSON 파일 발견")

        # JSON 데이터 로드
        all_json_data = []
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    all_json_data.append((json_file, json_data))
            except Exception as e:
                logger.error(f"JSON 로드 실패 {json_file.name}: {e}")

        if not all_json_data:
            logger.error("로드된 JSON이 없습니다.")
            return False

        # DB 연결 (PLAN_ID 매칭용)
        db_manager = None
        if not self.skip_db and DB_AVAILABLE:
            try:
                db_manager = OracleDBManager(ORACLE_CONFIG)
                db_manager.connect()
                logger.info("🔗 DB 연결 (PLAN_ID 매칭용)")
            except Exception as e:
                logger.warning(f"⚠️ DB 연결 실패: {e}")
                db_manager = None

        # Normalizer 초기화
        normalizer = GovernmentStandardNormalizer(
            str(json_files[0]),
            str(self.normalized_dir),
            db_manager=db_manager
        )

        # 각 JSON 파일 처리
        for json_file, json_data in all_json_data:
            logger.info(f"📋 정규화 중: {json_file.name}")

            # 파일명에서 연도 추출
            year_match = re.search(r'(20\d{2})', json_file.stem)
            if year_match:
                doc_year = int(year_match.group(1))
                normalizer.current_context['document_year'] = doc_year
                normalizer.current_context['performance_year'] = doc_year - 1
                normalizer.current_context['plan_year'] = doc_year
                logger.info(f"   📅 연도: {doc_year}년")

            normalizer.normalize(json_data)

        # CSV 저장
        normalizer.save_to_csv()

        # DB 연결 종료
        if db_manager:
            db_manager.close()

        # 통계 업데이트
        for table_name, records in normalizer.data.items():
            if isinstance(records, list):
                self.stats['total_records'] += len(records)

        normalizer.print_statistics()
        logger.info("✅ 정규화 완료")

        return True

    def load_to_database(self) -> bool:
        """Oracle DB 적재"""
        if self.skip_db:
            logger.info("\n⏭️ DB 적재 건너뜀")
            return True

        if not DB_AVAILABLE:
            logger.warning("⚠️ DB 모듈이 로드되지 않았습니다.")
            return False

        try:
            logger.info("\n" + "=" * 80)
            logger.info("3️⃣ Oracle 데이터베이스 적재")
            logger.info("=" * 80)

            loader = OracleDirectLoader(
                db_config_read=ORACLE_CONFIG,
                db_config_write=ORACLE_CONFIG_DEV,
                csv_dir=str(self.normalized_dir)
            )
            loader.connect()

            logger.info("\n📋 적재 흐름:")
            logger.info("   1️⃣ BICS.TB_PLAN_DATA 조회 (매칭용)")
            logger.info("   2️⃣ CSV와 매칭 (YEAR + BIZ_NM + DETAIL_BIZ_NM)")
            logger.info("   3️⃣ 하위 테이블 적재 → BICS_DEV")

            loader.load_with_matching()

            # 통계
            stats = loader.load_stats
            logger.info(f"\n✅ 적재 완료: {stats['total_records']:,}건")
            logger.info(f"   - 매칭 성공: {stats['matched']}건")
            logger.info(f"   - 매칭 실패: {stats['unmatched']}건")

            loader.close()

            self.stats['db_loaded'] = True
            self.stats['matched'] = stats['matched']
            self.stats['unmatched'] = stats['unmatched']

            return True

        except Exception as e:
            logger.error(f"❌ Oracle DB 적재 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run(self, pdf_files: List[str] = None):
        """파이프라인 실행"""
        logger.info("\n" + "=" * 80)
        logger.info("🚀 PDF to Database 파이프라인 시작")
        logger.info("=" * 80)
        
        # 이전 데이터 정리
        self.clean_previous_data()

        # PDF 파일 찾기
        if pdf_files:
            pdf_list = [Path(f) for f in pdf_files if Path(f).exists()]
        else:
            pdf_list = list(self.input_dir.glob("*.pdf"))
        
        if not pdf_list:
            logger.error(f"❌ 처리할 PDF 파일이 없습니다!")
            logger.error(f"   '{self.input_dir}' 폴더에 PDF 파일을 넣어주세요.")
            return False

        logger.info(f"📂 {len(pdf_list)}개 PDF 파일 발견\n")

        # 1단계: PDF → JSON
        for pdf_path in pdf_list:
            self.stats['pdf_files'].append(pdf_path.name)
            if self.process_pdf(pdf_path):
                self.stats['processed'] += 1
            else:
                self.stats['failed'] += 1

        if self.stats['processed'] == 0:
            logger.error("❌ 처리된 PDF가 없습니다.")
            return False

        # 2단계: JSON → CSV 정규화
        if not self.normalize_all():
            logger.error("❌ 정규화 실패")
            return False

        # 3단계: DB 적재
        if not self.skip_db:
            self.load_to_database()

        # 최종 통계
        elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 처리 결과")
        logger.info("=" * 80)
        logger.info(f"PDF 처리: {self.stats['processed']}/{len(pdf_list)} 성공")
        logger.info(f"총 레코드: {self.stats['total_records']:,}건")
        logger.info(f"DB 적재: {'✅' if self.stats['db_loaded'] else '⏭️ 건너뜀'}")
        logger.info(f"소요 시간: {elapsed:.1f}초")
        logger.info("=" * 80 + "\n")

        return True


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='생명공학육성시행계획 PDF 처리 시스템',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python main.py                    # input 폴더의 모든 PDF 처리
  python main.py doc1.pdf doc2.pdf  # 특정 PDF 파일 처리
  python main.py --skip-db          # DB 적재 건너뛰기 (CSV만 생성)
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
        help='데이터베이스 적재 건너뛰기 (CSV만 생성)'
    )
    
    args = parser.parse_args()
    
    # 파이프라인 실행
    pipeline = PDFtoDBPipeline(skip_db=args.skip_db)
    success = pipeline.run(args.pdf_files)
    
    if success:
        print("\n✅ 파이프라인 완료!")
    else:
        print("\n❌ 파이프라인 실패")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

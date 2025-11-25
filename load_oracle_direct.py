#!/usr/bin/env python3
"""
Oracle 데이터베이스 직접 적재 모듈
- BICS (읽기): 기존 TB_PLAN_DATA 조회 및 PLAN_ID 매칭
- BICS_DEV (쓰기): 하위 테이블 적재
"""
import csv
import re
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from oracle_db_manager import OracleDBManager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDirectLoader:
    """Oracle DB 직접 적재 클래스"""

    def __init__(self, db_config_read: Dict, db_config_write: Dict, csv_dir: str):
        """
        Args:
            db_config_read: 읽기용 DB 설정 (BICS - TB_PLAN_DATA 조회)
            db_config_write: 쓰기용 DB 설정 (BICS_DEV - 하위 테이블 적재)
            csv_dir: CSV 파일 디렉토리
        """
        self.db_config_read = db_config_read
        self.db_config_write = db_config_write
        self.csv_dir = Path(csv_dir)
        
        self.db_manager_read = None  # BICS (읽기)
        self.db_manager_write = None  # BICS_DEV (쓰기)
        
        # 기존 PLAN_DATA 캐시
        self.existing_plan_data = {}  # {(year, biz_nm, detail_biz_nm): plan_id}
        
        # 적재 통계
        self.load_stats = {
            'total_records': 0,
            'matched': 0,
            'unmatched': 0,
            'diff_found': 0,
            'records_by_table': {}
        }

    def connect(self):
        """데이터베이스 연결"""
        try:
            # 읽기용 DB 연결 (BICS)
            logger.info(f"🔗 읽기용 DB 연결 중... ({self.db_config_read['user']}@{self.db_config_read['host']})")
            self.db_manager_read = OracleDBManager(self.db_config_read)
            self.db_manager_read.connect()
            logger.info("✅ 읽기용 DB 연결 성공")

            # 쓰기용 DB 연결 (BICS_DEV)
            logger.info(f"🔗 쓰기용 DB 연결 중... ({self.db_config_write['user']}@{self.db_config_write['host']})")
            self.db_manager_write = OracleDBManager(self.db_config_write)
            self.db_manager_write.connect()
            logger.info("✅ 쓰기용 DB 연결 성공")

        except Exception as e:
            logger.error(f"❌ DB 연결 실패: {e}")
            raise

    def close(self):
        """연결 종료"""
        if self.db_manager_read:
            self.db_manager_read.close()
        if self.db_manager_write:
            self.db_manager_write.close()

    def _load_existing_plan_data(self):
        """기존 TB_PLAN_DATA 로드 (BICS에서)"""
        logger.info("📋 기존 TB_PLAN_DATA 로드 중...")
        
        try:
            cursor = self.db_manager_read.connection.cursor()
            query = """
                SELECT PLAN_ID, YEAR, BIZ_NM, DETAIL_BIZ_NM
                FROM TB_PLAN_DATA
                WHERE DELETE_YN = 'N'
            """
            cursor.execute(query)
            
            count = 0
            for plan_id, year, biz_nm, detail_biz_nm in cursor:
                count += 1
                # 정규화된 키 생성
                biz_nm_clean = (biz_nm or "").strip()
                detail_biz_nm_clean = (detail_biz_nm or "").strip()
                
                key = (year, biz_nm_clean, detail_biz_nm_clean)
                self.existing_plan_data[key] = (plan_id or "").strip()
            
            cursor.close()
            logger.info(f"✅ 기존 TB_PLAN_DATA 로드 완료: {count}건")
            
        except Exception as e:
            logger.warning(f"⚠️ TB_PLAN_DATA 로드 실패: {e}")

    def _normalize_for_matching(self, text: str) -> str:
        """매칭용 텍스트 정규화"""
        if not text:
            return ""
        
        # 특수문자 통일
        text = text.replace('∙', ' ').replace('·', ' ').replace('・', ' ')
        text = text.replace('/', ' ').replace('-', ' ')
        
        # 괄호 제거
        text = re.sub(r'\([^)]*\)', '', text)
        text = re.sub(r'\[[^\]]*\]', '', text)
        
        # 공백 제거
        text = re.sub(r'\s+', '', text)
        
        return text.strip()

    def _find_plan_id(self, year: int, biz_nm: str, detail_biz_nm: str) -> Tuple[Optional[str], str]:
        """
        PLAN_ID 찾기
        
        Returns:
            (plan_id, reason) - plan_id가 None이면 매칭 실패
        """
        # 1. 완전 일치
        key = (year, biz_nm.strip(), detail_biz_nm.strip())
        if key in self.existing_plan_data:
            return (self.existing_plan_data[key], "완전일치")
        
        # 2. 정규화 후 매칭
        norm_biz = self._normalize_for_matching(biz_nm)
        norm_detail = self._normalize_for_matching(detail_biz_nm)
        
        for (db_year, db_biz, db_detail), plan_id in self.existing_plan_data.items():
            if db_year != year:
                continue
            
            db_norm_biz = self._normalize_for_matching(db_biz)
            db_norm_detail = self._normalize_for_matching(db_detail)
            
            # BIZ_NM과 DETAIL_BIZ_NM 둘 다 정규화 후 일치
            if norm_biz == db_norm_biz and norm_detail == db_norm_detail:
                return (plan_id, "정규화매칭")
            
            # BIZ_NM만 일치하고 DETAIL이 유사
            if norm_biz == db_norm_biz:
                # 부분 문자열 포함 체크
                if norm_detail in db_norm_detail or db_norm_detail in norm_detail:
                    return (plan_id, "부분매칭")
        
        return (None, "매칭실패")

    def _copy_plan_data_to_dev(self):
        """
        BICS의 TB_PLAN_DATA를 BICS_DEV로 복사 (FK 제약조건용)
        - 이미 존재하면 건너뜀
        """
        logger.info("📋 TB_PLAN_DATA 복사 확인 중...")
        
        try:
            # BICS_DEV에 TB_PLAN_DATA 레코드 수 확인
            cursor_write = self.db_manager_write.connection.cursor()
            cursor_write.execute("SELECT COUNT(*) FROM TB_PLAN_DATA")
            dev_count = cursor_write.fetchone()[0]
            
            if dev_count > 0:
                logger.info(f"✅ BICS_DEV.TB_PLAN_DATA 이미 존재: {dev_count}건")
                cursor_write.close()
                return
            
            # BICS에서 데이터 조회
            cursor_read = self.db_manager_read.connection.cursor()
            cursor_read.execute("SELECT * FROM TB_PLAN_DATA WHERE DELETE_YN = 'N'")
            
            # 컬럼명 가져오기
            columns = [desc[0] for desc in cursor_read.description]
            
            # 데이터 복사
            rows = cursor_read.fetchall()
            if rows:
                placeholders = ', '.join([f':{i+1}' for i in range(len(columns))])
                insert_sql = f"INSERT INTO TB_PLAN_DATA ({', '.join(columns)}) VALUES ({placeholders})"
                
                cursor_write.executemany(insert_sql, rows)
                self.db_manager_write.connection.commit()
                
                logger.info(f"✅ TB_PLAN_DATA 복사 완료: {len(rows)}건")
            
            cursor_read.close()
            cursor_write.close()
            
        except Exception as e:
            logger.error(f"❌ TB_PLAN_DATA 복사 실패: {e}")
            # 실패해도 계속 진행 (이미 존재할 수 있음)

    def _read_csv(self, filename: str) -> List[Dict]:
        """CSV 파일 읽기"""
        csv_path = self.csv_dir / filename
        if not csv_path.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {filename}")
            return []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except Exception as e:
            logger.error(f"❌ CSV 읽기 실패 {filename}: {e}")
            return []

    def _generate_id(self, prefix: str, year: int, seq: int) -> str:
        """ID 생성 (CHAR(30) 포맷)"""
        # 예: BUD_2024_0001 형식, 총 30자
        id_str = f"{prefix}_{year}_{seq:06d}"
        return id_str.ljust(30)[:30]

    def _load_budget(self, records: List[Dict]) -> int:
        """TB_PLAN_BUDGET 적재"""
        if not records:
            return 0
        
        cursor = self.db_manager_write.connection.cursor()
        loaded = 0
        
        for idx, record in enumerate(records, 1):
            try:
                plan_id = record.get('PLAN_ID', '').strip()
                if not plan_id or plan_id.startswith('TEMP_'):
                    continue
                
                budget_year = record.get('BUDGET_YEAR')
                if not budget_year:
                    continue
                
                budget_id = self._generate_id('BUD', int(budget_year), idx)
                
                sql = """
                    INSERT INTO TB_PLAN_BUDGET (
                        BUDGET_ID, PLAN_ID, BUDGET_YEAR, CATEGORY,
                        TOTAL_AMOUNT, GOV_AMOUNT, PRIVATE_AMOUNT, LOCAL_AMOUNT, ETC_AMOUNT
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, :9
                    )
                """
                
                def safe_float(val):
                    try:
                        if val and str(val).strip():
                            return float(str(val).replace(',', ''))
                    except:
                        pass
                    return None
                
                cursor.execute(sql, (
                    budget_id,
                    plan_id.ljust(30)[:30],
                    int(budget_year),
                    record.get('CATEGORY', '계획'),
                    safe_float(record.get('TOTAL_AMOUNT')),
                    safe_float(record.get('GOV_AMOUNT')),
                    safe_float(record.get('PRIVATE_AMOUNT')),
                    safe_float(record.get('LOCAL_AMOUNT')),
                    safe_float(record.get('ETC_AMOUNT'))
                ))
                loaded += 1
                
            except Exception as e:
                logger.debug(f"Budget 적재 실패: {e}")
                continue
        
        self.db_manager_write.connection.commit()
        cursor.close()
        return loaded

    def _load_schedule(self, records: List[Dict]) -> int:
        """TB_PLAN_SCHEDULE 적재"""
        if not records:
            return 0
        
        cursor = self.db_manager_write.connection.cursor()
        loaded = 0
        
        for idx, record in enumerate(records, 1):
            try:
                plan_id = record.get('PLAN_ID', '').strip()
                if not plan_id or plan_id.startswith('TEMP_'):
                    continue
                
                schedule_year = record.get('SCHEDULE_YEAR')
                if not schedule_year:
                    continue
                
                schedule_id = self._generate_id('SCH', int(schedule_year), idx)
                
                sql = """
                    INSERT INTO TB_PLAN_SCHEDULE (
                        SCHEDULE_ID, PLAN_ID, SCHEDULE_YEAR, QUARTER,
                        TASK_NAME, TASK_CONTENT, START_DATE, END_DATE
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, TO_DATE(:7, 'YYYY-MM-DD'), TO_DATE(:8, 'YYYY-MM-DD')
                    )
                """
                
                cursor.execute(sql, (
                    schedule_id,
                    plan_id.ljust(30)[:30],
                    int(schedule_year),
                    record.get('QUARTER', ''),
                    (record.get('TASK_NAME') or '')[:768],
                    (record.get('TASK_CONTENT') or '')[:4000],
                    record.get('START_DATE'),
                    record.get('END_DATE')
                ))
                loaded += 1
                
            except Exception as e:
                logger.debug(f"Schedule 적재 실패: {e}")
                continue
        
        self.db_manager_write.connection.commit()
        cursor.close()
        return loaded

    def _load_performance(self, records: List[Dict]) -> int:
        """TB_PLAN_PERFORMANCE 적재"""
        if not records:
            return 0
        
        cursor = self.db_manager_write.connection.cursor()
        loaded = 0
        
        for idx, record in enumerate(records, 1):
            try:
                plan_id = record.get('PLAN_ID', '').strip()
                if not plan_id or plan_id.startswith('TEMP_'):
                    continue
                
                perf_year = record.get('PERFORMANCE_YEAR')
                if not perf_year:
                    continue
                
                perf_id = self._generate_id('PRF', int(perf_year), idx)
                
                sql = """
                    INSERT INTO TB_PLAN_PERFORMANCE (
                        PERFORMANCE_ID, PLAN_ID, PERFORMANCE_YEAR, PERFORMANCE_TYPE,
                        CATEGORY, VALUE, UNIT, ORIGINAL_TEXT
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8
                    )
                """
                
                def safe_float(val):
                    try:
                        if val and str(val).strip():
                            return float(str(val).replace(',', ''))
                    except:
                        pass
                    return None
                
                cursor.execute(sql, (
                    perf_id,
                    plan_id.ljust(30)[:30],
                    int(perf_year),
                    (record.get('PERFORMANCE_TYPE') or '')[:100],
                    (record.get('CATEGORY') or '')[:200],
                    safe_float(record.get('VALUE')),
                    (record.get('UNIT') or '')[:50],
                    (record.get('ORIGINAL_TEXT') or '')[:4000]
                ))
                loaded += 1
                
            except Exception as e:
                logger.debug(f"Performance 적재 실패: {e}")
                continue
        
        self.db_manager_write.connection.commit()
        cursor.close()
        return loaded

    def _load_achievements(self, records: List[Dict]) -> int:
        """TB_PLAN_ACHIEVEMENTS 적재"""
        if not records:
            return 0
        
        cursor = self.db_manager_write.connection.cursor()
        loaded = 0
        
        for idx, record in enumerate(records, 1):
            try:
                plan_id = record.get('PLAN_ID', '').strip()
                if not plan_id or plan_id.startswith('TEMP_'):
                    continue
                
                ach_year = record.get('ACHIEVEMENT_YEAR')
                if not ach_year:
                    continue
                
                ach_id = self._generate_id('ACH', int(ach_year), idx)
                
                sql = """
                    INSERT INTO TB_PLAN_ACHIEVEMENTS (
                        ACHIEVEMENT_ID, PLAN_ID, ACHIEVEMENT_YEAR,
                        ACHIEVEMENT_ORDER, DESCRIPTION
                    ) VALUES (
                        :1, :2, :3, :4, :5
                    )
                """
                
                cursor.execute(sql, (
                    ach_id,
                    plan_id.ljust(30)[:30],
                    int(ach_year),
                    record.get('ACHIEVEMENT_ORDER', idx),
                    (record.get('DESCRIPTION') or '')[:4000]
                ))
                loaded += 1
                
            except Exception as e:
                logger.debug(f"Achievement 적재 실패: {e}")
                continue
        
        self.db_manager_write.connection.commit()
        cursor.close()
        return loaded

    def _generate_matching_report(self, plan_data: List[Dict]):
        """매칭 리포트 생성"""
        report_dir = self.csv_dir / "matching_reports"
        report_dir.mkdir(exist_ok=True)
        
        matched_records = []
        unmatched_records = []
        
        for idx, record in enumerate(plan_data):
            plan_id = record.get('PLAN_ID', '')
            year = record.get('YEAR')
            biz_nm = record.get('BIZ_NM', '')
            detail_biz_nm = record.get('DETAIL_BIZ_NM', '')
            
            if plan_id and not plan_id.startswith('TEMP_'):
                matched_records.append({
                    'csv_index': idx + 1,
                    'year': year,
                    'biz_nm': biz_nm,
                    'detail_biz_nm': detail_biz_nm,
                    'plan_id': plan_id,
                    'status': 'matched'
                })
                self.load_stats['matched'] += 1
            else:
                unmatched_records.append({
                    'csv_index': idx + 1,
                    'year': year,
                    'biz_nm': biz_nm,
                    'detail_biz_nm': detail_biz_nm,
                    'plan_id': plan_id,
                    'reason': '매칭실패-신규사업'
                })
                self.load_stats['unmatched'] += 1
        
        # 매칭 리포트 저장
        if matched_records:
            with open(report_dir / "matching_report.csv", 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=['csv_index', 'year', 'biz_nm', 'detail_biz_nm', 'plan_id', 'status'])
                writer.writeheader()
                writer.writerows(matched_records)
        
        # 매칭 실패 리포트 저장
        if unmatched_records:
            with open(report_dir / "unmatched_records.csv", 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=['csv_index', 'year', 'biz_nm', 'detail_biz_nm', 'plan_id', 'reason'])
                writer.writeheader()
                writer.writerows(unmatched_records)
        
        logger.info(f"📊 매칭 리포트 생성: {report_dir}")
        logger.info(f"   - 매칭 성공: {len(matched_records)}건")
        logger.info(f"   - 매칭 실패: {len(unmatched_records)}건")

    def load_with_matching(self):
        """매칭 기반 데이터 적재 (메인 메서드)"""
        logger.info("\n" + "=" * 80)
        logger.info("🚀 매칭 기반 데이터 적재 시작")
        logger.info("=" * 80)
        
        # 1. 기존 PLAN_DATA 로드
        self._load_existing_plan_data()
        
        # 2. TB_PLAN_DATA 복사 (BICS → BICS_DEV)
        self._copy_plan_data_to_dev()
        
        # 3. CSV 파일 읽기
        plan_data = self._read_csv("TB_PLAN_DATA.csv")
        budgets = self._read_csv("TB_PLAN_BUDGET.csv")
        schedules = self._read_csv("TB_PLAN_SCHEDULE.csv")
        performances = self._read_csv("TB_PLAN_PERFORMANCE.csv")
        achievements = self._read_csv("TB_PLAN_ACHIEVEMENTS.csv")
        
        logger.info(f"\n📂 CSV 파일 로드:")
        logger.info(f"   - TB_PLAN_DATA: {len(plan_data)}건")
        logger.info(f"   - TB_PLAN_BUDGET: {len(budgets)}건")
        logger.info(f"   - TB_PLAN_SCHEDULE: {len(schedules)}건")
        logger.info(f"   - TB_PLAN_PERFORMANCE: {len(performances)}건")
        logger.info(f"   - TB_PLAN_ACHIEVEMENTS: {len(achievements)}건")
        
        # 4. 매칭 리포트 생성
        self._generate_matching_report(plan_data)
        
        # 5. 하위 테이블 적재
        logger.info("\n📥 하위 테이블 적재 중...")
        
        budget_count = self._load_budget(budgets)
        self.load_stats['records_by_table']['TB_PLAN_BUDGET'] = budget_count
        logger.info(f"   ✅ TB_PLAN_BUDGET: {budget_count}건")
        
        schedule_count = self._load_schedule(schedules)
        self.load_stats['records_by_table']['TB_PLAN_SCHEDULE'] = schedule_count
        logger.info(f"   ✅ TB_PLAN_SCHEDULE: {schedule_count}건")
        
        performance_count = self._load_performance(performances)
        self.load_stats['records_by_table']['TB_PLAN_PERFORMANCE'] = performance_count
        logger.info(f"   ✅ TB_PLAN_PERFORMANCE: {performance_count}건")
        
        achievement_count = self._load_achievements(achievements)
        self.load_stats['records_by_table']['TB_PLAN_ACHIEVEMENTS'] = achievement_count
        logger.info(f"   ✅ TB_PLAN_ACHIEVEMENTS: {achievement_count}건")
        
        # 6. 총 적재 레코드 계산
        self.load_stats['total_records'] = (
            budget_count + schedule_count + performance_count + achievement_count
        )
        
        logger.info("\n" + "=" * 80)
        logger.info(f"✅ 적재 완료: 총 {self.load_stats['total_records']}건")
        logger.info("=" * 80)


if __name__ == "__main__":
    # 테스트 실행
    from config import ORACLE_CONFIG, ORACLE_CONFIG_DEV, NORMALIZED_OUTPUT_GOVERNMENT_DIR
    
    loader = OracleDirectLoader(
        db_config_read=ORACLE_CONFIG,
        db_config_write=ORACLE_CONFIG_DEV,
        csv_dir=str(NORMALIZED_OUTPUT_GOVERNMENT_DIR)
    )
    
    try:
        loader.connect()
        loader.load_with_matching()
    finally:
        loader.close()

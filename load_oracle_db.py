"""
Oracle 데이터베이스 적재 모듈
MySQL/CSV → Oracle TB_PLAN_DATA 및 하위 테이블
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import re

from oracle_db_manager import OracleDBManager
from oracle_table_ddl import (
    TABLE_DEFINITIONS, CREATE_INDEXES,
    COMMENT_TB_PLAN_DATA
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDBLoader:
    """Oracle 데이터베이스 적재 클래스"""

    def __init__(self, db_config: Dict[str, Any], csv_dir: str):
        """
        Args:
            db_config: Oracle 연결 설정
            csv_dir: CSV 파일 디렉토리 (normalized_output_government)
        """
        self.db_manager = OracleDBManager(db_config)
        self.csv_dir = Path(csv_dir)
        self.plan_id_mapping = {}  # sub_project_id → PLAN_ID 매핑

        # 적재 통계
        self.load_stats = {
            'tables_created': 0,
            'total_records': 0,
            'records_by_table': {},
            'errors': []
        }

    def connect(self):
        """Oracle 연결"""
        return self.db_manager.connect()

    def drop_existing_tables(self):
        """기존 테이블 삭제 (역순)"""
        logger.info("🗑️ 기존 테이블 삭제 중...")

        # 역순으로 삭제 (외래키 제약 때문)
        tables_to_drop = [
            'TB_PLAN_DETAILS',
            'TB_PLAN_ACHIEVEMENTS',
            'TB_PLAN_BUDGETS',
            'TB_PLAN_PERFORMANCES',
            'TB_PLAN_SCHEDULES',
            'TB_PLAN_DATA'
        ]

        for table_name in tables_to_drop:
            try:
                self.db_manager.drop_table(table_name, cascade=True)
            except Exception as e:
                logger.warning(f"⚠️ {table_name} 삭제 실패 (없을 수 있음): {e}")

        logger.info("✅ 테이블 삭제 완료")

    def create_tables(self):
        """테이블 생성"""
        logger.info("📊 Oracle 테이블 생성 중...")

        for table_name, create_ddl, comments in TABLE_DEFINITIONS:
            try:
                # 테이블 생성
                logger.info(f"  생성 중: {table_name}")
                self.db_manager.execute_ddl(create_ddl)

                # 컬럼 주석 추가
                for comment in comments:
                    self.db_manager.execute_ddl(comment)

                self.load_stats['tables_created'] += 1
                logger.info(f"  ✅ {table_name} 생성 완료")

            except Exception as e:
                logger.error(f"❌ {table_name} 생성 실패: {e}")
                self.load_stats['errors'].append(f"{table_name} 생성: {str(e)}")
                raise

        # 인덱스 생성
        logger.info("📇 인덱스 생성 중...")
        for idx, index_ddl in enumerate(CREATE_INDEXES, 1):
            try:
                self.db_manager.execute_ddl(index_ddl)
                logger.info(f"  ✅ 인덱스 {idx}/{len(CREATE_INDEXES)} 생성")
            except Exception as e:
                logger.warning(f"⚠️ 인덱스 생성 실패: {e}")

        self.db_manager.commit()
        logger.info("✅ 모든 테이블 생성 완료")

    def load_tb_plan_data(self) -> Dict[int, str]:
        """
        TB_PLAN_DATA 적재 (sub_projects.csv + 다른 CSV 조인하여 완전히 채우기)
        Returns: sub_project_id → PLAN_ID 매핑 딕셔너리
        """
        logger.info("📥 TB_PLAN_DATA 적재 중 (NULL 최소화 모드)...")

        # 1. 모든 CSV 파일 로드
        csv_file = self.csv_dir / "sub_projects.csv"
        if not csv_file.exists():
            raise FileNotFoundError(f"❌ {csv_file} 파일이 없습니다.")

        sub_projects = pd.read_csv(csv_file, encoding='utf-8-sig')

        # 추가 데이터 로드
        overviews_file = self.csv_dir / "normalized_overviews.csv"
        budgets_file = self.csv_dir / "normalized_budgets.csv"
        schedules_file = self.csv_dir / "normalized_schedules.csv"

        overviews = pd.read_csv(overviews_file, encoding='utf-8-sig') if overviews_file.exists() else None
        budgets = pd.read_csv(budgets_file, encoding='utf-8-sig') if budgets_file.exists() else None
        schedules = pd.read_csv(schedules_file, encoding='utf-8-sig') if schedules_file.exists() else None

        insert_query = """
            INSERT INTO TB_PLAN_DATA (
                PLAN_ID, YEAR, NUM, NATION_ORGAN_NM, DETAIL_BIZ_NM, BIZ_NM,
                BIZ_TYPE, REP_FLD, LEAD_ORGAN_NM, MNG_ORGAN_NM,
                BIZ_SDT, BIZ_EDT,
                TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV,
                CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV,
                LAST_GOAL, BIZ_CONTENTS,
                PERFORM_PRC, PLAN_PRC,
                REGIST_DT, REGIST_ID
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
                TO_DATE(:11, 'YYYY-MM-DD'), TO_DATE(:12, 'YYYY-MM-DD'),
                :13, :14, :15, :16, :17, :18, :19, :20, :21, :22,
                SYSDATE, :23
            )
        """

        inserted_count = 0
        plan_id_mapping = {}

        for idx, row in sub_projects.iterrows():
            try:
                sub_project_id = row['id']
                year = row['document_year']
                num = idx + 1
                plan_id = f"{year}{num:03d}"

                # 매핑 저장
                plan_id_mapping[sub_project_id] = plan_id

                # ===== Overview 데이터 가져오기 =====
                biz_type = None
                rep_fld = None
                lead_organ = None
                mng_organ = None
                last_goal = None
                biz_contents = None

                if overviews is not None:
                    overview_data = overviews[overviews['sub_project_id'] == sub_project_id]
                    if not overview_data.empty:
                        ov = overview_data.iloc[0]
                        biz_type = str(ov['project_type'])[:768] if pd.notna(ov['project_type']) else None
                        rep_fld = str(ov['field'])[:768] if pd.notna(ov['field']) else None
                        lead_organ = str(ov['managing_dept'])[:768] if pd.notna(ov['managing_dept']) else None
                        mng_organ = str(ov['managing_org'])[:768] if pd.notna(ov['managing_org']) else None
                        last_goal = str(ov['objective'])[:4000] if pd.notna(ov['objective']) else None
                        biz_contents = str(ov['content'])[:4000] if pd.notna(ov['content']) else None

                # ===== 예산 데이터 집계 =====
                total_resprc = None
                total_resprc_gov = None
                total_resprc_civ = None
                cur_resprc = None
                cur_resprc_gov = None
                cur_resprc_civ = None
                perform_prc = None
                plan_prc = None

                if budgets is not None:
                    project_budgets = budgets[budgets['sub_project_id'] == sub_project_id]

                    if not project_budgets.empty:
                        # 총 연구비 (모든 연도)
                        total_resprc_gov = project_budgets[
                            project_budgets['budget_type'] == '정부'
                        ]['amount'].sum()

                        total_resprc_civ = project_budgets[
                            project_budgets['budget_type'] == '민간'
                        ]['amount'].sum()

                        local_total = project_budgets[
                            project_budgets['budget_type'] == '지방비'
                        ]['amount'].sum()

                        total_resprc = total_resprc_gov + total_resprc_civ + local_total

                        # 현재 연도 연구비
                        cur_budgets = project_budgets[project_budgets['budget_year'] == year]
                        if not cur_budgets.empty:
                            cur_resprc_gov = cur_budgets[
                                cur_budgets['budget_type'] == '정부'
                            ]['amount'].sum()

                            cur_resprc_civ = cur_budgets[
                                cur_budgets['budget_type'] == '민간'
                            ]['amount'].sum()

                            cur_local = cur_budgets[
                                cur_budgets['budget_type'] == '지방비'
                            ]['amount'].sum()

                            cur_resprc = cur_resprc_gov + cur_resprc_civ + cur_local

                        # 실적/계획 비용
                        perform_prc = project_budgets[
                            project_budgets['is_actual'] == True
                        ]['amount'].sum()

                        plan_prc = project_budgets[
                            project_budgets['is_actual'] == False
                        ]['amount'].sum()

                        # 0이면 None으로 처리
                        if total_resprc == 0: total_resprc = None
                        if total_resprc_gov == 0: total_resprc_gov = None
                        if total_resprc_civ == 0: total_resprc_civ = None
                        if cur_resprc == 0: cur_resprc = None
                        if cur_resprc_gov == 0: cur_resprc_gov = None
                        if cur_resprc_civ == 0: cur_resprc_civ = None
                        if perform_prc == 0: perform_prc = None
                        if plan_prc == 0: plan_prc = None

                # ===== 사업 기간 (일정에서 추출) =====
                biz_sdt = None
                biz_edt = None

                if schedules is not None:
                    project_schedules = schedules[schedules['sub_project_id'] == sub_project_id]
                    if not project_schedules.empty:
                        # start_date와 end_date가 있으면 min/max 추출
                        if 'start_date' in project_schedules.columns:
                            dates = project_schedules['start_date'].dropna()
                            if len(dates) > 0:
                                biz_sdt = str(dates.min())[:10]  # YYYY-MM-DD 형식

                        if 'end_date' in project_schedules.columns:
                            dates = project_schedules['end_date'].dropna()
                            if len(dates) > 0:
                                biz_edt = str(dates.max())[:10]  # YYYY-MM-DD 형식

                # ===== 데이터 준비 =====
                data = (
                    plan_id,                                      # PLAN_ID
                    int(year),                                    # YEAR
                    num,                                          # NUM
                    str(row['department_name'])[:768],            # NATION_ORGAN_NM
                    str(row['sub_project_name'])[:768],          # DETAIL_BIZ_NM
                    str(row['main_project_name'])[:768],         # BIZ_NM
                    biz_type,                                     # BIZ_TYPE ✅
                    rep_fld,                                      # REP_FLD ✅
                    lead_organ,                                   # LEAD_ORGAN_NM ✅
                    mng_organ,                                    # MNG_ORGAN_NM ✅
                    biz_sdt,                                      # BIZ_SDT ✅
                    biz_edt,                                      # BIZ_EDT ✅
                    total_resprc,                                 # TOTAL_RESPRC ✅
                    total_resprc_gov,                             # TOTAL_RESPRC_GOV ✅
                    total_resprc_civ,                             # TOTAL_RESPRC_CIV ✅
                    cur_resprc,                                   # CUR_RESPRC ✅
                    cur_resprc_gov,                               # CUR_RESPRC_GOV ✅
                    cur_resprc_civ,                               # CUR_RESPRC_CIV ✅
                    last_goal,                                    # LAST_GOAL ✅
                    biz_contents,                                 # BIZ_CONTENTS ✅
                    perform_prc,                                  # PERFORM_PRC ✅
                    plan_prc,                                     # PLAN_PRC ✅
                    'SYSTEM'                                      # REGIST_ID
                )

                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1

                if inserted_count % 50 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")

            except Exception as e:
                logger.error(f"❌ TB_PLAN_DATA 삽입 실패 (행 {idx}, sub_project_id={sub_project_id}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_DATA 행 {idx}: {str(e)}")

        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_DATA: {inserted_count}건 적재 완료 (NULL 최소화 모드)")

        self.load_stats['records_by_table']['TB_PLAN_DATA'] = inserted_count
        self.load_stats['total_records'] += inserted_count

        return plan_id_mapping

    def load_tb_plan_schedules(self):
        """TB_PLAN_SCHEDULES 적재"""
        logger.info("📥 TB_PLAN_SCHEDULES 적재 중...")

        csv_file = self.csv_dir / "normalized_schedules.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다. 스킵합니다.")
            return

        df = pd.read_csv(csv_file, encoding='utf-8-sig')

        insert_query = """
            INSERT INTO TB_PLAN_SCHEDULES (
                PLAN_ID, YEAR, QUARTER, MONTH_START, MONTH_END,
                START_DATE, END_DATE, TASK_CATEGORY, TASK_DESCRIPTION,
                ORIGINAL_PERIOD
            ) VALUES (
                :1, :2, :3, :4, :5, TO_DATE(:6, 'YYYY-MM-DD'), 
                TO_DATE(:7, 'YYYY-MM-DD'), :8, :9, :10
            )
        """

        inserted_count = 0

        for idx, row in df.iterrows():
            try:
                sub_project_id = row['sub_project_id']
                plan_id = self.plan_id_mapping.get(sub_project_id)

                if not plan_id:
                    logger.warning(f"⚠️ PLAN_ID 매핑 없음: sub_project_id={sub_project_id}")
                    continue

                data = (
                    plan_id,
                    int(row['year']) if pd.notna(row['year']) else None,
                    int(row['quarter']) if pd.notna(row['quarter']) else None,
                    int(row['month_start']) if pd.notna(row['month_start']) else None,
                    int(row['month_end']) if pd.notna(row['month_end']) else None,
                    str(row['start_date']) if pd.notna(row['start_date']) else None,
                    str(row['end_date']) if pd.notna(row['end_date']) else None,
                    str(row['task_category'])[:200] if pd.notna(row['task_category']) else None,
                    str(row['task_description']) if pd.notna(row['task_description']) else None,
                    str(row['original_period'])[:100] if pd.notna(row['original_period']) else None
                )

                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1

                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")

            except Exception as e:
                logger.error(f"❌ TB_PLAN_SCHEDULES 삽입 실패 (행 {idx}): {e}")

        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_SCHEDULES: {inserted_count}건 적재 완료")

        self.load_stats['records_by_table']['TB_PLAN_SCHEDULES'] = inserted_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_performances(self):
        """TB_PLAN_PERFORMANCES 적재"""
        logger.info("📥 TB_PLAN_PERFORMANCES 적재 중...")

        csv_file = self.csv_dir / "normalized_performances.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다. 스킵합니다.")
            return

        df = pd.read_csv(csv_file, encoding='utf-8-sig')

        insert_query = """
            INSERT INTO TB_PLAN_PERFORMANCES (
                PLAN_ID, PERFORMANCE_YEAR, INDICATOR_CATEGORY,
                INDICATOR_TYPE, VALUE, UNIT, ORIGINAL_TEXT
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7
            )
        """

        inserted_count = 0

        for idx, row in df.iterrows():
            try:
                sub_project_id = row['sub_project_id']
                plan_id = self.plan_id_mapping.get(sub_project_id)

                if not plan_id:
                    continue

                data = (
                    plan_id,
                    int(row['performance_year']) if pd.notna(row['performance_year']) else None,
                    str(row['indicator_category'])[:100] if pd.notna(row['indicator_category']) else None,
                    str(row['indicator_type'])[:200] if pd.notna(row['indicator_type']) else None,
                    int(row['value']) if pd.notna(row['value']) else None,
                    str(row['unit'])[:50] if pd.notna(row['unit']) else None,
                    str(row['original_text']) if pd.notna(row['original_text']) else None
                )

                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1

                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")

            except Exception as e:
                logger.error(f"❌ TB_PLAN_PERFORMANCES 삽입 실패 (행 {idx}): {e}")

        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_PERFORMANCES: {inserted_count}건 적재 완료")

        self.load_stats['records_by_table']['TB_PLAN_PERFORMANCES'] = inserted_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_budgets(self):
        """TB_PLAN_BUDGETS 적재"""
        logger.info("📥 TB_PLAN_BUDGETS 적재 중...")

        csv_file = self.csv_dir / "normalized_budgets.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다. 스킵합니다.")
            return

        df = pd.read_csv(csv_file, encoding='utf-8-sig')

        insert_query = """
            INSERT INTO TB_PLAN_BUDGETS (
                PLAN_ID, BUDGET_YEAR, BUDGET_CATEGORY, BUDGET_TYPE,
                AMOUNT, CURRENCY, IS_ACTUAL, ORIGINAL_TEXT
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8
            )
        """

        inserted_count = 0

        for idx, row in df.iterrows():
            try:
                sub_project_id = row['sub_project_id']
                plan_id = self.plan_id_mapping.get(sub_project_id)

                if not plan_id:
                    continue

                data = (
                    plan_id,
                    int(row['budget_year']) if pd.notna(row['budget_year']) else None,
                    str(row['budget_category'])[:100] if pd.notna(row['budget_category']) else None,
                    str(row['budget_type'])[:100] if pd.notna(row['budget_type']) else None,
                    float(row['amount']) if pd.notna(row['amount']) else None,
                    str(row['currency'])[:10] if pd.notna(row['currency']) else 'KRW',
                    'Y' if row.get('is_actual', False) else 'N',
                    str(row['original_text']) if pd.notna(row['original_text']) else None
                )

                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1

                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")

            except Exception as e:
                logger.error(f"❌ TB_PLAN_BUDGETS 삽입 실패 (행 {idx}): {e}")

        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_BUDGETS: {inserted_count}건 적재 완료")

        self.load_stats['records_by_table']['TB_PLAN_BUDGETS'] = inserted_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_achievements(self):
        """TB_PLAN_ACHIEVEMENTS 적재"""
        logger.info("📥 TB_PLAN_ACHIEVEMENTS 적재 중...")

        csv_file = self.csv_dir / "key_achievements.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다. 스킵합니다.")
            return

        df = pd.read_csv(csv_file, encoding='utf-8-sig')

        insert_query = """
            INSERT INTO TB_PLAN_ACHIEVEMENTS (
                PLAN_ID, ACHIEVEMENT_YEAR, ACHIEVEMENT_ORDER, DESCRIPTION
            ) VALUES (
                :1, :2, :3, :4
            )
        """

        inserted_count = 0

        for idx, row in df.iterrows():
            try:
                sub_project_id = row['sub_project_id']
                plan_id = self.plan_id_mapping.get(sub_project_id)

                if not plan_id:
                    continue

                data = (
                    plan_id,
                    int(row['achievement_year']) if pd.notna(row['achievement_year']) else None,
                    int(row['achievement_order']) if pd.notna(row['achievement_order']) else None,
                    str(row['description']) if pd.notna(row['description']) else None
                )

                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1

                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")

            except Exception as e:
                logger.error(f"❌ TB_PLAN_ACHIEVEMENTS 삽입 실패 (행 {idx}): {e}")

        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_ACHIEVEMENTS: {inserted_count}건 적재 완료")

        self.load_stats['records_by_table']['TB_PLAN_ACHIEVEMENTS'] = inserted_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_details(self):
        """TB_PLAN_DETAILS 적재"""
        logger.info("📥 TB_PLAN_DETAILS 적재 중...")

        csv_file = self.csv_dir / "plan_details.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다. 스킵합니다.")
            return

        df = pd.read_csv(csv_file, encoding='utf-8-sig')

        insert_query = """
            INSERT INTO TB_PLAN_DETAILS (
                PLAN_ID, PLAN_YEAR, PLAN_ORDER, DESCRIPTION
            ) VALUES (
                :1, :2, :3, :4
            )
        """

        inserted_count = 0

        for idx, row in df.iterrows():
            try:
                sub_project_id = row['sub_project_id']
                plan_id = self.plan_id_mapping.get(sub_project_id)

                if not plan_id:
                    continue

                data = (
                    plan_id,
                    int(row['plan_year']) if pd.notna(row['plan_year']) else None,
                    int(row['plan_order']) if pd.notna(row['plan_order']) else None,
                    str(row['description']) if pd.notna(row['description']) else None
                )

                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1

                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")

            except Exception as e:
                logger.error(f"❌ TB_PLAN_DETAILS 삽입 실패 (행 {idx}): {e}")

        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_DETAILS: {inserted_count}건 적재 완료")

        self.load_stats['records_by_table']['TB_PLAN_DETAILS'] = inserted_count
        self.load_stats['total_records'] += inserted_count

    def load_all_tables(self):
        """모든 테이블 적재"""
        logger.info("📥 데이터 적재 시작...")

        # 1. TB_PLAN_DATA 먼저 적재 (마스터)
        self.plan_id_mapping = self.load_tb_plan_data()

        # 2. 하위 테이블 적재
        self.load_tb_plan_schedules()
        self.load_tb_plan_performances()
        self.load_tb_plan_budgets()
        self.load_tb_plan_achievements()
        self.load_tb_plan_details()

        logger.info("✅ 모든 데이터 적재 완료")
        self._print_load_summary()

    def _print_load_summary(self):
        """적재 요약 출력"""
        print("\n" + "="*70)
        print("📊 Oracle 데이터 적재 요약")
        print("="*70)
        print(f"✅ 생성된 테이블: {self.load_stats['tables_created']}개")
        print(f"✅ 총 적재 레코드: {self.load_stats['total_records']:,}건")
        print(f"✅ PLAN_ID 매핑: {len(self.plan_id_mapping)}건")
        print("\n테이블별 적재 현황:")

        for table, count in self.load_stats['records_by_table'].items():
            print(f"  • {table}: {count:,}건")

        if self.load_stats['errors']:
            print(f"\n⚠️ 오류 발생: {len(self.load_stats['errors'])}건")
            for error in self.load_stats['errors'][:10]:  # 최대 10개만 표시
                print(f"  - {error}")

        print("="*70)

    def close(self):
        """연결 종료"""
        self.db_manager.close()


def main():
    """메인 실행"""
    from config import ORACLE_CONFIG

    # CSV 디렉토리
    csv_dir = "normalized_output_government"

    # 적재 실행
    loader = OracleDBLoader(ORACLE_CONFIG, csv_dir)

    try:
        # 연결
        logger.info("🔌 Oracle 데이터베이스 연결 중...")
        loader.connect()

        # 기존 테이블 삭제
        loader.drop_existing_tables()

        # 테이블 생성
        loader.create_tables()

        # 데이터 적재
        loader.load_all_tables()

        print("\n✅ Oracle 데이터베이스 적재 완료!")

    except Exception as e:
        logger.error(f"❌ 적재 실패: {e}")
        raise

    finally:
        loader.close()


if __name__ == "__main__":
    main()


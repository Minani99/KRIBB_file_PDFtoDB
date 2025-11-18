"""
Oracle DB 적재 - TB_PLAN_* 테이블에 직접 적재
CSV 파일이 Oracle 스키마와 동일하므로 간단한 매핑만 필요
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from oracle_db_manager import OracleDBManager
from oracle_table_ddl import (
    TABLE_DEFINITIONS,
    CREATE_INDEXES,
    TABLE_CREATE_ORDER,
    TABLE_DROP_ORDER
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDirectLoader:
    """Oracle 직접 적재 클래스 - CSV가 이미 Oracle 스키마와 일치"""

    def __init__(self, db_config: Dict[str, Any], csv_dir: str):
        self.db_manager = OracleDBManager(db_config)
        self.csv_dir = Path(csv_dir)

        self.load_stats = {
            'tables_created': 0,
            'total_records': 0,
            'records_by_table': {},
            'errors': []
        }

    def connect(self):
        """Oracle 연결"""
        return self.db_manager.connect()

    def create_tables(self):
        """테이블 생성"""
        logger.info("\n🏗️ Oracle 테이블 생성 중...")

        for table_name in TABLE_CREATE_ORDER:
            try:
                ddl = TABLE_DEFINITIONS[table_name]
                self.db_manager.execute_ddl(ddl)
                logger.info(f"  ✅ {table_name} 생성 완료")
                self.load_stats['tables_created'] += 1
            except Exception as e:
                logger.warning(f"  ⚠️ {table_name} 생성 실패 (이미 존재하거나 에러): {e}")

    def truncate_tables(self):
        """기존 데이터 삭제 (테이블 구조는 유지)"""
        logger.info("\n🗑️ 기존 데이터 삭제 중...")

        # 역순으로 TRUNCATE (FK 제약 때문)
        truncate_order = list(reversed(TABLE_CREATE_ORDER))

        truncated_count = 0
        cursor = self.db_manager.connection.cursor()

        for table_name in truncate_order:
            try:
                # ✅ CASCADE 옵션으로 FK 제약조건 무시
                cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                self.db_manager.connection.commit()
                logger.info(f"  ✅ {table_name} 데이터 삭제 완료")
                truncated_count += 1
            except Exception as e:
                error_msg = str(e)
                # 테이블이 없는 경우는 무시
                if "ORA-00942" in error_msg:  # table or view does not exist
                    logger.debug(f"  ⏭️ {table_name} 테이블 없음 (건너뜀)")
                else:
                    # ✅ 에러를 더 상세하게 로깅
                    logger.error(f"  ❌ {table_name} 삭제 실패: {error_msg}")
                    # FK 제약조건 에러 발생 시 재시도 (CASCADE 없이)
                    if "ORA-02266" in error_msg:  # foreign key constraint
                        logger.warning(f"  🔄 {table_name} FK 제약조건 무시하고 재시도...")
                        try:
                            # FK 제약조건 비활성화 후 삭제
                            cursor.execute(f"DELETE FROM {table_name}")
                            self.db_manager.connection.commit()
                            logger.info(f"  ✅ {table_name} DELETE로 삭제 완료")
                            truncated_count += 1
                        except Exception as e2:
                            logger.error(f"  ❌ {table_name} DELETE도 실패: {e2}")

        cursor.close()

        if truncated_count > 0:
            logger.info(f"✅ {truncated_count}개 테이블 데이터 삭제 완료")
            return truncated_count
        else:
            logger.warning("⚠️ 삭제된 테이블이 없습니다. 테이블이 존재하지 않을 수 있습니다.")
            return 0

    def load_tb_plan_master(self) -> int:
        """TB_PLAN_MASTER 적재"""
        logger.info("\n1️⃣ TB_PLAN_MASTER 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_MASTER.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        cursor = self.db_manager.connection.cursor()

        for idx, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO TB_PLAN_MASTER (
                        PLAN_ID, YEAR, NUM,
                        NATION_ORGAN_NM, BIZ_NM, DETAIL_BIZ_NM,
                        REGIST_ID, REGIST_DT, DELETE_YN
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, 'SYSTEM', SYSDATE, 'N'
                    )
                """, (
                    row['PLAN_ID'],
                    int(row['YEAR']) if pd.notna(row['YEAR']) else None,
                    int(row['NUM']) if pd.notna(row['NUM']) else None,
                    str(row['NATION_ORGAN_NM'])[:768] if pd.notna(row['NATION_ORGAN_NM']) else None,
                    str(row['BIZ_NM'])[:768] if pd.notna(row['BIZ_NM']) else None,
                    str(row['DETAIL_BIZ_NM'])[:768] if pd.notna(row['DETAIL_BIZ_NM']) else None
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager.connection.commit()
        logger.info(f"  ✅ TB_PLAN_MASTER 적재 완료: {inserted}건")
        self.load_stats['records_by_table']['TB_PLAN_MASTER'] = inserted
        return inserted

    def load_tb_plan_detail(self) -> int:
        """TB_PLAN_DETAIL 적재"""
        logger.info("\n2️⃣ TB_PLAN_DETAIL 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_DETAIL.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        cursor = self.db_manager.connection.cursor()

        for idx, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO TB_PLAN_DETAIL (
                        DETAIL_ID, PLAN_ID, BIZ_TYPE, REP_FLD, AREA,
                        LEAD_ORGAN_NM, MNG_ORGAN_NM, BIZ_SDT, BIZ_EDT,
                        RESPERIOD, CUR_RESPERIOD, LAST_GOAL, BIZ_CONTENTS, BIZ_CONTENTS_KEYWORD
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14
                    )
                """, (
                    row['DETAIL_ID'],
                    row['PLAN_ID'],
                    str(row['BIZ_TYPE'])[:768] if pd.notna(row['BIZ_TYPE']) else None,
                    str(row['REP_FLD'])[:768] if pd.notna(row['REP_FLD']) else None,
                    str(row['AREA'])[:768] if pd.notna(row['AREA']) else None,
                    str(row['LEAD_ORGAN_NM'])[:768] if pd.notna(row['LEAD_ORGAN_NM']) else None,
                    str(row['MNG_ORGAN_NM'])[:768] if pd.notna(row['MNG_ORGAN_NM']) else None,
                    None,  # BIZ_SDT
                    None,  # BIZ_EDT
                    None,  # RESPERIOD
                    None,  # CUR_RESPERIOD
                    str(row['LAST_GOAL'])[:4000] if pd.notna(row['LAST_GOAL']) else None,
                    str(row['BIZ_CONTENTS'])[:4000] if pd.notna(row['BIZ_CONTENTS']) else None,
                    None  # BIZ_CONTENTS_KEYWORD
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager.connection.commit()
        logger.info(f"  ✅ TB_PLAN_DETAIL 적재 완료: {inserted}건")
        self.load_stats['records_by_table']['TB_PLAN_DETAIL'] = inserted
        return inserted

    def load_tb_plan_budget(self) -> int:
        """TB_PLAN_BUDGET 적재"""
        logger.info("\n3️⃣ TB_PLAN_BUDGET 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_BUDGET.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        cursor = self.db_manager.connection.cursor()

        for idx, row in df.iterrows():
            try:
                budget_id = f"{row['PLAN_ID']}B{str(idx+1).zfill(3)}"
                cursor.execute("""
                    INSERT INTO TB_PLAN_BUDGET (
                        BUDGET_ID, PLAN_ID, BUDGET_YEAR, CATEGORY,
                        TOTAL_AMOUNT, GOV_AMOUNT, PRIVATE_AMOUNT,
                        LOCAL_AMOUNT, ETC_AMOUNT, PERFORM_PRC, PLAN_PRC,
                        REGIST_DT
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, SYSDATE
                    )
                """, (
                    budget_id,
                    row['PLAN_ID'],
                    int(row['BUDGET_YEAR']) if pd.notna(row['BUDGET_YEAR']) else None,
                    str(row['CATEGORY'])[:50] if pd.notna(row['CATEGORY']) else None,
                    float(row['TOTAL_AMOUNT']) if pd.notna(row['TOTAL_AMOUNT']) else None,
                    float(row['GOV_AMOUNT']) if pd.notna(row['GOV_AMOUNT']) else None,
                    float(row['PRIVATE_AMOUNT']) if pd.notna(row['PRIVATE_AMOUNT']) else None,
                    float(row['LOCAL_AMOUNT']) if pd.notna(row['LOCAL_AMOUNT']) else None,
                    float(row['ETC_AMOUNT']) if pd.notna(row['ETC_AMOUNT']) else None,
                    float(row['PERFORM_PRC']) if pd.notna(row['PERFORM_PRC']) else None,
                    float(row['PLAN_PRC']) if pd.notna(row['PLAN_PRC']) else None
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager.connection.commit()
        logger.info(f"  ✅ TB_PLAN_BUDGET 적재 완료: {inserted}건")
        self.load_stats['records_by_table']['TB_PLAN_BUDGET'] = inserted
        return inserted

    def load_tb_plan_schedule(self) -> int:
        """TB_PLAN_SCHEDULE 적재"""
        logger.info("\n4️⃣ TB_PLAN_SCHEDULE 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_SCHEDULE.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        cursor = self.db_manager.connection.cursor()

        for idx, row in df.iterrows():
            try:
                schedule_id = f"{row['PLAN_ID']}S{str(idx+1).zfill(3)}"
                cursor.execute("""
                    INSERT INTO TB_PLAN_SCHEDULE (
                        SCHEDULE_ID, PLAN_ID, SCHEDULE_YEAR, QUARTER,
                        TASK_NAME, TASK_CONTENT, START_DATE, END_DATE,
                        REGIST_DT
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6,
                        TO_DATE(:7, 'YYYY-MM-DD'),
                        TO_DATE(:8, 'YYYY-MM-DD'),
                        SYSDATE
                    )
                """, (
                    schedule_id,
                    row['PLAN_ID'],
                    int(row['SCHEDULE_YEAR']) if pd.notna(row['SCHEDULE_YEAR']) else None,
                    str(row['QUARTER'])[:50] if pd.notna(row['QUARTER']) else None,
                    str(row['TASK_NAME'])[:768] if pd.notna(row['TASK_NAME']) else None,
                    str(row['TASK_CONTENT'])[:4000] if pd.notna(row['TASK_CONTENT']) else None,
                    str(row['START_DATE']) if pd.notna(row['START_DATE']) else None,
                    str(row['END_DATE']) if pd.notna(row['END_DATE']) else None
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager.connection.commit()
        logger.info(f"  ✅ TB_PLAN_SCHEDULE 적재 완료: {inserted}건")
        self.load_stats['records_by_table']['TB_PLAN_SCHEDULE'] = inserted
        return inserted

    def load_tb_plan_performance(self) -> int:
        """TB_PLAN_PERFORMANCE 적재"""
        logger.info("\n5️⃣ TB_PLAN_PERFORMANCE 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_PERFORMANCE.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        cursor = self.db_manager.connection.cursor()

        for idx, row in df.iterrows():
            try:
                performance_id = f"{row['PLAN_ID']}P{str(idx+1).zfill(3)}"
                cursor.execute("""
                    INSERT INTO TB_PLAN_PERFORMANCE (
                        PERFORMANCE_ID, PLAN_ID, PERFORMANCE_YEAR,
                        PERFORMANCE_TYPE, CATEGORY, VALUE, UNIT,
                        ORIGINAL_TEXT, REGIST_DT
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, SYSDATE
                    )
                """, (
                    performance_id,
                    row['PLAN_ID'],
                    int(row['PERFORMANCE_YEAR']) if pd.notna(row['PERFORMANCE_YEAR']) else None,
                    str(row['PERFORMANCE_TYPE'])[:100] if pd.notna(row['PERFORMANCE_TYPE']) else None,
                    str(row['CATEGORY'])[:200] if pd.notna(row['CATEGORY']) else None,
                    float(row['VALUE']) if pd.notna(row['VALUE']) else None,
                    str(row['UNIT'])[:50] if pd.notna(row['UNIT']) else None,
                    str(row['ORIGINAL_TEXT'])[:4000] if pd.notna(row['ORIGINAL_TEXT']) else None
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager.connection.commit()
        logger.info(f"  ✅ TB_PLAN_PERFORMANCE 적재 완료: {inserted}건")
        self.load_stats['records_by_table']['TB_PLAN_PERFORMANCE'] = inserted
        return inserted

    def load_all_tables(self):
        """모든 테이블 적재"""
        logger.info("\n" + "="*80)
        logger.info("🚀 Oracle DB 적재 시작")
        logger.info("="*80)

        # 기존 데이터 삭제 (중복 방지)
        self.truncate_tables()

        self.load_tb_plan_master()
        self.load_tb_plan_detail()
        self.load_tb_plan_budget()
        self.load_tb_plan_schedule()
        self.load_tb_plan_performance()

        total = sum(self.load_stats['records_by_table'].values())
        self.load_stats['total_records'] = total

        logger.info("\n" + "="*80)
        logger.info("✅ 데이터 적재 완료")
        logger.info("="*80)
        logger.info(f"총 레코드: {total}건")
        logger.info(f"테이블별 레코드:")
        for table, count in self.load_stats['records_by_table'].items():
            logger.info(f"  • {table}: {count}건")


if __name__ == "__main__":
    from config import ORACLE_CONFIG

    loader = OracleDirectLoader(ORACLE_CONFIG, 'normalized_output_government')
    loader.connect()

    # 테이블 생성
    loader.create_tables()

    # 데이터 적재
    loader.load_all_tables()

    loader.db_manager.close()


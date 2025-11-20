"""
Oracle DB 적재 - TB_PLAN_DATA + 하위 테이블 4개
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any

from oracle_db_manager import OracleDBManager
from oracle_table_ddl import (
    TABLE_DEFINITIONS,
    TABLE_CREATE_ORDER
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDirectLoader:
    """Oracle 직접 적재 클래스 - TB_PLAN_DATA 기반"""

    def __init__(self, db_config_read: Dict[str, Any], db_config_write: Dict[str, Any], csv_dir: str):
        """
        Args:
            db_config_read: TB_PLAN_DATA 읽기용 DB 설정 (BICS)
            db_config_write: 하위 테이블 쓰기용 DB 설정 (BICS_DEV)
            csv_dir: CSV 파일 디렉토리
        """
        self.db_manager_read = OracleDBManager(db_config_read)   # 읽기용 (BICS)
        self.db_manager_write = OracleDBManager(db_config_write)  # 쓰기용 (BICS_DEV)
        self.csv_dir = Path(csv_dir)

        self.load_stats = {
            'tables_created': 0,
            'total_records': 0,
            'records_by_table': {},
            'errors': [],
            'matched': 0,
            'unmatched': 0,
            'diff_found': 0
        }

        # 기존 PLAN_DATA 캐시 (YEAR, DETAIL_BIZ_NM) -> PLAN_ID
        self.existing_plan_data = {}
        self.matching_report = []
        self.unmatched_records = []
        self.diff_records = []

    def connect(self):
        """Oracle 연결 (읽기용 + 쓰기용)"""
        self.db_manager_read.connect()
        self.db_manager_write.connect()
        return True
    def connect(self):
        """Oracle 연결 (읽기용 + 쓰기용)"""
        self.db_manager_read.connect()
        self.db_manager_write.connect()
        return True

    def create_tables(self):
        """테이블 생성"""
        logger.info("\n🏗️ Oracle 테이블 생성 중...")

        for table_name in TABLE_CREATE_ORDER:
            try:
                table_def = TABLE_DEFINITIONS[table_name]
                # DDL 실행
                self.db_manager_write.execute_ddl(table_def['ddl'])
                logger.info(f"  ✅ {table_name} 테이블 생성 완료")

                # 컬럼 주석 실행
                for comment_sql in table_def['comments']:
                    try:
                        self.db_manager_write.execute_ddl(comment_sql)
                    except Exception as e:
                        logger.debug(f"  주석 실행 실패 (무시): {e}")

                self.load_stats['tables_created'] += 1
            except Exception as e:
                logger.warning(f"  ⚠️ {table_name} 생성 실패 (이미 존재하거나 에러): {e}")

    def truncate_tables(self):
        """기존 데이터 삭제 (테이블 구조는 유지)"""
        logger.info("\n🗑️ 기존 데이터 삭제 중...")

        # 역순으로 DELETE (FK 제약 때문)
        delete_order = list(reversed(TABLE_CREATE_ORDER))

        deleted_count = 0
        cursor = self.db_manager_write.connection.cursor()

        for table_name in delete_order:
            try:
                # TRUNCATE 대신 DELETE 사용 (FK 제약조건 고려)
                cursor.execute(f"DELETE FROM {table_name}")
                deleted_rows = cursor.rowcount
                self.db_manager_write.connection.commit()
                logger.info(f"  ✅ {table_name} 데이터 삭제 완료 ({deleted_rows}건)")
                deleted_count += 1
            except Exception as e:
                error_msg = str(e)
                if "ORA-00942" in error_msg:
                    logger.debug(f"  ⏭️ {table_name} 테이블 없음 (건너뜀)")
                else:
                    logger.error(f"  ❌ {table_name} 삭제 실패: {error_msg}")

        cursor.close()
        if deleted_count > 0:
            logger.info(f"✅ {deleted_count}개 테이블 데이터 삭제 완료")
        return deleted_count

    def load_existing_plan_data(self):
        """기존 TB_PLAN_DATA 전체 조회 및 캐싱 (BICS 스키마에서 읽기)"""
        logger.info("\n📂 기존 TB_PLAN_DATA 조회 중...")

        cursor = self.db_manager_read.connection.cursor()

        try:
            cursor.execute("""
                SELECT 
                    PLAN_ID, YEAR, NUM, NATION_ORGAN_NM, DETAIL_BIZ_NM, BIZ_NM,
                    BIZ_TYPE, AREA, REP_FLD, LEAD_ORGAN_NM, MNG_ORGAN_NM,
                    RESPERIOD, CUR_RESPERIOD,
                    TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV,
                    CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV,
                    LAST_GOAL, BIZ_CONTENTS
                FROM TB_PLAN_DATA
                WHERE DELETE_YN = 'N'
                ORDER BY YEAR, NUM
            """)

            rows = cursor.fetchall()
            logger.info(f"  ✅ 기존 레코드: {len(rows)}건")

            # 캐시 생성: (YEAR, BIZ_NM, DETAIL_BIZ_NM) -> 전체 레코드
            for row in rows:
                year = row[1]
                detail_biz_nm = row[4] if row[4] else ""  # DETAIL_BIZ_NM (세부사업명)
                biz_nm = row[5] if row[5] else ""  # BIZ_NM (내역사업명)

                # 정규화: 공백 제거
                key = (year, biz_nm.strip(), detail_biz_nm.strip())

                self.existing_plan_data[key] = {
                    'PLAN_ID': row[0],
                    'YEAR': row[1],
                    'NUM': row[2],
                    'NATION_ORGAN_NM': row[3],
                    'DETAIL_BIZ_NM': row[4],
                    'BIZ_NM': row[5],
                    'BIZ_TYPE': row[6],
                    'AREA': row[7],
                    'REP_FLD': row[8],
                    'LEAD_ORGAN_NM': row[9],
                    'MNG_ORGAN_NM': row[10],
                    'RESPERIOD': row[11],
                    'CUR_RESPERIOD': row[12],
                    'TOTAL_RESPRC': row[13],
                    'TOTAL_RESPRC_GOV': row[14],
                    'TOTAL_RESPRC_CIV': row[15],
                    'CUR_RESPRC': row[16],
                    'CUR_RESPRC_GOV': row[17],
                    'CUR_RESPRC_CIV': row[18],
                    'LAST_GOAL': row[19],
                    'BIZ_CONTENTS': row[20]
                }

            logger.info(f"  ✅ 캐시 생성 완료: {len(self.existing_plan_data)}개 키")

        except Exception as e:
            logger.error(f"  ❌ 기존 데이터 조회 실패: {e}")
            raise
        finally:
            cursor.close()

    def copy_plan_data_to_dev(self):
        """BICS의 TB_PLAN_DATA를 BICS_DEV로 복사 (FK 제약조건용)"""
        logger.info("\n📋 BICS → BICS_DEV TB_PLAN_DATA 복사 중...")

        try:
            cursor_write = self.db_manager_write.connection.cursor()

            # 1. BICS_DEV에 TB_PLAN_DATA 테이블이 있는지 확인
            cursor_write.execute("""
                SELECT COUNT(*) FROM user_tables 
                WHERE table_name = 'TB_PLAN_DATA'
            """)
            table_exists = cursor_write.fetchone()[0] > 0

            if not table_exists:
                # 테이블이 없으면 생성
                logger.info("  📝 TB_PLAN_DATA 테이블 생성 중...")
                from oracle_table_ddl import TABLE_DEFINITIONS
                table_def = TABLE_DEFINITIONS['TB_PLAN_DATA']
                self.db_manager_write.execute_ddl(table_def['ddl'])
                logger.info("  ✅ TB_PLAN_DATA 테이블 생성 완료")

            # 2. 기존 데이터 삭제
            cursor_write.execute("DELETE FROM TB_PLAN_DATA WHERE DELETE_YN = 'N'")
            deleted = cursor_write.rowcount
            logger.info(f"  🗑️ 기존 데이터 삭제: {deleted}건")

            # 3. BICS에서 데이터 조회
            cursor_read = self.db_manager_read.connection.cursor()
            cursor_read.execute("""
                SELECT PLAN_ID, YEAR, NUM, NATION_ORGAN_NM, BIZ_NM, DETAIL_BIZ_NM,
                       BIZ_TYPE, AREA, REP_FLD, BIOLOGY_WEI, RED_WEI, GREEN_WEI, 
                       WHITE_WEI, FUSION_WEI, LEAD_ORGAN_NM, MNG_ORGAN_NM,
                       BIZ_SDT, BIZ_EDT, RESPERIOD, CUR_RESPERIOD,
                       TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV,
                       CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV,
                       LAST_GOAL, BIZ_CONTENTS, BIZ_CONTENTS_KEYWORD,
                       REGUL_WEI, WEI, PERFORM_PRC, PLAN_PRC
                FROM TB_PLAN_DATA
                WHERE DELETE_YN = 'N'
            """)

            rows = cursor_read.fetchall()
            logger.info(f"  📥 BICS에서 {len(rows)}건 조회")

            # 4. BICS_DEV에 INSERT
            insert_sql = """
                INSERT INTO TB_PLAN_DATA (
                    PLAN_ID, YEAR, NUM, NATION_ORGAN_NM, BIZ_NM, DETAIL_BIZ_NM,
                    BIZ_TYPE, AREA, REP_FLD, BIOLOGY_WEI, RED_WEI, GREEN_WEI,
                    WHITE_WEI, FUSION_WEI, LEAD_ORGAN_NM, MNG_ORGAN_NM,
                    BIZ_SDT, BIZ_EDT, RESPERIOD, CUR_RESPERIOD,
                    TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV,
                    CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV,
                    LAST_GOAL, BIZ_CONTENTS, BIZ_CONTENTS_KEYWORD,
                    REGUL_WEI, WEI, PERFORM_PRC, PLAN_PRC,
                    REGIST_DT, DELETE_YN
                ) VALUES (
                    :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14,
                    :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26,
                    :27, :28, :29, :30, :31, :32, :33, SYSDATE, 'N'
                )
            """

            cursor_write.executemany(insert_sql, rows)
            self.db_manager_write.connection.commit()

            logger.info(f"  ✅ BICS_DEV에 {len(rows)}건 복사 완료")

            cursor_read.close()
            cursor_write.close()

        except Exception as e:
            logger.error(f"  ❌ TB_PLAN_DATA 복사 실패: {e}")
            raise


    def match_plan_id(self, csv_row: pd.Series) -> Dict[str, Any]:
        """
        CSV 레코드를 기존 TB_PLAN_DATA와 매칭

        ✅ 매칭 우선순위:
        1순위: CSV에 이미 유효한 PLAN_ID가 있으면 그대로 사용
        2순위: PLAN_ID가 없거나 TEMP_로 시작하면 매칭 시도

        매칭 기준: YEAR + BIZ_NM(내역사업명) + DETAIL_BIZ_NM(세부사업명)

        Returns:
            {
                'matched': bool,
                'plan_id': str or None,
                'has_diff': bool,
                'diff_details': dict
            }
        """
        # ✅ 1순위: CSV에 이미 유효한 PLAN_ID가 있는지 확인
        csv_plan_id = str(csv_row['PLAN_ID']).strip() if pd.notna(csv_row['PLAN_ID']) and csv_row['PLAN_ID'] else None

        if csv_plan_id and not csv_plan_id.startswith('TEMP_') and csv_plan_id != '':
            # 유효한 PLAN_ID가 이미 있으면 그대로 사용
            return {
                'matched': True,
                'plan_id': csv_plan_id,
                'has_diff': False,
                'diff_details': {},
                'reason': 'CSV에 이미 매칭된 PLAN_ID 존재'
            }

        # 2순위: 매칭 시도 (PLAN_ID가 없거나 TEMP_로 시작하는 경우)
        year = int(csv_row['YEAR']) if pd.notna(csv_row['YEAR']) else None
        biz_nm = str(csv_row['BIZ_NM']).strip() if pd.notna(csv_row['BIZ_NM']) else ""
        detail_biz_nm = str(csv_row['DETAIL_BIZ_NM']).strip() if pd.notna(csv_row['DETAIL_BIZ_NM']) else ""

        if not year or not biz_nm:
            return {
                'matched': False,
                'plan_id': None,
                'has_diff': False,
                'diff_details': {},
                'reason': 'YEAR 또는 BIZ_NM 누락'
            }

        # 매칭 시도: YEAR + BIZ_NM + DETAIL_BIZ_NM
        key = (year, biz_nm, detail_biz_nm)
        existing = self.existing_plan_data.get(key)

        if not existing:
            return {
                'matched': False,
                'plan_id': None,
                'has_diff': False,
                'diff_details': {},
                'reason': f'기존 데이터에 없음: {year}년 - {biz_nm} - {detail_biz_nm}'
            }

        # 매칭 성공! 차이점 확인
        diff_details = {}

        # 비교할 필드들
        compare_fields = [
            'NATION_ORGAN_NM', 'BIZ_TYPE', 'AREA', 'REP_FLD',
            'LEAD_ORGAN_NM', 'MNG_ORGAN_NM', 'RESPERIOD',
            'TOTAL_RESPRC_GOV', 'TOTAL_RESPRC_CIV',
            'LAST_GOAL', 'BIZ_CONTENTS'
        ]

        for field in compare_fields:
            csv_val = str(csv_row[field]).strip() if pd.notna(csv_row.get(field)) else ""
            db_val = str(existing.get(field)).strip() if existing.get(field) else ""

            if csv_val and db_val and csv_val != db_val:
                diff_details[field] = {
                    'csv': csv_val[:100],  # 최대 100자
                    'db': db_val[:100]
                }

        return {
            'matched': True,
            'plan_id': existing['PLAN_ID'],
            'has_diff': len(diff_details) > 0,
            'diff_details': diff_details,
            'reason': 'SUCCESS'
        }

    def process_matching(self) -> Dict[int, str]:
        """
        CSV 데이터와 기존 TB_PLAN_DATA 매칭
        Returns: {csv_index(int): plan_id(str)}
        """
        logger.info("\n🔍 PLAN_ID 매칭 시작...")

        csv_file = self.csv_dir / "TB_PLAN_DATA.csv"
        if not csv_file.exists():
            logger.error(f"❌ CSV 파일 없음: {csv_file}")
            return {}

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 처리할 레코드: {len(df)}건")

        plan_id_mapping: Dict[int, str] = {}  # 타입 명시

        for idx, row in df.iterrows():
            result = self.match_plan_id(row)

            match_record = {
                'csv_index': int(idx),  # type: ignore
                'year': row.get('YEAR'),
                'biz_nm': row.get('BIZ_NM'),  # 내역사업명
                'detail_biz_nm': row.get('DETAIL_BIZ_NM'),  # 세부사업명
                'matched': result['matched'],
                'plan_id': result['plan_id'],
                'has_diff': result.get('has_diff', False),
                'reason': result.get('reason', '')
            }

            self.matching_report.append(match_record)

            if result['matched']:
                plan_id_mapping[int(idx)] = result['plan_id']  # type: ignore
                self.load_stats['matched'] += 1

                if result['has_diff']:
                    self.load_stats['diff_found'] += 1
                    self.diff_records.append({
                        'csv_index': idx,
                        'plan_id': result['plan_id'],
                        'year': row.get('YEAR'),
                        'detail_biz_nm': row.get('DETAIL_BIZ_NM'),
                        'diffs': result['diff_details']
                    })
            else:
                self.load_stats['unmatched'] += 1
                self.unmatched_records.append(match_record)

        logger.info(f"  ✅ 매칭 완료:")
        logger.info(f"     - 성공: {self.load_stats['matched']}건")
        logger.info(f"     - 실패: {self.load_stats['unmatched']}건")
        logger.info(f"     - 차이점 발견: {self.load_stats['diff_found']}건")

        return plan_id_mapping

    def update_csv_with_plan_ids(self, plan_id_mapping: Dict[int, str]):
        """
        매칭된 PLAN_ID를 모든 CSV에 업데이트

        Args:
            plan_id_mapping: {csv_index: plan_id}
        """
        logger.info("\n📝 매칭된 PLAN_ID를 CSV에 업데이트 중...")

        # 1. TB_PLAN_DATA 업데이트
        plan_data_file = self.csv_dir / "TB_PLAN_DATA.csv"
        if plan_data_file.exists():
            df = pd.read_csv(plan_data_file, encoding='utf-8-sig')

            for idx, plan_id in plan_id_mapping.items():
                if idx < len(df):
                    df.at[idx, 'PLAN_ID'] = plan_id

            df.to_csv(plan_data_file, index=False, encoding='utf-8-sig')
            logger.info(f"  ✅ TB_PLAN_DATA.csv 업데이트 완료 ({len(plan_id_mapping)}건)")

        # 2. 하위 테이블 업데이트 (TB_PLAN_BUDGET, SCHEDULE, PERFORMANCE, ACHIEVEMENTS)
        # CSV의 _internal_id를 기반으로 PLAN_ID 매핑 (정규화 단계에서 sub_project_id로 연결)
        # 실제로는 TB_PLAN_DATA의 index와 하위 테이블이 직접 연결되지 않으므로,
        # 하위 테이블의 빈 PLAN_ID를 채우는 방식이 필요합니다.

        # 전략: TB_PLAN_DATA의 (YEAR, BIZ_NM, DETAIL_BIZ_NM)으로 역매핑
        year_biz_to_plan_id = {}
        plan_data_df = pd.read_csv(plan_data_file, encoding='utf-8-sig')

        for _, row in plan_data_df.iterrows():
            if pd.notna(row['PLAN_ID']) and row['PLAN_ID']:
                year = row['YEAR']
                biz_nm = str(row['BIZ_NM']).strip() if pd.notna(row['BIZ_NM']) else ""
                detail_biz_nm = str(row['DETAIL_BIZ_NM']).strip() if pd.notna(row['DETAIL_BIZ_NM']) else ""

                if year and biz_nm:
                    key = (int(year), biz_nm, detail_biz_nm)
                    year_biz_to_plan_id[key] = row['PLAN_ID']

        logger.info(f"  📋 PLAN_ID 매핑 테이블 생성: {len(year_biz_to_plan_id)}개")

        # 하위 테이블 파일들
        sub_tables = [
            'TB_PLAN_BUDGET',
            'TB_PLAN_SCHEDULE',
            'TB_PLAN_PERFORMANCE',
            'TB_PLAN_ACHIEVEMENTS'
        ]

        updated_counts = {}

        for table_name in sub_tables:
            csv_file = self.csv_dir / f"{table_name}.csv"
            if not csv_file.exists():
                continue

            df = pd.read_csv(csv_file, encoding='utf-8-sig')

            # PLAN_ID가 빈 문자열인 경우만 업데이트 필요
            # 하지만 하위 테이블에는 YEAR, BIZ_NM, DETAIL_BIZ_NM이 없으므로
            # 정규화 단계에서 이미 연결되어 있어야 함

            # ⚠️ 현재 하위 테이블에는 PLAN_ID만 있고 매칭 키가 없음!
            # 해결책: 정규화 단계에서 _internal_id를 보존하거나,
            # CSV에 임시로 매칭 키를 추가해야 함

            # 임시 해결: 하위 테이블도 YEAR 정보가 있다면 활용
            if 'BUDGET_YEAR' in df.columns:  # TB_PLAN_BUDGET
                # 예산 테이블은 BUDGET_YEAR가 있음
                # 하지만 BIZ_NM이 없어서 매칭 불가...
                pass
            elif 'SCHEDULE_YEAR' in df.columns:  # TB_PLAN_SCHEDULE
                pass
            elif 'PERFORMANCE_YEAR' in df.columns:  # TB_PLAN_PERFORMANCE
                pass

            # ⚠️ 근본적 문제: 하위 테이블에 매칭 키가 없음!
            logger.warning(f"  ⚠️ {table_name}: 매칭 키 없음 (PLAN_ID 업데이트 불가)")
            updated_counts[table_name] = 0

        logger.info(f"  ⚠️ 하위 테이블 업데이트 실패: 매칭 키 부족")
        logger.info(f"     → 해결 방법: 정규화 단계에서 sub_project_id 보존 필요")

    def save_reports(self):
        """매칭 리포트 저장"""
        logger.info("\n📊 리포트 생성 중...")

        report_dir = self.csv_dir / "matching_reports"
        report_dir.mkdir(exist_ok=True)

        # 1. 전체 매칭 리포트
        if self.matching_report:
            df_report = pd.DataFrame(self.matching_report)
            report_file = report_dir / "matching_report.csv"
            df_report.to_csv(report_file, index=False, encoding='utf-8-sig')
            logger.info(f"  ✅ 매칭 리포트: {report_file}")

        # 2. 매칭 실패 목록
        if self.unmatched_records:
            df_unmatched = pd.DataFrame(self.unmatched_records)
            unmatched_file = report_dir / "unmatched_records.csv"
            df_unmatched.to_csv(unmatched_file, index=False, encoding='utf-8-sig')
            logger.info(f"  ⚠️ 매칭 실패: {unmatched_file} ({len(self.unmatched_records)}건)")

        # 3. 차이점 발견 목록
        if self.diff_records:
            diff_data = []
            for record in self.diff_records:
                base = {
                    'csv_index': record['csv_index'],
                    'plan_id': record['plan_id'],
                    'year': record['year'],
                    'detail_biz_nm': record['detail_biz_nm']
                }
                for field, diff in record['diffs'].items():
                    diff_data.append({
                        **base,
                        'field': field,
                        'csv_value': diff['csv'],
                        'db_value': diff['db']
                    })

            df_diff = pd.DataFrame(diff_data)
            diff_file = report_dir / "diff_report.csv"
            df_diff.to_csv(diff_file, index=False, encoding='utf-8-sig')
            logger.info(f"  🔍 차이점 발견: {diff_file} ({len(self.diff_records)}건)")

    def load_tb_plan_data(self) -> int:
        """TB_PLAN_DATA 적재"""
        logger.info("\n1️⃣ TB_PLAN_DATA 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_DATA.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        cursor = self.db_manager_write.connection.cursor()

        for idx, row in df.iterrows():
            try:
                # 데이터 준비
                plan_id = str(row['PLAN_ID']) if pd.notna(row['PLAN_ID']) else None
                year = int(row['YEAR']) if pd.notna(row['YEAR']) else None
                num = int(row['NUM']) if pd.notna(row['NUM']) else None

                # MERGE: 중복 시 UPDATE, 없으면 INSERT
                cursor.execute("""
                    MERGE INTO TB_PLAN_DATA tgt
                    USING (
                        SELECT
                            :1 AS PLAN_ID, :2 AS YEAR, :3 AS NUM, :4 AS NATION_ORGAN_NM,
                            :5 AS DETAIL_BIZ_NM, :6 AS BIZ_NM, :7 AS BIZ_TYPE, :8 AS AREA,
                            :9 AS REP_FLD, :10 AS BIOLOGY_WEI, :11 AS RED_WEI, :12 AS GREEN_WEI,
                            :13 AS WHITE_WEI, :14 AS FUSION_WEI, :15 AS LEAD_ORGAN_NM, :16 AS MNG_ORGAN_NM,
                            :17 AS BIZ_SDT, :18 AS BIZ_EDT, :19 AS RESPERIOD, :20 AS CUR_RESPERIOD,
                            :21 AS TOTAL_RESPRC, :22 AS TOTAL_RESPRC_GOV, :23 AS TOTAL_RESPRC_CIV,
                            :24 AS CUR_RESPRC, :25 AS CUR_RESPRC_GOV, :26 AS CUR_RESPRC_CIV,
                            :27 AS LAST_GOAL, :28 AS BIZ_CONTENTS, :29 AS BIZ_CONTENTS_KEYWORD,
                            :30 AS REGUL_WEI, :31 AS WEI, :32 AS PERFORM_PRC, :33 AS PLAN_PRC
                        FROM dual
                    ) src
                    ON (tgt.PLAN_ID = src.PLAN_ID)
                    WHEN MATCHED THEN
                        UPDATE SET
                            tgt.YEAR = src.YEAR,
                            tgt.NUM = src.NUM,
                            tgt.NATION_ORGAN_NM = src.NATION_ORGAN_NM,
                            tgt.DETAIL_BIZ_NM = src.DETAIL_BIZ_NM,
                            tgt.BIZ_NM = src.BIZ_NM,
                            tgt.BIZ_TYPE = src.BIZ_TYPE,
                            tgt.AREA = src.AREA,
                            tgt.REP_FLD = src.REP_FLD,
                            tgt.BIOLOGY_WEI = src.BIOLOGY_WEI,
                            tgt.RED_WEI = src.RED_WEI,
                            tgt.GREEN_WEI = src.GREEN_WEI,
                            tgt.WHITE_WEI = src.WHITE_WEI,
                            tgt.FUSION_WEI = src.FUSION_WEI,
                            tgt.LEAD_ORGAN_NM = src.LEAD_ORGAN_NM,
                            tgt.MNG_ORGAN_NM = src.MNG_ORGAN_NM,
                            tgt.BIZ_SDT = src.BIZ_SDT,
                            tgt.BIZ_EDT = src.BIZ_EDT,
                            tgt.RESPERIOD = src.RESPERIOD,
                            tgt.CUR_RESPERIOD = src.CUR_RESPERIOD,
                            tgt.TOTAL_RESPRC = src.TOTAL_RESPRC,
                            tgt.TOTAL_RESPRC_GOV = src.TOTAL_RESPRC_GOV,
                            tgt.TOTAL_RESPRC_CIV = src.TOTAL_RESPRC_CIV,
                            tgt.CUR_RESPRC = src.CUR_RESPRC,
                            tgt.CUR_RESPRC_GOV = src.CUR_RESPRC_GOV,
                            tgt.CUR_RESPRC_CIV = src.CUR_RESPRC_CIV,
                            tgt.LAST_GOAL = src.LAST_GOAL,
                            tgt.BIZ_CONTENTS = src.BIZ_CONTENTS,
                            tgt.BIZ_CONTENTS_KEYWORD = src.BIZ_CONTENTS_KEYWORD,
                            tgt.REGUL_WEI = src.REGUL_WEI,
                            tgt.WEI = src.WEI,
                            tgt.PERFORM_PRC = src.PERFORM_PRC,
                            tgt.PLAN_PRC = src.PLAN_PRC,
                            tgt.MODIFY_DT = SYSDATE,
                            tgt.MODIFY_ID = 'SYSTEM'
                    WHEN NOT MATCHED THEN
                        INSERT (
                            PLAN_ID, YEAR, NUM, NATION_ORGAN_NM, DETAIL_BIZ_NM, BIZ_NM,
                            BIZ_TYPE, AREA, REP_FLD,
                            BIOLOGY_WEI, RED_WEI, GREEN_WEI, WHITE_WEI, FUSION_WEI,
                            LEAD_ORGAN_NM, MNG_ORGAN_NM, BIZ_SDT, BIZ_EDT,
                            RESPERIOD, CUR_RESPERIOD,
                            TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV,
                            CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV,
                            LAST_GOAL, BIZ_CONTENTS, BIZ_CONTENTS_KEYWORD,
                            REGIST_DT, DELETE_YN, REGIST_ID,
                            REGUL_WEI, WEI, PERFORM_PRC, PLAN_PRC
                        ) VALUES (
                            src.PLAN_ID, src.YEAR, src.NUM, src.NATION_ORGAN_NM, src.DETAIL_BIZ_NM, src.BIZ_NM,
                            src.BIZ_TYPE, src.AREA, src.REP_FLD,
                            src.BIOLOGY_WEI, src.RED_WEI, src.GREEN_WEI, src.WHITE_WEI, src.FUSION_WEI,
                            src.LEAD_ORGAN_NM, src.MNG_ORGAN_NM, src.BIZ_SDT, src.BIZ_EDT,
                            src.RESPERIOD, src.CUR_RESPERIOD,
                            src.TOTAL_RESPRC, src.TOTAL_RESPRC_GOV, src.TOTAL_RESPRC_CIV,
                            src.CUR_RESPRC, src.CUR_RESPRC_GOV, src.CUR_RESPRC_CIV,
                            src.LAST_GOAL, src.BIZ_CONTENTS, src.BIZ_CONTENTS_KEYWORD,
                            SYSDATE, 'N', 'SYSTEM',
                            src.REGUL_WEI, src.WEI, src.PERFORM_PRC, src.PLAN_PRC
                        )
                """, (
                    plan_id,
                    year,
                    num,
                    str(row['NATION_ORGAN_NM'])[:768] if pd.notna(row['NATION_ORGAN_NM']) else None,
                    str(row['DETAIL_BIZ_NM'])[:768] if pd.notna(row['DETAIL_BIZ_NM']) else None,
                    str(row['BIZ_NM'])[:768] if pd.notna(row['BIZ_NM']) else None,
                    str(row['BIZ_TYPE'])[:768] if pd.notna(row['BIZ_TYPE']) else None,
                    str(row['AREA'])[:768] if pd.notna(row['AREA']) else None,
                    str(row['REP_FLD'])[:768] if pd.notna(row['REP_FLD']) else None,
                    float(row['BIOLOGY_WEI']) if pd.notna(row['BIOLOGY_WEI']) else None,
                    float(row['RED_WEI']) if pd.notna(row['RED_WEI']) else None,
                    float(row['GREEN_WEI']) if pd.notna(row['GREEN_WEI']) else None,
                    float(row['WHITE_WEI']) if pd.notna(row['WHITE_WEI']) else None,
                    float(row['FUSION_WEI']) if pd.notna(row['FUSION_WEI']) else None,
                    str(row['LEAD_ORGAN_NM'])[:768] if pd.notna(row['LEAD_ORGAN_NM']) else None,
                    str(row['MNG_ORGAN_NM'])[:768] if pd.notna(row['MNG_ORGAN_NM']) else None,
                    None,  # BIZ_SDT
                    None,  # BIZ_EDT
                    str(row['RESPERIOD'])[:768] if pd.notna(row['RESPERIOD']) else None,
                    str(row['CUR_RESPERIOD'])[:768] if pd.notna(row['CUR_RESPERIOD']) else None,
                    str(row['TOTAL_RESPRC'])[:768] if pd.notna(row['TOTAL_RESPRC']) else None,
                    float(row['TOTAL_RESPRC_GOV']) if pd.notna(row['TOTAL_RESPRC_GOV']) else None,
                    float(row['TOTAL_RESPRC_CIV']) if pd.notna(row['TOTAL_RESPRC_CIV']) else None,
                    str(row['CUR_RESPRC'])[:768] if pd.notna(row['CUR_RESPRC']) else None,
                    float(row['CUR_RESPRC_GOV']) if pd.notna (row['CUR_RESPRC_GOV']) else None,
                    float(row['CUR_RESPRC_CIV']) if pd.notna(row['CUR_RESPRC_CIV']) else None,
                    str(row['LAST_GOAL'])[:4000] if pd.notna(row['LAST_GOAL']) else None,
                    str(row['BIZ_CONTENTS'])[:4000] if pd.notna(row['BIZ_CONTENTS']) else None,
                    str(row['BIZ_CONTENTS_KEYWORD'])[:4000] if pd.notna(row['BIZ_CONTENTS_KEYWORD']) else None,
                    float(row['REGUL_WEI']) if pd.notna(row['REGUL_WEI']) else None,
                    str(row['WEI'])[:768] if pd.notna(row['WEI']) else None,
                    float(row['PERFORM_PRC']) if pd.notna(row['PERFORM_PRC']) else None,
                    float(row['PLAN_PRC']) if pd.notna(row['PLAN_PRC']) else None,
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager_write.connection.commit()
        logger.info(f"  ✅ TB_PLAN_DATA 적재 완료: {inserted}건")
        self.load_stats['records_by_table']['TB_PLAN_DATA'] = inserted
        return inserted

    def load_child_tables_with_mapping(self, plan_id_mapping: Dict[int, str]):
        """
        매칭된 PLAN_ID를 사용하여 하위 테이블 적재

        Args:
            plan_id_mapping: {csv_index: plan_id}
        """
        logger.info("\n" + "="*80)
        logger.info("🚀 하위 테이블 적재 시작 (매칭된 PLAN_ID 사용)")
        logger.info("="*80)

        # 각 하위 테이블 적재
        self.load_tb_plan_budget_with_mapping(plan_id_mapping)
        self.load_tb_plan_schedule_with_mapping(plan_id_mapping)
        self.load_tb_plan_performance_with_mapping(plan_id_mapping)
        self.load_tb_plan_achievements_with_mapping(plan_id_mapping)

        total = sum(self.load_stats['records_by_table'].values())
        self.load_stats['total_records'] = total

        logger.info("\n" + "="*80)
        logger.info("✅ 하위 테이블 적재 완료")
        logger.info("="*80)
        logger.info(f"총 레코드: {total}건")
        logger.info(f"테이블별 레코드:")
        for table, count in self.load_stats['records_by_table'].items():
            logger.info(f"  • {table}: {count}건")

    def load_tb_plan_budget_with_mapping(self, plan_id_mapping: Dict[int, str]) -> int:
        """TB_PLAN_BUDGET 적재 (매칭된 PLAN_ID 사용)"""
        logger.info("\n2️⃣ TB_PLAN_BUDGET 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_BUDGET.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        skipped = 0
        cursor = self.db_manager_write.connection.cursor()

        # 매칭 키 컬럼 존재 여부 확인
        has_matching_keys = 'BIZ_NM' in df.columns and 'DETAIL_BIZ_NM' in df.columns and 'DOC_YEAR' in df.columns

        for idx, row in df.iterrows():
            try:
                # ✅ 1순위: CSV에 있는 PLAN_ID 사용 (정규화 단계에서 매칭된 경우)
                plan_id = str(row['PLAN_ID']).strip() if pd.notna(row['PLAN_ID']) and row['PLAN_ID'] else None

                # PLAN_ID가 없거나 "TEMP_"로 시작하면 매칭 시도
                if not plan_id or plan_id.startswith('TEMP_'):
                    if has_matching_keys:
                        biz_nm = str(row['BIZ_NM']).strip() if pd.notna(row['BIZ_NM']) else ""
                        detail_biz_nm = str(row['DETAIL_BIZ_NM']).strip() if pd.notna(row['DETAIL_BIZ_NM']) else ""
                        doc_year = int(row['DOC_YEAR']) if pd.notna(row['DOC_YEAR']) else None

                        if biz_nm and doc_year:
                            # 기존 PLAN_DATA에서 매칭
                            key = (doc_year, biz_nm, detail_biz_nm)
                            existing = self.existing_plan_data.get(key)

                            if existing:
                                plan_id = existing['PLAN_ID']
                                logger.debug(f"  🔍 매칭 성공: {biz_nm} -> {plan_id}")
                            else:
                                logger.warning(f"  ⚠️ 행 {idx} 건너뜀: 매칭 실패 ({doc_year}년 - {biz_nm})")
                                skipped += 1
                                continue
                        else:
                            logger.warning(f"  ⚠️ 행 {idx} 건너뜀: BIZ_NM 또는 DOC_YEAR 누락")
                            skipped += 1
                            continue
                    else:
                        logger.warning(f"  ⚠️ 행 {idx} 건너뜀: 매칭 키 없음")
                        skipped += 1
                        continue

                # PLAN_ID 검증 (최종 확인)
                if not plan_id or plan_id.startswith('TEMP_'):
                    logger.warning(f"  ⚠️ 행 {idx} 건너뜀: 유효한 PLAN_ID 없음")
                    skipped += 1
                    continue

                budget_id = f"{plan_id}B{str(inserted+1).zfill(3)}"

                # INSERT (중복 시 무시)
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
                    budget_id,                                                                         # :1
                    plan_id,                                                                          # :2
                    int(row['BUDGET_YEAR']) if pd.notna(row['BUDGET_YEAR']) else None,              # :3
                    str(row['CATEGORY'])[:50] if pd.notna(row['CATEGORY']) else None,               # :4
                    float(row['TOTAL_AMOUNT']) if pd.notna(row['TOTAL_AMOUNT']) else None,          # :5
                    float(row['GOV_AMOUNT']) if pd.notna(row['GOV_AMOUNT']) else None,              # :6
                    float(row['PRIVATE_AMOUNT']) if pd.notna(row['PRIVATE_AMOUNT']) else None,      # :7
                    float(row['LOCAL_AMOUNT']) if pd.notna(row['LOCAL_AMOUNT']) else None,          # :8
                    float(row['ETC_AMOUNT']) if pd.notna(row['ETC_AMOUNT']) else None,              # :9
                    float(row['PERFORM_PRC']) if pd.notna(row['PERFORM_PRC']) else None,            # :10
                    float(row['PLAN_PRC']) if pd.notna(row['PLAN_PRC']) else None                   # :11
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager_write.connection.commit()
        cursor.close()
        logger.info(f"  ✅ TB_PLAN_BUDGET 적재 완료: {inserted}건 (건너뜀: {skipped}건)")
        self.load_stats['records_by_table']['TB_PLAN_BUDGET'] = inserted
        return inserted

    def load_tb_plan_schedule_with_mapping(self, plan_id_mapping: Dict[int, str]) -> int:
        """TB_PLAN_SCHEDULE 적재 (매칭된 PLAN_ID 사용)"""
        logger.info("\n3️⃣ TB_PLAN_SCHEDULE 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_SCHEDULE.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        skipped = 0
        cursor = self.db_manager_write.connection.cursor()

        for idx, row in df.iterrows():
            try:
                # ✅ CSV에 있는 PLAN_ID 직접 사용
                plan_id = str(row['PLAN_ID']).strip() if pd.notna(row['PLAN_ID']) and row['PLAN_ID'] else None

                if not plan_id or plan_id.startswith('TEMP_'):
                    logger.warning(f"  ⚠️ 행 {idx} 건너뜀: 유효한 PLAN_ID 없음")
                    skipped += 1
                    continue

                schedule_id = f"{plan_id}S{str(inserted+1).zfill(3)}"

                # INSERT
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
                    schedule_id,                                                                      # :1
                    plan_id,                                                                          # :2
                    int(row['SCHEDULE_YEAR']) if pd.notna(row['SCHEDULE_YEAR']) else None,          # :3
                    str(row['QUARTER'])[:50] if pd.notna(row['QUARTER']) else None,                 # :4
                    str(row['TASK_NAME'])[:768] if pd.notna(row['TASK_NAME']) else None,            # :5
                    str(row['TASK_CONTENT'])[:4000] if pd.notna(row['TASK_CONTENT']) else None,     # :6
                    str(row['START_DATE']) if pd.notna(row['START_DATE']) else None,                # :7
                    str(row['END_DATE']) if pd.notna(row['END_DATE']) else None                     # :8
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager_write.connection.commit()
        logger.info(f"  ✅ TB_PLAN_SCHEDULE 적재 완료: {inserted}건 (건너뜀: {skipped}건)")
        self.load_stats['records_by_table']['TB_PLAN_SCHEDULE'] = inserted
        return inserted

    def load_tb_plan_performance_with_mapping(self, plan_id_mapping: Dict[int, str]) -> int:
        """TB_PLAN_PERFORMANCE 적재 (매칭된 PLAN_ID 사용)"""
        logger.info("\n4️⃣ TB_PLAN_PERFORMANCE 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_PERFORMANCE.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        skipped = 0
        cursor = self.db_manager_write.connection.cursor()

        for idx, row in df.iterrows():
            try:
                # ✅ CSV에 있는 PLAN_ID 직접 사용
                plan_id = str(row['PLAN_ID']).strip() if pd.notna(row['PLAN_ID']) and row['PLAN_ID'] else None

                if not plan_id or plan_id.startswith('TEMP_'):
                    logger.warning(f"  ⚠️ 행 {idx} 건너뜀: 유효한 PLAN_ID 없음")
                    skipped += 1
                    continue

                performance_id = f"{plan_id}P{str(inserted+1).zfill(3)}"

                # INSERT
                cursor.execute("""
                    INSERT INTO TB_PLAN_PERFORMANCE (
                        PERFORMANCE_ID, PLAN_ID, PERFORMANCE_YEAR,
                        PERFORMANCE_TYPE, CATEGORY, VALUE, UNIT,
                        ORIGINAL_TEXT, REGIST_DT
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, SYSDATE
                    )
                """, (
                    performance_id,                                                                          # :1
                    plan_id,                                                                                 # :2
                    int(row['PERFORMANCE_YEAR']) if pd.notna(row['PERFORMANCE_YEAR']) else None,           # :3
                    str(row['PERFORMANCE_TYPE'])[:100] if pd.notna(row['PERFORMANCE_TYPE']) else None,     # :4
                    str(row['CATEGORY'])[:200] if pd.notna(row['CATEGORY']) else None,                     # :5
                    float(row['VALUE']) if pd.notna(row['VALUE']) else None,                               # :6
                    str(row['UNIT'])[:50] if pd.notna(row['UNIT']) else None,                              # :7
                    str(row['ORIGINAL_TEXT'])[:4000] if pd.notna(row['ORIGINAL_TEXT']) else None           # :8
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager_write.connection.commit()
        logger.info(f"  ✅ TB_PLAN_PERFORMANCE 적재 완료: {inserted}건 (건너뜀: {skipped}건)")
        self.load_stats['records_by_table']['TB_PLAN_PERFORMANCE'] = inserted
        return inserted

    def load_tb_plan_achievements_with_mapping(self, plan_id_mapping: Dict[int, str]) -> int:
        """TB_PLAN_ACHIEVEMENTS 적재 (매칭된 PLAN_ID 사용)"""
        logger.info("\n5️⃣ TB_PLAN_ACHIEVEMENTS 적재 중...")

        csv_file = self.csv_dir / "TB_PLAN_ACHIEVEMENTS.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ CSV 파일 없음: {csv_file}")
            return 0

        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        logger.info(f"  📄 로드된 레코드: {len(df)}건")

        inserted = 0
        skipped = 0
        cursor = self.db_manager_write.connection.cursor()

        for idx, row in df.iterrows():
            try:
                # ✅ CSV에 있는 PLAN_ID 직접 사용
                plan_id = str(row['PLAN_ID']).strip() if pd.notna(row['PLAN_ID']) and row['PLAN_ID'] else None

                if not plan_id or plan_id.startswith('TEMP_'):
                    skipped += 1
                    continue

                achievement_id = f"{plan_id}A{str(inserted+1).zfill(3)}"

                # INSERT
                cursor.execute("""
                    INSERT INTO TB_PLAN_ACHIEVEMENTS (
                        ACHIEVEMENT_ID, PLAN_ID, ACHIEVEMENT_YEAR,
                        ACHIEVEMENT_ORDER, DESCRIPTION, REGIST_DT
                    ) VALUES (
                        :1, :2, :3, :4, :5, SYSDATE
                    )
                """, (
                    achievement_id,                                                                      # :1
                    plan_id,                                                                             # :2
                    int(row['ACHIEVEMENT_YEAR']) if pd.notna(row['ACHIEVEMENT_YEAR']) else None,       # :3
                    idx + 1,                                                                             # :4 (순서는 idx 사용)
                    str(row['DESCRIPTION'])[:4000] if pd.notna(row['DESCRIPTION']) else None           # :5
                ))
                inserted += 1
            except Exception as e:
                logger.error(f"❌ 행 {idx} 삽입 실패: {e}")

        self.db_manager_write.connection.commit()
        logger.info(f"  ✅ TB_PLAN_ACHIEVEMENTS 적재 완료: {inserted}건 (건너뜀: {skipped}건)")
        self.load_stats['records_by_table']['TB_PLAN_ACHIEVEMENTS'] = inserted
        return inserted

    def load_all_tables(self):
        """모든 테이블 적재 (레거시 - 사용 중단 예정)"""
        logger.warning("⚠️ load_all_tables()는 deprecated입니다. load_with_matching()을 사용하세요.")
        logger.info("\n" + "="*80)
        logger.info("🚀 Oracle DB 적재 시작")
        logger.info("="*80)

        # 기존 데이터 삭제 (중복 방지)
        self.truncate_tables()

        # 순서 보장: 부모 테이블 먼저 적재
        logger.info("\n📌 부모 테이블(TB_PLAN_DATA) 적재...")
        self.load_tb_plan_data()

        logger.info("\n📌 하위 테이블 적재...")
        # 주의: 이 메서드들은 이제 _with_mapping으로 대체되었습니다
        # 여기서는 빈 매핑으로 호출
        empty_mapping = {}
        self.load_tb_plan_budget_with_mapping(empty_mapping)
        self.load_tb_plan_schedule_with_mapping(empty_mapping)
        self.load_tb_plan_performance_with_mapping(empty_mapping)
        self.load_tb_plan_achievements_with_mapping(empty_mapping)

        total = sum(self.load_stats['records_by_table'].values())
        self.load_stats['total_records'] = total

        logger.info("\n" + "="*80)
        logger.info("✅ 데이터 적재 완료")
        logger.info("="*80)
        logger.info(f"총 레코드: {total}건")
        logger.info(f"테이블별 레코드:")
        for table, count in self.load_stats['records_by_table'].items():
            logger.info(f"  • {table}: {count}건")

        return self.load_stats

    def load_with_matching(self):
        """
        기존 TB_PLAN_DATA와 매칭하여 하위 테이블만 적재
        """
        logger.info("\n" + "="*80)
        logger.info("🚀 매칭 기반 DB 적재 시작")
        logger.info("="*80)

        # 0단계: BICS의 TB_PLAN_DATA를 BICS_DEV로 복사 (FK 제약조건용)
        self.copy_plan_data_to_dev()

        # 1단계: 기존 TB_PLAN_DATA 조회
        self.load_existing_plan_data()

        # 2단계: CSV와 매칭
        plan_id_mapping = self.process_matching()

        # 3단계: 리포트 저장
        self.save_reports()

        # 4단계: 하위 테이블만 적재 (기존 PLAN_ID 사용)
        if plan_id_mapping:
            # 기존 하위 테이블 데이터 삭제
            logger.info("\n🗑️ 기존 하위 테이블 데이터 삭제 중...")
            cursor = self.db_manager_write.connection.cursor()
            for table in ['TB_PLAN_ACHIEVEMENTS', 'TB_PLAN_PERFORMANCE', 'TB_PLAN_SCHEDULE', 'TB_PLAN_BUDGET']:
                try:
                    cursor.execute(f"DELETE FROM {table}")
                    deleted = cursor.rowcount
                    self.db_manager_write.connection.commit()
                    logger.info(f"  ✅ {table} 삭제: {deleted}건")
                except Exception as e:
                    logger.error(f"  ❌ {table} 삭제 실패: {e}")
            cursor.close()

            # 하위 테이블 적재
            self.load_child_tables_with_mapping(plan_id_mapping)
        else:
            logger.warning("⚠️ 매칭된 레코드가 없어 하위 테이블 적재를 건너뜁니다.")

        # 최종 통계
        logger.info("\n" + "="*80)
        logger.info("✅ 매칭 기반 적재 완료")
        logger.info("="*80)
        logger.info(f"📊 매칭 통계:")
        logger.info(f"  • 매칭 성공: {self.load_stats['matched']}건")
        logger.info(f"  • 매칭 실패: {self.load_stats['unmatched']}건")
        logger.info(f"  • 차이점 발견: {self.load_stats['diff_found']}건")
        logger.info(f"\n📊 적재 통계:")
        logger.info(f"  • 총 레코드: {self.load_stats['total_records']}건")
        for table, count in self.load_stats['records_by_table'].items():
            logger.info(f"  • {table}: {count}건")

        return self.load_stats

    def close(self):
        """DB 연결 종료"""
        if hasattr(self, 'db_manager_read'):
            self.db_manager_read.close()
        if hasattr(self, 'db_manager_write'):
            self.db_manager_write.close()

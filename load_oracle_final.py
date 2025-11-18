"""
Oracle 데이터베이스 적재 모듈 - 최종 운영 버전
회사 실제 DDL 기준으로 작성
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import os

from oracle_db_manager_improved import OracleDBManager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Oracle 한글 처리
os.environ['NLS_LANG'] = 'KOREAN_KOREA.AL32UTF8'


class OracleDBLoader:
    """Oracle 데이터베이스 적재 클래스 - 운영 버전"""

    def __init__(self, db_config: Dict[str, Any]):
        """
        Args:
            db_config: Oracle 연결 설정
        """
        self.db_manager = OracleDBManager(db_config)
        self.plan_id_mapping = {}  # sub_project_id → PLAN_ID 매핑
        
        # 데이터 디렉토리 설정
        self.normalized_dir = Path("normalized")  # normalized 폴더
        self.normalized_output_gov_dir = Path("normalized_output_government")  # 정부 출력
        
        # 적재 통계
        self.load_stats = {
            'total_records': 0,
            'records_by_table': {},
            'skipped_records': {},
            'errors': []
        }

    def connect(self):
        """Oracle 연결"""
        return self.db_manager.connect()

    def truncate_tables(self):
        """기존 테이블 데이터만 삭제 (구조는 유지)"""
        logger.info("🗑️ 기존 테이블 데이터 삭제 중 (TRUNCATE)...")
        
        # 역순으로 TRUNCATE (외래키 제약 때문)
        tables_to_truncate = [
            'TB_PLAN_DETAILS',
            'TB_PLAN_ACHIEVEMENTS', 
            'TB_PLAN_BUDGETS',
            'TB_PLAN_PERFORMANCES',
            'TB_PLAN_SCHEDULES',
            'TB_PLAN_DATA'
        ]
        
        for table_name in tables_to_truncate:
            try:
                # 테이블 존재 확인
                if self.db_manager.table_exists(table_name):
                    self.db_manager.truncate_table(table_name)
                    logger.info(f"  ✅ {table_name} 데이터 삭제 완료")
                else:
                    logger.warning(f"  ⚠️ {table_name} 테이블이 없습니다")
            except Exception as e:
                logger.error(f"  ❌ {table_name} TRUNCATE 실패: {e}")
                self.load_stats['errors'].append(f"{table_name} TRUNCATE: {str(e)}")

    def load_tb_plan_data(self) -> Dict[int, str]:
        """
        TB_PLAN_DATA 적재 - 회사 DDL 기준
        금액 필드: TOTAL_RESPRC, CUR_RESPRC는 VARCHAR2
        나머지 금액 필드는 NUMBER
        """
        logger.info("📥 TB_PLAN_DATA 적재 중...")
        
        # CSV 파일 로드 - normalized 폴더에서
        sub_projects_file = self.normalized_dir / "sub_projects.csv"
        if not sub_projects_file.exists():
            raise FileNotFoundError(f"❌ {sub_projects_file} 파일이 없습니다.")
        
        sub_projects = pd.read_csv(sub_projects_file, encoding='utf-8-sig')
        
        # 추가 CSV 파일 로드
        overviews_file = self.normalized_dir / "normalized_overviews.csv"
        budgets_file = self.normalized_dir / "normalized_budgets.csv"
        schedules_file = self.normalized_dir / "normalized_schedules.csv"
        performances_file = self.normalized_dir / "normalized_performances.csv"
        
        overviews = pd.read_csv(overviews_file, encoding='utf-8-sig') if overviews_file.exists() else None
        budgets = pd.read_csv(budgets_file, encoding='utf-8-sig') if budgets_file.exists() else None
        schedules = pd.read_csv(schedules_file, encoding='utf-8-sig') if schedules_file.exists() else None
        performances = pd.read_csv(performances_file, encoding='utf-8-sig') if performances_file.exists() else None
        
        # INSERT 쿼리 - 회사 DDL 기준
        insert_query = """
            INSERT INTO TB_PLAN_DATA (
                PLAN_ID, YEAR, NUM, NATION_ORGAN_NM, DETAIL_BIZ_NM, BIZ_NM,
                BIZ_TYPE, AREA, REP_FLD, 
                BIOLOGY_WEI, RED_WEI, GREEN_WEI, WHITE_WEI, FUSION_WEI,
                LEAD_ORGAN_NM, MNG_ORGAN_NM,
                BIZ_SDT, BIZ_EDT, RESPERIOD, CUR_RESPERIOD,
                TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV,
                CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV,
                LAST_GOAL, BIZ_CONTENTS, BIZ_CONTENTS_KEYWORD,
                REGIST_DT, DELETE_YN, REGIST_ID,
                REGUL_WEI, WEI, PERFORM_PRC, PLAN_PRC
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
                :11, :12, :13, :14, :15, :16,
                TO_DATE(:17, 'YYYY-MM-DD'), TO_DATE(:18, 'YYYY-MM-DD'), :19, :20,
                :21, :22, :23, :24, :25, :26, :27, :28, :29,
                SYSDATE, 'N', :30, :31, :32, :33, :34
            )
        """
        
        inserted_count = 0
        skipped_count = 0
        
        for idx, row in sub_projects.iterrows():
            try:
                sub_project_id = row['id']
                year = row['document_year']
                num = idx + 1
                plan_id = f"{year}{num:03d}"
                
                # 매핑 저장
                self.plan_id_mapping[sub_project_id] = plan_id
                
                # Overview 데이터
                overview_data = self._get_overview_data(overviews, sub_project_id)
                
                # 예산 데이터
                budget_data = self._get_budget_data(budgets, sub_project_id, year)
                
                # 사업 기간
                date_range = self._get_date_range(schedules, sub_project_id)
                
                # 연구기간 문자열 (날짜에서 생성)
                resperiod = None
                cur_resperiod = None
                if date_range['start_date'] and date_range['end_date']:
                    resperiod = f"{date_range['start_date']} ~ {date_range['end_date']}"
                    # 현재 연도 기간 (임시)
                    cur_resperiod = f"{year}"
                
                # 데이터 준비
                data = (
                    plan_id,                                           # 1. PLAN_ID
                    int(year) if pd.notna(year) else None,           # 2. YEAR
                    num,                                              # 3. NUM
                    str(row['department_name'])[:768] if pd.notna(row.get('department_name')) else None,  # 4. NATION_ORGAN_NM
                    str(row['sub_project_name'])[:768] if pd.notna(row.get('sub_project_name')) else None, # 5. DETAIL_BIZ_NM
                    str(row['main_project_name'])[:768] if pd.notna(row.get('main_project_name')) else None, # 6. BIZ_NM
                    overview_data['biz_type'],                       # 7. BIZ_TYPE
                    None,                                             # 8. AREA (추후 입력)
                    overview_data['rep_fld'],                        # 9. REP_FLD
                    None,                                             # 10. BIOLOGY_WEI (추후 입력)
                    None,                                             # 11. RED_WEI (추후 입력)
                    None,                                             # 12. GREEN_WEI (추후 입력)
                    None,                                             # 13. WHITE_WEI (추후 입력)
                    None,                                             # 14. FUSION_WEI (추후 입력)
                    overview_data['lead_organ'],                     # 15. LEAD_ORGAN_NM
                    overview_data['mng_organ'],                      # 16. MNG_ORGAN_NM
                    date_range['start_date'],                        # 17. BIZ_SDT
                    date_range['end_date'],                          # 18. BIZ_EDT
                    resperiod,                                        # 19. RESPERIOD
                    cur_resperiod,                                   # 20. CUR_RESPERIOD
                    str(budget_data['total_resprc']) if budget_data['total_resprc'] else None,  # 21. TOTAL_RESPRC (VARCHAR2)
                    budget_data['total_resprc_gov'],                 # 22. TOTAL_RESPRC_GOV (NUMBER)
                    budget_data['total_resprc_civ'],                 # 23. TOTAL_RESPRC_CIV (NUMBER)
                    str(budget_data['cur_resprc']) if budget_data['cur_resprc'] else None,      # 24. CUR_RESPRC (VARCHAR2)
                    budget_data['cur_resprc_gov'],                   # 25. CUR_RESPRC_GOV (NUMBER)
                    budget_data['cur_resprc_civ'],                   # 26. CUR_RESPRC_CIV (NUMBER)
                    overview_data['last_goal'],                      # 27. LAST_GOAL
                    overview_data['biz_contents'],                   # 28. BIZ_CONTENTS
                    None,                                             # 29. BIZ_CONTENTS_KEYWORD (추후 입력)
                    'SYSTEM',                                         # 30. REGIST_ID
                    None,                                             # 31. REGUL_WEI (추후 입력)
                    None,                                             # 32. WEI (추후 입력)
                    budget_data['perform_prc'],                      # 33. PERFORM_PRC (NUMBER)
                    budget_data['plan_prc']                          # 34. PLAN_PRC (NUMBER)
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 50 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_DATA 삽입 실패 (행 {idx}, sub_project_id={sub_project_id}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_DATA 행 {idx}: {str(e)}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_DATA: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_DATA'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_DATA'] = skipped_count
        self.load_stats['total_records'] += inserted_count
        
        return self.plan_id_mapping

    def _get_overview_data(self, overviews: pd.DataFrame, sub_project_id: int) -> Dict:
        """Overview 데이터 추출"""
        data = {
            'biz_type': None,
            'rep_fld': None,
            'lead_organ': None,
            'mng_organ': None,
            'last_goal': None,
            'biz_contents': None
        }
        
        if overviews is not None:
            overview_rows = overviews[overviews['sub_project_id'] == sub_project_id]
            if not overview_rows.empty:
                ov = overview_rows.iloc[0]
                data['biz_type'] = str(ov['project_type'])[:768] if pd.notna(ov.get('project_type')) else None
                data['rep_fld'] = str(ov['field'])[:768] if pd.notna(ov.get('field')) else None
                data['lead_organ'] = str(ov['managing_dept'])[:768] if pd.notna(ov.get('managing_dept')) else None
                data['mng_organ'] = str(ov['managing_org'])[:768] if pd.notna(ov.get('managing_org')) else None
                data['last_goal'] = str(ov['objective'])[:4000] if pd.notna(ov.get('objective')) else None
                data['biz_contents'] = str(ov['content'])[:4000] if pd.notna(ov.get('content')) else None
        
        return data

    def _get_budget_data(self, budgets: pd.DataFrame, sub_project_id: int, year: int) -> Dict:
        """
        예산 데이터 추출
        회사 DDL: TOTAL_RESPRC, CUR_RESPRC는 VARCHAR2, 나머지는 NUMBER
        """
        data = {
            'total_resprc': None,      # VARCHAR2로 저장
            'total_resprc_gov': None,  # NUMBER
            'total_resprc_civ': None,  # NUMBER
            'cur_resprc': None,        # VARCHAR2로 저장
            'cur_resprc_gov': None,    # NUMBER
            'cur_resprc_civ': None,    # NUMBER
            'perform_prc': None,       # NUMBER
            'plan_prc': None          # NUMBER
        }
        
        if budgets is not None:
            project_budgets = budgets[budgets['sub_project_id'] == sub_project_id]
            
            if not project_budgets.empty:
                # 총 연구비 계산
                gov_total = project_budgets[
                    project_budgets['budget_type'] == '정부'
                ]['amount'].sum() if '정부' in project_budgets['budget_type'].values else 0
                
                # 민간 (인코딩 문제 처리)
                civil_mask = project_budgets['budget_type'].str.contains('민간|민감', na=False, regex=True)
                civil_total = project_budgets[civil_mask]['amount'].sum() if civil_mask.any() else 0
                
                local_total = project_budgets[
                    project_budgets['budget_type'] == '지방비'
                ]['amount'].sum() if '지방비' in project_budgets['budget_type'].values else 0
                
                other_total = project_budgets[
                    project_budgets['budget_type'] == '기타'
                ]['amount'].sum() if '기타' in project_budgets['budget_type'].values else 0
                
                # 총 연구비 (모든 예산 합계)
                total = gov_total + civil_total + local_total + other_total
                
                # NUMBER 타입 필드
                data['total_resprc_gov'] = float(gov_total) if gov_total > 0 else None
                data['total_resprc_civ'] = float(civil_total) if civil_total > 0 else None
                
                # VARCHAR2 타입 필드 (숫자를 문자열로)
                data['total_resprc'] = str(int(total)) if total > 0 else None
                
                # 현재 연도 연구비
                cur_budgets = project_budgets[project_budgets['budget_year'] == year]
                if not cur_budgets.empty:
                    cur_gov = cur_budgets[
                        cur_budgets['budget_type'] == '정부'
                    ]['amount'].sum() if '정부' in cur_budgets['budget_type'].values else 0
                    
                    cur_civil_mask = cur_budgets['budget_type'].str.contains('민간|민감', na=False, regex=True)
                    cur_civil = cur_budgets[cur_civil_mask]['amount'].sum() if cur_civil_mask.any() else 0
                    
                    cur_local = cur_budgets[
                        cur_budgets['budget_type'] == '지방비'
                    ]['amount'].sum() if '지방비' in cur_budgets['budget_type'].values else 0
                    
                    cur_other = cur_budgets[
                        cur_budgets['budget_type'] == '기타'
                    ]['amount'].sum() if '기타' in cur_budgets['budget_type'].values else 0
                    
                    cur_total = cur_gov + cur_civil + cur_local + cur_other
                    
                    # NUMBER 타입 필드
                    data['cur_resprc_gov'] = float(cur_gov) if cur_gov > 0 else None
                    data['cur_resprc_civ'] = float(cur_civil) if cur_civil > 0 else None
                    
                    # VARCHAR2 타입 필드
                    data['cur_resprc'] = str(int(cur_total)) if cur_total > 0 else None
                
                # 실적/계획 비용 (NUMBER 타입)
                if 'is_actual' in project_budgets.columns:
                    perform = project_budgets[project_budgets['is_actual'] == True]['amount'].sum()
                    plan = project_budgets[project_budgets['is_actual'] == False]['amount'].sum()
                else:
                    # category로 판단
                    perform = project_budgets[
                        project_budgets['budget_category'].str.contains('실적', na=False)
                    ]['amount'].sum() if 'budget_category' in project_budgets.columns else 0
                    
                    plan = project_budgets[
                        project_budgets['budget_category'].str.contains('계획', na=False)
                    ]['amount'].sum() if 'budget_category' in project_budgets.columns else 0
                
                data['perform_prc'] = float(perform) if perform > 0 else None
                data['plan_prc'] = float(plan) if plan > 0 else None
        
        return data

    def _get_date_range(self, schedules: pd.DataFrame, sub_project_id: int) -> Dict:
        """일정에서 날짜 범위 추출"""
        data = {
            'start_date': None,
            'end_date': None
        }
        
        if schedules is not None:
            project_schedules = schedules[schedules['sub_project_id'] == sub_project_id]
            if not project_schedules.empty:
                if 'start_date' in project_schedules.columns:
                    dates = project_schedules['start_date'].dropna()
                    if len(dates) > 0:
                        data['start_date'] = str(dates.min())[:10]
                
                if 'end_date' in project_schedules.columns:
                    dates = project_schedules['end_date'].dropna()
                    if len(dates) > 0:
                        data['end_date'] = str(dates.max())[:10]
        
        return data

    def load_tb_plan_schedules(self):
        """TB_PLAN_SCHEDULES 적재"""
        logger.info("📥 TB_PLAN_SCHEDULES 적재 중...")
        
        csv_file = self.normalized_dir / "normalized_schedules.csv"
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
        skipped_count = 0
        
        for idx, row in df.iterrows():
            try:
                sub_project_id = row.get('sub_project_id')
                plan_id = self.plan_id_mapping.get(sub_project_id)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    int(row['year']) if pd.notna(row.get('year')) else None,
                    int(row['quarter']) if pd.notna(row.get('quarter')) else None,
                    int(row['month_start']) if pd.notna(row.get('month_start')) else None,
                    int(row['month_end']) if pd.notna(row.get('month_end')) else None,
                    str(row['start_date'])[:10] if pd.notna(row.get('start_date')) else None,
                    str(row['end_date'])[:10] if pd.notna(row.get('end_date')) else None,
                    str(row['task_category'])[:200] if pd.notna(row.get('task_category')) else None,
                    str(row['task_description']) if pd.notna(row.get('task_description')) else None,
                    str(row['original_period'])[:100] if pd.notna(row.get('original_period')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_SCHEDULES 삽입 실패 (행 {idx}): {e}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_SCHEDULES: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_SCHEDULES'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_SCHEDULES'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_performances(self):
        """TB_PLAN_PERFORMANCES 적재"""
        logger.info("📥 TB_PLAN_PERFORMANCES 적재 중...")
        
        csv_file = self.normalized_dir / "normalized_performances.csv"
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
        skipped_count = 0
        
        for idx, row in df.iterrows():
            try:
                sub_project_id = row.get('sub_project_id')
                plan_id = self.plan_id_mapping.get(sub_project_id)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    int(row['performance_year']) if pd.notna(row.get('performance_year')) else None,
                    str(row['indicator_category'])[:100] if pd.notna(row.get('indicator_category')) else None,
                    str(row['indicator_type'])[:200] if pd.notna(row.get('indicator_type')) else None,
                    float(row['value']) if pd.notna(row.get('value')) else None,
                    str(row['unit'])[:50] if pd.notna(row.get('unit')) else None,
                    str(row['original_text']) if pd.notna(row.get('original_text')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_PERFORMANCES 삽입 실패 (행 {idx}): {e}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_PERFORMANCES: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_PERFORMANCES'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_PERFORMANCES'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_budgets(self):
        """TB_PLAN_BUDGETS 적재"""
        logger.info("📥 TB_PLAN_BUDGETS 적재 중...")
        
        csv_file = self.normalized_dir / "normalized_budgets.csv"
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
        skipped_count = 0
        
        for idx, row in df.iterrows():
            try:
                sub_project_id = row.get('sub_project_id')
                plan_id = self.plan_id_mapping.get(sub_project_id)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                # 예산 타입 인코딩 문제 처리
                budget_type = str(row['budget_type']) if pd.notna(row.get('budget_type')) else None
                if budget_type and ('민' in budget_type or '��' in budget_type):
                    budget_type = '민간'
                
                # is_actual 판단
                is_actual = 'N'
                if 'is_actual' in row:
                    is_actual = 'Y' if row['is_actual'] else 'N'
                elif 'budget_category' in row and pd.notna(row['budget_category']):
                    if '실적' in str(row['budget_category']):
                        is_actual = 'Y'
                
                data = (
                    plan_id,
                    int(row['budget_year']) if pd.notna(row.get('budget_year')) else None,
                    str(row['budget_category'])[:100] if pd.notna(row.get('budget_category')) else None,
                    budget_type[:100] if budget_type else None,
                    float(row['amount']) if pd.notna(row.get('amount')) else None,
                    str(row['currency'])[:10] if pd.notna(row.get('currency')) else 'KRW',
                    is_actual,
                    str(row['original_text']) if pd.notna(row.get('original_text')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_BUDGETS 삽입 실패 (행 {idx}): {e}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_BUDGETS: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_BUDGETS'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_BUDGETS'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_achievements(self):
        """TB_PLAN_ACHIEVEMENTS 적재"""
        logger.info("📥 TB_PLAN_ACHIEVEMENTS 적재 중...")
        
        csv_file = self.normalized_dir / "key_achievements.csv"
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
        skipped_count = 0
        
        for idx, row in df.iterrows():
            try:
                sub_project_id = row.get('sub_project_id')
                plan_id = self.plan_id_mapping.get(sub_project_id)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    int(row['achievement_year']) if pd.notna(row.get('achievement_year')) else None,
                    int(row['achievement_order']) if pd.notna(row.get('achievement_order')) else None,
                    str(row['description']) if pd.notna(row.get('description')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_ACHIEVEMENTS 삽입 실패 (행 {idx}): {e}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_ACHIEVEMENTS: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_ACHIEVEMENTS'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_ACHIEVEMENTS'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_details(self):
        """TB_PLAN_DETAILS 적재"""
        logger.info("📥 TB_PLAN_DETAILS 적재 중...")
        
        csv_file = self.normalized_dir / "plan_details.csv"
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
        skipped_count = 0
        
        for idx, row in df.iterrows():
            try:
                sub_project_id = row.get('sub_project_id')
                plan_id = self.plan_id_mapping.get(sub_project_id)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    int(row['plan_year']) if pd.notna(row.get('plan_year')) else None,
                    int(row['plan_order']) if pd.notna(row.get('plan_order')) else None,
                    str(row['description']) if pd.notna(row.get('description')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_DETAILS 삽입 실패 (행 {idx}): {e}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_DETAILS: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_DETAILS'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_DETAILS'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_all_tables(self):
        """모든 테이블 적재"""
        logger.info("📥 Oracle 데이터 적재 시작...")
        
        # 1. TB_PLAN_DATA 먼저 적재 (마스터)
        self.plan_id_mapping = self.load_tb_plan_data()
        logger.info(f"✅ PLAN_ID 매핑 생성 완료: {len(self.plan_id_mapping)}건")
        
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
        print("\n" + "="*80)
        print("📊 Oracle 데이터 적재 요약")
        print("="*80)
        print(f"✅ 총 적재 레코드: {self.load_stats['total_records']:,}건")
        print(f"✅ PLAN_ID 매핑: {len(self.plan_id_mapping)}건")
        
        print("\n📊 테이블별 적재 현황:")
        for table, count in self.load_stats['records_by_table'].items():
            skipped = self.load_stats['skipped_records'].get(table, 0)
            if skipped > 0:
                print(f"  • {table}: {count:,}건 (스킵: {skipped:,}건)")
            else:
                print(f"  • {table}: {count:,}건")
        
        if self.load_stats['errors']:
            print(f"\n⚠️ 오류 발생: {len(self.load_stats['errors'])}건")
            for i, error in enumerate(self.load_stats['errors'][:5], 1):
                print(f"  {i}. {error}")
            if len(self.load_stats['errors']) > 5:
                print(f"  ... 외 {len(self.load_stats['errors']) - 5}건")
        
        print("="*80)

    def close(self):
        """연결 종료"""
        self.db_manager.close()


def main():
    """메인 실행 함수"""
    from config import ORACLE_CONFIG
    
    # 적재 실행
    loader = OracleDBLoader(db_config=ORACLE_CONFIG)
    
    try:
        # 연결
        logger.info("🔌 Oracle 데이터베이스 연결 중...")
        loader.connect()
        
        # 기존 데이터 삭제 (필요시)
        # loader.truncate_tables()
        
        # 데이터 적재
        loader.load_all_tables()
        
        print("\n✅ Oracle 데이터베이스 적재 완료!")
        
    except Exception as e:
        logger.error(f"❌ 적재 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        loader.close()


if __name__ == "__main__":
    main()
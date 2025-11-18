"""
Oracle 데이터베이스 적재 모듈 - 개선 버전
MySQL/CSV → Oracle TB_PLAN_DATA 및 하위 테이블
주요 개선사항:
1. plan_id_mapping 문제 해결
2. 금액 포맷팅 제거 (NUMBER 타입 직접 사용)
3. 기존 테이블 보존 옵션 추가
4. 필수 컬럼 데이터 채우기
5. 인코딩 문제 해결
6. 상세 로깅 개선
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re
import json

from oracle_db_manager_improved import OracleDBManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDBLoader:
    """Oracle 데이터베이스 적재 클래스 - 개선 버전"""

    def __init__(self, db_config: Dict[str, Any], data_dir: str, 
                 use_existing_tables: bool = True):
        """
        Args:
            db_config: Oracle 연결 설정
            data_dir: 데이터 파일 디렉토리
            use_existing_tables: 기존 테이블 사용 여부 (True면 TRUNCATE, False면 재생성)
        """
        self.db_manager = OracleDBManager(db_config)
        self.data_dir = Path(data_dir)
        self.use_existing_tables = use_existing_tables
        self.plan_id_mapping = {}  # sub_project_id → PLAN_ID 매핑 (인스턴스 변수)
        
        # JSON 파일에서 추가 데이터 로드용
        self.json_data_cache = {}
        
        # 적재 통계
        self.load_stats = {
            'tables_created': 0,
            'tables_truncated': 0,
            'total_records': 0,
            'records_by_table': {},
            'skipped_records': {},
            'errors': []
        }

    def connect(self):
        """Oracle 연결"""
        return self.db_manager.connect()

    def prepare_tables(self):
        """테이블 준비 (기존 테이블 사용 또는 재생성)"""
        if self.use_existing_tables:
            self._truncate_tables()
        else:
            self._recreate_tables()

    def _truncate_tables(self):
        """기존 테이블 TRUNCATE (데이터만 삭제)"""
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
                if self.db_manager.table_exists(table_name):
                    self.db_manager.truncate_table(table_name)
                    self.load_stats['tables_truncated'] += 1
                    logger.info(f"  ✅ {table_name} 데이터 삭제 완료")
                else:
                    logger.warning(f"  ⚠️ {table_name} 테이블이 없습니다")
            except Exception as e:
                logger.error(f"  ❌ {table_name} TRUNCATE 실패: {e}")
                self.load_stats['errors'].append(f"{table_name} TRUNCATE: {str(e)}")
                
        logger.info("✅ 테이블 데이터 삭제 완료")

    def _recreate_tables(self):
        """테이블 재생성 (DROP & CREATE)"""
        logger.warning("⚠️ 테이블 재생성은 권장하지 않습니다. 회사 스키마를 사용하세요.")
        # 기존 drop_existing_tables() + create_tables() 로직
        pass

    def load_json_data(self, json_file: str) -> Dict:
        """JSON 파일 로드 및 캐싱"""
        if json_file not in self.json_data_cache:
            json_path = self.data_dir / json_file
            if json_path.exists():
                with open(json_path, 'r', encoding='utf-8') as f:
                    self.json_data_cache[json_file] = json.load(f)
            else:
                self.json_data_cache[json_file] = {}
        return self.json_data_cache[json_file]

    def get_additional_project_data(self, sub_project_id: int) -> Dict:
        """
        JSON 파일에서 추가 프로젝트 데이터 추출
        AREA, REGUL_WEI, WEI, BIZ_CONTENTS_KEYWORD 등
        """
        additional_data = {
            'area': None,           # 3대영역
            'regul_wei': None,      # 규제 비중
            'wei': None,            # 비중
            'biology_wei': None,    # 생명과학 비중
            'red_wei': None,        # 레드 비중
            'green_wei': None,      # 그린 비중  
            'white_wei': None,      # 화이트 비중
            'fusion_wei': None,     # 융합 비중
            'biz_contents_keyword': None,  # 사업 내용 키워드
            'resperiod': None,      # 연구기간
            'cur_resperiod': None   # 현 연구기간
        }
        
        # 프로젝트 세부 정보 JSON 파일이 있다면 로드
        project_details = self.load_json_data('project_details.json')
        if str(sub_project_id) in project_details:
            details = project_details[str(sub_project_id)]
            
            # 3대영역 추출
            if 'area' in details:
                additional_data['area'] = str(details['area'])[:768]
            
            # 가중치 정보 추출
            if 'weights' in details:
                weights = details['weights']
                additional_data['biology_wei'] = self._safe_float(weights.get('biology'))
                additional_data['red_wei'] = self._safe_float(weights.get('red'))
                additional_data['green_wei'] = self._safe_float(weights.get('green'))
                additional_data['white_wei'] = self._safe_float(weights.get('white'))
                additional_data['fusion_wei'] = self._safe_float(weights.get('fusion'))
                additional_data['regul_wei'] = self._safe_float(weights.get('regulation'))
                
                # 전체 가중치 문자열
                if 'total' in weights:
                    additional_data['wei'] = str(weights['total'])[:768]
            
            # 키워드 추출
            if 'keywords' in details:
                keywords = details['keywords']
                if isinstance(keywords, list):
                    additional_data['biz_contents_keyword'] = ', '.join(keywords)[:4000]
                else:
                    additional_data['biz_contents_keyword'] = str(keywords)[:4000]
            
            # 연구기간 추출
            if 'research_period' in details:
                additional_data['resperiod'] = str(details['research_period'])[:768]
            if 'current_research_period' in details:
                additional_data['cur_resperiod'] = str(details['current_research_period'])[:768]
        
        return additional_data

    def _safe_float(self, value) -> Optional[float]:
        """안전한 float 변환"""
        if value is None or pd.isna(value):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value) -> Optional[int]:
        """안전한 int 변환"""
        if value is None or pd.isna(value):
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def load_tb_plan_data(self) -> Dict[int, str]:
        """
        TB_PLAN_DATA 적재 (개선 버전)
        Returns: sub_project_id → PLAN_ID 매핑 딕셔너리
        """
        logger.info("📥 TB_PLAN_DATA 적재 중 (개선 버전)...")
        
        # CSV 파일 로드
        csv_file = self.data_dir / "sub_projects.csv"
        if not csv_file.exists():
            raise FileNotFoundError(f"❌ {csv_file} 파일이 없습니다.")
        
        sub_projects = pd.read_csv(csv_file, encoding='utf-8-sig')
        
        # 추가 CSV 파일 로드
        overviews_file = self.data_dir / "normalized_overviews.csv"
        budgets_file = self.data_dir / "normalized_budgets.csv"
        schedules_file = self.data_dir / "normalized_schedules.csv"
        
        overviews = pd.read_csv(overviews_file, encoding='utf-8-sig') if overviews_file.exists() else None
        budgets = pd.read_csv(budgets_file, encoding='utf-8-sig') if budgets_file.exists() else None
        schedules = pd.read_csv(schedules_file, encoding='utf-8-sig') if schedules_file.exists() else None
        
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
                
                # 인스턴스 변수에 매핑 저장 (중요!)
                self.plan_id_mapping[sub_project_id] = plan_id
                
                # 추가 데이터 가져오기
                additional = self.get_additional_project_data(sub_project_id)
                
                # Overview 데이터
                overview_data = self._get_overview_data(overviews, sub_project_id)
                
                # 예산 데이터 (금액은 NUMBER 타입으로 직접 저장)
                budget_data = self._get_budget_data(budgets, sub_project_id, year)
                
                # 사업 기간
                date_range = self._get_date_range(schedules, sub_project_id)
                
                # 연구기간 문자열 생성
                resperiod = additional['resperiod']
                cur_resperiod = additional['cur_resperiod']
                
                # 연구기간이 없으면 날짜에서 생성
                if not resperiod and date_range['start_date'] and date_range['end_date']:
                    resperiod = f"{date_range['start_date']} ~ {date_range['end_date']}"
                
                # 데이터 준비
                data = (
                    plan_id,                                           # 1. PLAN_ID
                    self._safe_int(year),                            # 2. YEAR
                    num,                                              # 3. NUM
                    str(row['department_name'])[:768] if pd.notna(row['department_name']) else None,  # 4. NATION_ORGAN_NM
                    str(row['sub_project_name'])[:768] if pd.notna(row['sub_project_name']) else None, # 5. DETAIL_BIZ_NM
                    str(row['main_project_name'])[:768] if pd.notna(row['main_project_name']) else None, # 6. BIZ_NM
                    overview_data['biz_type'],                       # 7. BIZ_TYPE
                    additional['area'],                              # 8. AREA ✅
                    overview_data['rep_fld'],                        # 9. REP_FLD
                    additional['biology_wei'],                       # 10. BIOLOGY_WEI ✅
                    additional['red_wei'],                           # 11. RED_WEI ✅
                    additional['green_wei'],                         # 12. GREEN_WEI ✅
                    additional['white_wei'],                         # 13. WHITE_WEI ✅
                    additional['fusion_wei'],                        # 14. FUSION_WEI ✅
                    overview_data['lead_organ'],                     # 15. LEAD_ORGAN_NM
                    overview_data['mng_organ'],                      # 16. MNG_ORGAN_NM
                    date_range['start_date'],                        # 17. BIZ_SDT
                    date_range['end_date'],                          # 18. BIZ_EDT
                    resperiod,                                        # 19. RESPERIOD ✅
                    cur_resperiod,                                   # 20. CUR_RESPERIOD ✅
                    budget_data['total_resprc'],                     # 21. TOTAL_RESPRC (NUMBER) ✅
                    budget_data['total_resprc_gov'],                 # 22. TOTAL_RESPRC_GOV (NUMBER) ✅
                    budget_data['total_resprc_civ'],                 # 23. TOTAL_RESPRC_CIV (NUMBER) ✅
                    budget_data['cur_resprc'],                       # 24. CUR_RESPRC (NUMBER) ✅
                    budget_data['cur_resprc_gov'],                   # 25. CUR_RESPRC_GOV (NUMBER) ✅
                    budget_data['cur_resprc_civ'],                   # 26. CUR_RESPRC_CIV (NUMBER) ✅
                    overview_data['last_goal'],                      # 27. LAST_GOAL
                    overview_data['biz_contents'],                   # 28. BIZ_CONTENTS
                    additional['biz_contents_keyword'],              # 29. BIZ_CONTENTS_KEYWORD ✅
                    'SYSTEM',                                         # 30. REGIST_ID
                    additional['regul_wei'],                         # 31. REGUL_WEI ✅
                    additional['wei'],                                # 32. WEI ✅
                    budget_data['perform_prc'],                      # 33. PERFORM_PRC (NUMBER) ✅
                    budget_data['plan_prc']                          # 34. PLAN_PRC (NUMBER) ✅
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
        
        # 인스턴스 변수인 self.plan_id_mapping을 반환
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
                data['biz_type'] = str(ov['project_type'])[:768] if pd.notna(ov['project_type']) else None
                data['rep_fld'] = str(ov['field'])[:768] if pd.notna(ov['field']) else None
                data['lead_organ'] = str(ov['managing_dept'])[:768] if pd.notna(ov['managing_dept']) else None
                data['mng_organ'] = str(ov['managing_org'])[:768] if pd.notna(ov['managing_org']) else None
                data['last_goal'] = str(ov['objective'])[:4000] if pd.notna(ov['objective']) else None
                data['biz_contents'] = str(ov['content'])[:4000] if pd.notna(ov['content']) else None
        
        return data

    def _get_budget_data(self, budgets: pd.DataFrame, sub_project_id: int, year: int) -> Dict:
        """
        예산 데이터 추출 (개선: 금액을 NUMBER로 직접 반환)
        """
        data = {
            'total_resprc': None,
            'total_resprc_gov': None,
            'total_resprc_civ': None,
            'cur_resprc': None,
            'cur_resprc_gov': None,
            'cur_resprc_civ': None,
            'perform_prc': None,
            'plan_prc': None
        }
        
        if budgets is not None:
            project_budgets = budgets[budgets['sub_project_id'] == sub_project_id]
            
            if not project_budgets.empty:
                # 총 연구비 계산
                gov_total = self._safe_float(
                    project_budgets[project_budgets['budget_type'] == '정부']['amount'].sum()
                )
                
                # '민간' 인코딩 문제 처리
                civil_mask = project_budgets['budget_type'].str.contains('민간|민감', na=False, regex=True)
                civil_total = self._safe_float(
                    project_budgets[civil_mask]['amount'].sum()
                )
                
                local_total = self._safe_float(
                    project_budgets[project_budgets['budget_type'] == '지방비']['amount'].sum()
                )
                
                other_total = self._safe_float(
                    project_budgets[project_budgets['budget_type'] == '기타']['amount'].sum()
                )
                
                # 총 연구비 (정부 + 민간 + 지방비 + 기타)
                total = 0
                if gov_total: total += gov_total
                if civil_total: total += civil_total
                if local_total: total += local_total
                if other_total: total += other_total
                
                data['total_resprc_gov'] = gov_total if gov_total and gov_total > 0 else None
                data['total_resprc_civ'] = civil_total if civil_total and civil_total > 0 else None
                data['total_resprc'] = total if total > 0 else None
                
                # 현재 연도 연구비
                cur_budgets = project_budgets[project_budgets['budget_year'] == year]
                if not cur_budgets.empty:
                    cur_gov = self._safe_float(
                        cur_budgets[cur_budgets['budget_type'] == '정부']['amount'].sum()
                    )
                    
                    cur_civil_mask = cur_budgets['budget_type'].str.contains('민간|민감', na=False, regex=True)
                    cur_civil = self._safe_float(
                        cur_budgets[cur_civil_mask]['amount'].sum()
                    )
                    
                    cur_local = self._safe_float(
                        cur_budgets[cur_budgets['budget_type'] == '지방비']['amount'].sum()
                    )
                    
                    cur_other = self._safe_float(
                        cur_budgets[cur_budgets['budget_type'] == '기타']['amount'].sum()
                    )
                    
                    cur_total = 0
                    if cur_gov: cur_total += cur_gov
                    if cur_civil: cur_total += cur_civil
                    if cur_local: cur_total += cur_local
                    if cur_other: cur_total += cur_other
                    
                    data['cur_resprc_gov'] = cur_gov if cur_gov and cur_gov > 0 else None
                    data['cur_resprc_civ'] = cur_civil if cur_civil and cur_civil > 0 else None
                    data['cur_resprc'] = cur_total if cur_total > 0 else None
                
                # 실적/계획 비용 (is_actual 컬럼 사용)
                if 'is_actual' in project_budgets.columns:
                    perform = self._safe_float(
                        project_budgets[project_budgets['is_actual'] == True]['amount'].sum()
                    )
                    plan = self._safe_float(
                        project_budgets[project_budgets['is_actual'] == False]['amount'].sum()
                    )
                else:
                    # is_actual 컬럼이 없으면 category로 판단
                    perform = self._safe_float(
                        project_budgets[
                            project_budgets['budget_category'].str.contains('실적', na=False)
                        ]['amount'].sum()
                    )
                    plan = self._safe_float(
                        project_budgets[
                            project_budgets['budget_category'].str.contains('계획', na=False)
                        ]['amount'].sum()
                    )
                
                data['perform_prc'] = perform if perform and perform > 0 else None
                data['plan_prc'] = plan if plan and plan > 0 else None
        
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
        """TB_PLAN_SCHEDULES 적재 (개선)"""
        logger.info("📥 TB_PLAN_SCHEDULES 적재 중...")
        
        csv_file = self.data_dir / "normalized_schedules.csv"
        if not csv_file.exists():
            logger.warning(f"⚠️ {csv_file} 파일이 없습니다. 스킵합니다.")
            return
        
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        
        # plan_id 컬럼이 있는지 확인
        if 'plan_id' in df.columns:
            logger.info("  ℹ️ CSV에 plan_id 컬럼이 있습니다.")
        
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
                # plan_id 결정 로직 개선
                plan_id = self._resolve_plan_id(row)
                
                if not plan_id:
                    logger.debug(f"⚠️ PLAN_ID 매핑 없음: 행 {idx}")
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    self._safe_int(row['year']) if pd.notna(row.get('year')) else None,
                    self._safe_int(row['quarter']) if pd.notna(row.get('quarter')) else None,
                    self._safe_int(row['month_start']) if pd.notna(row.get('month_start')) else None,
                    self._safe_int(row['month_end']) if pd.notna(row.get('month_end')) else None,
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
                    logger.info(f"  {inserted_count}건 적재 중...")
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_SCHEDULES 삽입 실패 (행 {idx}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_SCHEDULES 행 {idx}: {str(e)}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_SCHEDULES: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_SCHEDULES'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_SCHEDULES'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def _resolve_plan_id(self, row: pd.Series) -> Optional[str]:
        """
        PLAN_ID 결정 로직
        1. CSV에 plan_id 컬럼이 있으면 사용
        2. 없으면 sub_project_id로 매핑 조회
        3. 매핑도 없으면 DB에서 조회 시도
        """
        # 1. CSV에 plan_id가 있으면 사용
        if 'plan_id' in row and pd.notna(row['plan_id']):
            return str(row['plan_id'])
        
        # 2. sub_project_id로 매핑 조회
        if 'sub_project_id' in row:
            sub_project_id = row['sub_project_id']
            if sub_project_id in self.plan_id_mapping:
                return self.plan_id_mapping[sub_project_id]
            
            # 3. DB에서 조회 시도 (fallback)
            if hasattr(self.db_manager, 'lookup_plan_id'):
                plan_id = self.db_manager.lookup_plan_id(sub_project_id)
                if plan_id:
                    # 캐시에 저장
                    self.plan_id_mapping[sub_project_id] = plan_id
                    return plan_id
        
        return None

    def load_tb_plan_performances(self):
        """TB_PLAN_PERFORMANCES 적재 (개선)"""
        logger.info("📥 TB_PLAN_PERFORMANCES 적재 중...")
        
        csv_file = self.data_dir / "normalized_performances.csv"
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
                plan_id = self._resolve_plan_id(row)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    self._safe_int(row['performance_year']) if pd.notna(row.get('performance_year')) else None,
                    str(row['indicator_category'])[:100] if pd.notna(row.get('indicator_category')) else None,
                    str(row['indicator_type'])[:200] if pd.notna(row.get('indicator_type')) else None,
                    self._safe_float(row['value']) if pd.notna(row.get('value')) else None,
                    str(row['unit'])[:50] if pd.notna(row.get('unit')) else None,
                    str(row['original_text']) if pd.notna(row.get('original_text')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_PERFORMANCES 삽입 실패 (행 {idx}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_PERFORMANCES 행 {idx}: {str(e)}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_PERFORMANCES: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_PERFORMANCES'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_PERFORMANCES'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_budgets(self):
        """TB_PLAN_BUDGETS 적재 (개선)"""
        logger.info("📥 TB_PLAN_BUDGETS 적재 중...")
        
        csv_file = self.data_dir / "normalized_budgets.csv"
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
                plan_id = self._resolve_plan_id(row)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                # 예산 타입 인코딩 문제 처리
                budget_type = str(row['budget_type']) if pd.notna(row.get('budget_type')) else None
                if budget_type and '민' in budget_type and '간' in budget_type:
                    budget_type = '민간'  # 인코딩 문제 수정
                
                # is_actual 판단 개선
                is_actual = 'N'
                if 'is_actual' in row:
                    is_actual = 'Y' if row['is_actual'] else 'N'
                elif 'budget_category' in row and pd.notna(row['budget_category']):
                    if '실적' in str(row['budget_category']):
                        is_actual = 'Y'
                
                data = (
                    plan_id,
                    self._safe_int(row['budget_year']) if pd.notna(row.get('budget_year')) else None,
                    str(row['budget_category'])[:100] if pd.notna(row.get('budget_category')) else None,
                    budget_type[:100] if budget_type else None,
                    self._safe_float(row['amount']) if pd.notna(row.get('amount')) else None,
                    str(row['currency'])[:10] if pd.notna(row.get('currency')) else 'KRW',
                    is_actual,
                    str(row['original_text']) if pd.notna(row.get('original_text')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_BUDGETS 삽입 실패 (행 {idx}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_BUDGETS 행 {idx}: {str(e)}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_BUDGETS: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_BUDGETS'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_BUDGETS'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_achievements(self):
        """TB_PLAN_ACHIEVEMENTS 적재 (개선)"""
        logger.info("📥 TB_PLAN_ACHIEVEMENTS 적재 중...")
        
        csv_file = self.data_dir / "key_achievements.csv"
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
                plan_id = self._resolve_plan_id(row)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    self._safe_int(row['achievement_year']) if pd.notna(row.get('achievement_year')) else None,
                    self._safe_int(row['achievement_order']) if pd.notna(row.get('achievement_order')) else None,
                    str(row['description']) if pd.notna(row.get('description')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_ACHIEVEMENTS 삽입 실패 (행 {idx}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_ACHIEVEMENTS 행 {idx}: {str(e)}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_ACHIEVEMENTS: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_ACHIEVEMENTS'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_ACHIEVEMENTS'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_tb_plan_details(self):
        """TB_PLAN_DETAILS 적재 (개선)"""
        logger.info("📥 TB_PLAN_DETAILS 적재 중...")
        
        csv_file = self.data_dir / "plan_details.csv"
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
                plan_id = self._resolve_plan_id(row)
                
                if not plan_id:
                    skipped_count += 1
                    continue
                
                data = (
                    plan_id,
                    self._safe_int(row['plan_year']) if pd.notna(row.get('plan_year')) else None,
                    self._safe_int(row['plan_order']) if pd.notna(row.get('plan_order')) else None,
                    str(row['description']) if pd.notna(row.get('description')) else None
                )
                
                self.db_manager.cursor.execute(insert_query, data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    self.db_manager.commit()
                    logger.info(f"  {inserted_count}건 적재 중...")
                    
            except Exception as e:
                logger.error(f"❌ TB_PLAN_DETAILS 삽입 실패 (행 {idx}): {e}")
                self.load_stats['errors'].append(f"TB_PLAN_DETAILS 행 {idx}: {str(e)}")
                skipped_count += 1
        
        self.db_manager.commit()
        logger.info(f"✅ TB_PLAN_DETAILS: {inserted_count}건 적재 완료 (스킵: {skipped_count}건)")
        
        self.load_stats['records_by_table']['TB_PLAN_DETAILS'] = inserted_count
        self.load_stats['skipped_records']['TB_PLAN_DETAILS'] = skipped_count
        self.load_stats['total_records'] += inserted_count

    def load_all_tables(self):
        """모든 테이블 적재 (개선)"""
        logger.info("📥 데이터 적재 시작 (개선 버전)...")
        
        # 1. TB_PLAN_DATA 먼저 적재 (마스터)
        # 중요: 반환된 매핑을 인스턴스 변수에 저장 (이미 내부에서 처리됨)
        plan_id_mapping = self.load_tb_plan_data()
        
        # 매핑 검증
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
        """적재 요약 출력 (개선)"""
        print("\n" + "="*80)
        print("📊 Oracle 데이터 적재 요약 (개선 버전)")
        print("="*80)
        
        if self.use_existing_tables:
            print(f"✅ TRUNCATE된 테이블: {self.load_stats['tables_truncated']}개")
        else:
            print(f"✅ 생성된 테이블: {self.load_stats['tables_created']}개")
        
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
            for i, error in enumerate(self.load_stats['errors'][:10], 1):
                print(f"  {i}. {error}")
            if len(self.load_stats['errors']) > 10:
                print(f"  ... 외 {len(self.load_stats['errors']) - 10}건")
        
        print("="*80)

    def close(self):
        """연결 종료"""
        self.db_manager.close()


def main():
    """메인 실행"""
    from config import ORACLE_CONFIG
    
    # 데이터 디렉토리 (실제 경로로 변경 필요)
    data_dir = "data"  # CSV 및 JSON 파일이 있는 디렉토리
    
    # 적재 실행
    loader = OracleDBLoader(
        db_config=ORACLE_CONFIG,
        data_dir=data_dir,
        use_existing_tables=True  # 기존 테이블 사용 (TRUNCATE만)
    )
    
    try:
        # 연결
        logger.info("🔌 Oracle 데이터베이스 연결 중...")
        loader.connect()
        
        # 테이블 준비
        loader.prepare_tables()
        
        # 데이터 적재
        loader.load_all_tables()
        
        print("\n✅ Oracle 데이터베이스 적재 완료 (개선 버전)!")
        
    except Exception as e:
        logger.error(f"❌ 적재 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
        
    finally:
        loader.close()


if __name__ == "__main__":
    main()
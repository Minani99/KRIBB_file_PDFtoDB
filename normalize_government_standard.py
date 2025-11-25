import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import logging
from fuzzywuzzy import fuzz

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GovernmentStandardNormalizer:
    """정부 표준 형식으로 정규화"""

    SPECIAL_CHAR_PATTERN = re.compile(r'[(){}\[\]<>「」『』"\'`]|\s{2,}')

    @staticmethod
    def _clean_text(value: str, max_length: int = None):
        """텍스트 정리 - 특수문자 제거 (DB 적재용)"""
        if not value:
            return ""  # None 대신 빈 문자열 반환
        # 특수문자 제거: (), {}, [], <>, 「」, 『』, "', `, ‧ (가운뎃점), · (중점), ∙ (bullet operator) 등
        cleaned = re.sub(r'[(){}\[\]<>「」『』"\'`‧·∙・]', '', value)
        # 연속된 공백을 하나로
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        if max_length:
            return cleaned[:max_length]
        return cleaned

    @staticmethod
    def _normalize_for_matching(value: str):
        """
        매칭용 텍스트 정규화 - 개선 버전 (정보 손실 최소화)
        - 특수문자 통일
        - 괄호 내용은 제거하지 않고 공백으로 변환 (정보 보존)
        - 공백을 완전히 제거 (매칭 정확도 향상)
        - 접미사는 제거하지 않음 (정보 보존)
        """
        if not value:
            return ""

        # 1. 특수문자를 공백으로 통일 (∙, ·, ・, /, -, 등)
        text = value.replace('∙', ' ').replace('·', ' ').replace('・', ' ')
        text = text.replace('/', ' ').replace('-', ' ')

        # 2. 괄호는 제거하지 않고 공백으로 변환 (내용 보존)
        # 예: "바이오의료(R&D)" → "바이오의료 R&D"
        text = text.replace('(', ' ').replace(')', ' ')
        text = text.replace('[', ' ').replace(']', ' ')
        text = text.replace('{', ' ').replace('}', ' ')
        text = text.replace('「', ' ').replace('」', ' ')
        text = text.replace('『', ' ').replace('』', ' ')

        # 3. 따옴표와 기타 특수문자 제거
        text = text.replace('"', '').replace("'", '').replace('`', '')
        text = text.replace('‧', '').replace('·', '').replace('∙', '')

        # 4.  공백을 완전히 제거 (띄어쓰기 차이로 인한 매칭 실패 방지)
        # "바이오 의료" vs "바이오의료" → 둘 다 "바이오의료"로 통일
        text = text.replace(' ', '')

        # 5. 접미사는 제거하지 않음 (원본 정보 보존)
        # 기존에 접미사를 제거하면서 정보가 손실되었던 문제 해결

        return text

    def _find_best_match(self, year, biz_name, detail_biz_name, threshold=85):
        """
        스마트 매칭 - (YEAR, BIZ_NM, DETAIL_BIZ_NM) 둘 다 일치해야 매칭

        ⭐ 핵심 개선: 원본 텍스트 그대로 비교 (정규화 최소화)

        Returns:
            (plan_id, score, reason) 또는 (None, 0, None)
        """
        if not biz_name or not detail_biz_name:
            return (None, 0, None)

        # 1. 완전 일치 확인 (원본 텍스트 그대로)
        for (db_year, db_biz, db_detail), plan_id in self.existing_plan_data.items():
            if db_year != year:
                continue

            # 원본 그대로 비교 (대소문자 무시, 공백 trim)
            if (db_biz.strip() == biz_name.strip() and
                db_detail.strip() == detail_biz_name.strip()):
                return (plan_id, 100, "완전일치")

        # 2. 유사도 기반 매칭 - 원본 텍스트로 퍼지 매칭
        best_score = 0
        best_plan_id = None
        best_reason = None

        for (db_year, db_biz, db_detail), plan_id in self.existing_plan_data.items():
            if db_year != year:
                continue

            # ⭐ 핵심: 원본 텍스트를 그대로 fuzz 비교
            biz_similarity = fuzz.token_sort_ratio(biz_name, db_biz)
            detail_similarity = fuzz.token_sort_ratio(detail_biz_name, db_detail)

            # 둘 다 threshold 이상이어야 매칭 고려
            if biz_similarity < threshold or detail_similarity < threshold:
                continue

            # 종합 점수: BIZ(50%) + DETAIL(50%)
            combined_score = int((biz_similarity + detail_similarity) / 2)

            if combined_score > best_score:
                best_score = combined_score
                best_plan_id = plan_id
                best_reason = f"BIZ({biz_similarity})+DETAIL({detail_similarity})={combined_score}"

        return (best_plan_id, best_score, best_reason)


    def __init__(self, json_path: str, output_dir: str, db_manager=None):
        self.json_path = Path(json_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.db_manager = db_manager  # Oracle DB 연결 (PLAN_ID 매칭용)

        # 파일명에서 연도 추출 (예: "2024년도 생명공학육성시행계획.json" -> 2024)
        document_year = 0000  # 기본값
        filename = self.json_path.stem  # 확장자 제외한 파일명

        import re
        year_match = re.search(r'(20\d{2})', filename)
        if year_match:
            document_year = int(year_match.group(1))

        logger.info(f" 문서 연도 추출: {filename} -> {document_year}년")

        # ID 카운터 (Oracle DB 형식: 년도 + 일련번호)
        self.id_counters = {
            'sub_project': 1,
            'raw_data': 1,
        }

        # sub_project_id → PLAN_ID 매핑 (Oracle DB용)
        self.plan_id_mapping = {}  # {sub_project_id: PLAN_ID}

        # 데이터 저장소 (Oracle DB 스키마와 동일한 구조)
        self.data = {
            # 메인 테이블 (TB_PLAN_DATA용 - 회사 기존 스키마)
            'plan_data': [],

            # 예산 상세 (TB_PLAN_BUDGET용 - 1:N)
            'budgets': [],

            # 일정 상세 (TB_PLAN_SCHEDULE용 - 1:N)
            'schedules': [],

            # 성과 상세 (TB_PLAN_PERFORMANCE용 - 1:N)
            'performances': [],

            # 대표성과 (TB_PLAN_ACHIEVEMENTS용 - 1:N)
            'achievements': [],

            # 원본 데이터 (감사용, DB 적재 안함)
            'raw_data': [],
        }

        # 컨텍스트
        self.current_context = {
            'sub_project_id': None,
            'document_year': document_year,
            'performance_year': document_year - 1,  # 성과는 전년도
            'plan_year': document_year  # 계획은 당해년도
        }

        # 검증 통계
        self.validation_stats = {
            'total_pages': 0,
            'total_tables': 0,
            'processed_tables': 0,
            'normalized_records': 0,
            'errors': []
        }

        # 기존 PLAN_DATA 캐시 (YEAR, BIZ_NM, DETAIL_BIZ_NM) -> PLAN_ID
        self.existing_plan_data = {}
        if db_manager:
            self._load_existing_plan_data()

    def _load_existing_plan_data(self):
        """기존 PLAN_DATA를 DB에서 로드 (캐시용)"""
        logger.info("[LOAD] 기존 PLAN_DATA 로드 중...")
        try:
            cursor = self.db_manager.connection.cursor()
            query = """
                SELECT PLAN_ID, YEAR, BIZ_NM, DETAIL_BIZ_NM
                FROM TB_PLAN_DATA
                WHERE DELETE_YN = 'N'
            """
            cursor.execute(query)

            count = 0
            for plan_id, year, biz_nm, detail_biz_nm in cursor:
                count += 1
                # ⭐ 원본 그대로 저장 (trim만 적용)
                biz_nm_clean = biz_nm.strip() if biz_nm else ""
                detail_biz_nm_clean = detail_biz_nm.strip() if detail_biz_nm else ""

                key = (year, biz_nm_clean, detail_biz_nm_clean)
                self.existing_plan_data[key] = plan_id.strip() if plan_id else None

                # 디버깅: 2021년 데이터 처음 10개 출력
                if year == 2021 and count <= 10:
                    logger.info(f"   [{plan_id}] BIZ='{biz_nm_clean}' / DETAIL='{detail_biz_nm_clean}'")

            cursor.close()
            logger.info(f"[OK] 기존 PLAN_DATA 로드 완료: {len(self.existing_plan_data)}건")
        except Exception as e:
            logger.warning(f"[WARN] 기존 PLAN_DATA 로드 실패: {e}")

    def _get_next_id(self, entity_type: str) -> int:
        """ID 생성"""
        current = self.id_counters[entity_type]
        self.id_counters[entity_type] += 1
        return current

    def _save_raw_data(self, data_type: str, content: Any,
                      page_number: int, table_index: int) -> int:
        """원본 데이터 저장 (감사용, DB에 적재하지 않음)"""
        raw_id = self._get_next_id('raw_data')

        self.data['raw_data'].append({
            'id': raw_id,
            'data_type': data_type,
            'data_year': self.current_context.get(f'{data_type}_year',
                                                 self.current_context['document_year']),
            'raw_content': json.dumps(content, ensure_ascii=False) if isinstance(content, (dict, list)) else str(content),
            'page_number': page_number,
            'table_index': table_index,
            'created_at': datetime.now().isoformat()
        })

        return raw_id

    def _extract_key_achievements(self, full_text: str, page_number: int) -> List[Dict]:
        """대표성과 추출 - TB_PLAN_ACHIEVEMENTS용"""
        achievements = []

        if not self.current_context.get('sub_project_id'):
            return []

        #  PLAN_ID를 가져오되, 없으면 빈 문자열
        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')

        # "① 대표성과" 섹션 찾기
        match = re.search(r'①\s*대표성과(.*?)(?:②|③|\(2\)|\(3\)|$)', full_text, re.DOTALL)
        if not match:
            return achievements

        achievement_text = match.group(1).strip()

        # "○" 기호로 개별 성과 분리
        individual_achievements = re.split(r'\n○\s+', achievement_text)

        for idx, achievement in enumerate(individual_achievements):
            achievement = achievement.strip()
            if achievement and len(achievement) > 10:  # 최소 길이 체크
                achievements.append({
                    'PLAN_ID': plan_id,
                    'ACHIEVEMENT_YEAR': self.current_context['performance_year'],
                    'ACHIEVEMENT_ORDER': idx + 1,
                    'DESCRIPTION': achievement[:4000]  # VARCHAR2(4000) 제한
                })

        return achievements

    def _aggregate_plan_data_fields(self):
        """
        하위 테이블(BUDGET, SCHEDULE)에서 계산한 데이터를 TB_PLAN_DATA 집계 필드에 채우기
        - RESPERIOD, CUR_RESPERIOD (연구기간)
        - BIZ_SDT, BIZ_EDT (사업 시작일/종료일)
        - TOTAL_RESPRC, TOTAL_RESPRC_GOV, TOTAL_RESPRC_CIV (총 연구비)
        - CUR_RESPRC, CUR_RESPRC_GOV, CUR_RESPRC_CIV (당해연도 연구비)
        - PERFORM_PRC, PLAN_PRC (실적/계획 비용)
        """
        logger.info("� TB_PLAN_DATA 집계 필드 계산 중...")

        for plan_data in self.data['plan_data']:
            plan_id = plan_data['PLAN_ID']
            doc_year = plan_data['YEAR']

            # ============================================================
            # 1. 예산 데이터로부터 집계 (TB_PLAN_BUDGET)
            # ============================================================
            plan_budgets = [b for b in self.data['budgets'] if b['PLAN_ID'] == plan_id]

            if plan_budgets:
                # 총 연구비 집계 (모든 연도 합산)
                total_gov = sum(b.get('GOV_AMOUNT') or 0 for b in plan_budgets)
                total_private = sum(b.get('PRIVATE_AMOUNT') or 0 for b in plan_budgets)
                total_local = sum(b.get('LOCAL_AMOUNT') or 0 for b in plan_budgets)
                total_etc = sum(b.get('ETC_AMOUNT') or 0 for b in plan_budgets)
                total_all = total_gov + total_private + total_local + total_etc

                plan_data['TOTAL_RESPRC'] = f"{total_all:,.0f}" if total_all > 0 else None
                plan_data['TOTAL_RESPRC_GOV'] = total_gov if total_gov > 0 else None
                plan_data['TOTAL_RESPRC_CIV'] = total_private if total_private > 0 else None

                # 당해연도 연구비 집계 (문서 연도만)
                cur_year_budgets = [b for b in plan_budgets if b.get('BUDGET_YEAR') == doc_year]
                cur_gov = sum(b.get('GOV_AMOUNT') or 0 for b in cur_year_budgets)
                cur_private = sum(b.get('PRIVATE_AMOUNT') or 0 for b in cur_year_budgets)
                cur_local = sum(b.get('LOCAL_AMOUNT') or 0 for b in cur_year_budgets)
                cur_etc = sum(b.get('ETC_AMOUNT') or 0 for b in cur_year_budgets)
                cur_all = cur_gov + cur_private + cur_local + cur_etc

                plan_data['CUR_RESPRC'] = f"{cur_all:,.0f}" if cur_all > 0 else None
                plan_data['CUR_RESPRC_GOV'] = cur_gov if cur_gov > 0 else None
                plan_data['CUR_RESPRC_CIV'] = cur_private if cur_private > 0 else None

                # 실적 비용 (실적 연도 합산)
                perform_budgets = [b for b in plan_budgets if b.get('CATEGORY') == '실적']
                perform_total = sum(b.get('TOTAL_AMOUNT') or 0 for b in perform_budgets)
                plan_data['PERFORM_PRC'] = perform_total if perform_total > 0 else None

                # 계획 비용 (계획 연도 합산)
                plan_budgets_only = [b for b in plan_budgets if b.get('CATEGORY') == '계획']
                plan_total = sum(b.get('TOTAL_AMOUNT') or 0 for b in plan_budgets_only)
                plan_data['PLAN_PRC'] = plan_total if plan_total > 0 else None

                # 연구기간 계산 (예산 테이블의 최소~최대 연도)
                all_years = [b['BUDGET_YEAR'] for b in plan_budgets if b.get('BUDGET_YEAR')]
                if all_years:
                    min_year = min(all_years)
                    max_year = max(all_years)
                    plan_data['RESPERIOD'] = f"{min_year}~{max_year}"

                    # 당해연도 연구기간 (문서 연도만)
                    if doc_year in all_years:
                        plan_data['CUR_RESPERIOD'] = f"{doc_year}"

            # ============================================================
            # 2. 일정 데이터로부터 사업 시작일/종료일 (TB_PLAN_SCHEDULE)
            # ============================================================
            plan_schedules = [s for s in self.data['schedules'] if s['PLAN_ID'] == plan_id]

            if plan_schedules:
                # START_DATE가 있는 레코드에서 최소값
                start_dates = [s.get('START_DATE') for s in plan_schedules if s.get('START_DATE')]
                if start_dates:
                    plan_data['BIZ_SDT'] = min(start_dates)

                # END_DATE가 있는 레코드에서 최대값
                end_dates = [s.get('END_DATE') for s in plan_schedules if s.get('END_DATE')]
                if end_dates:
                    plan_data['BIZ_EDT'] = max(end_dates)

        logger.info(" TB_PLAN_DATA 집계 완료")

    def _extract_plan_details(self, full_text: str, page_number: int) -> List[Dict]:
        """주요 추진계획 내용 추출"""
        plans = []

        # "① 주요 추진계획 내용" 섹션 찾기
        match = re.search(r'①\s*주요\s*추진계획\s*내용(.*?)(?:②|③|\(2\)|\(3\)|$)', full_text, re.DOTALL)

        # 패턴1이 없으면 "(3) 년도 추진계획" 섹션에서 ① 이후 내용 찾기 (연도 무관)
        if not match:
            match = re.search(r'\(3\)\s*\d{4}년도\s*추진계획\s*①\s*(.*?)(?:②|③|$)', full_text, re.DOTALL)

        if not match:
            return []

        plan_text = match.group(1).strip()

        # "○" 또는 "-" 기호로 개별 계획 분리
        individual_plans = re.split(r'\n[○\-]\s+', plan_text)

        for idx, plan in enumerate(individual_plans):
            plan = plan.strip()
            if plan and len(plan) > 5:
                plans.append({
                    'sub_project_id': self.current_context['sub_project_id'],
                    'plan_year': self.current_context['plan_year'],
                    'plan_order': idx + 1,
                    'description': plan,
                    'page_number': page_number
                })

        return plans

    def _extract_qualitative_achievements(self, full_text: str, page_num: int) -> List[Dict]:
        """정성적 성과 추출 (텍스트 기반)"""
        normalized = []

        if not self.current_context.get('sub_project_id'):
            return []

        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')
        year = self.current_context.get('plan_year', self.current_context.get('document_year'))

        # "추진실적", "주요성과" 섹션 찾기
        patterns = [
            r'(?:추진실적|주요성과)\s*[:\n]?\s*(.*?)(?=\n\n|$|\(2\)|②)',
            r'○\s*(?:추진실적|주요성과)\s*(.*?)(?=○|$)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, full_text, re.DOTALL)
            for match in matches:
                content = match.strip()
                if len(content) > 10:  # 최소 길이 체크
                    # 줄바꿈으로 분리하여 여러 항목 추출
                    items = [item.strip() for item in content.split('\n') if item.strip()]

                    for item in items:
                        # 불릿 포인트나 숫자로 시작하는 항목
                        if re.match(r'^[•\-\d).]\s*', item):
                            normalized.append({
                                'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                                'PERFORMANCE_YEAR': year,
                                'PERFORMANCE_TYPE': '정성적실적',
                                'CATEGORY': '추진실적',
                                'VALUE': None,
                                'UNIT': None,
                                'ORIGINAL_TEXT': item[:4000]
                            })

        return normalized

    def _normalize_schedule_data(self, period: str, task: str, detail: str,
                                raw_data_id: int) -> List[Dict]:
        """일정 데이터 정규화 - 세부일정(task/detail)에서 실제 날짜 추출"""
        normalized = []
        year = self.current_context['plan_year']

        if not self.current_context.get('sub_project_id'):
            return []

        #  PLAN_ID 가져오기
        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')

        if not period or not task or period in ['구분', '추진일정', '추진사항', '항목', '주요내용']:
            return []

        #  task와 detail을 합쳐서 전체 텍스트로 처리
        full_task_text = f"{task}\n{detail}" if detail else task

        # task를 개별 항목으로 분리
        task_items = []
        if '•' in full_task_text:
            parts = full_task_text.split('•')
            for part in parts:
                part = part.strip()
                if part:
                    task_items.append('• ' + part)
        else:
            task_items = [full_task_text]

        def get_quarter_end_date(year: int, quarter: int) -> str:
            month_end = quarter * 3
            return f"{year}-{month_end:02d}-{[31,30,30,31][quarter-1]:02d}"

        #  세부일정 텍스트에서 실제 날짜 추출
        def extract_month_range_from_detail(text):
            """
            세부일정에서 실제 날짜 추출:
            - '20.1월~12월
            - 1월~3월
            - 21년 1월
            """
            # 패턴 1: "'20.1월~12월", "'21.1월~3월"
            match1 = re.search(r"'(\d{2})\.(\d+)월\s*[~\-]\s*(\d+)월", text)
            if match1:
                year_short = int(match1.group(1))
                start_month = int(match1.group(2))
                end_month = int(match1.group(3))
                full_year = 2000 + year_short
                return (full_year, start_month, end_month)

            # 패턴 2: "1월~12월", "1월 ~ 3월"
            match2 = re.search(r'(\d+)월\s*[~\-]\s*(\d+)월', text)
            if match2:
                start_month = int(match2.group(1))
                end_month = int(match2.group(2))
                return (year, start_month, end_month)

            # 패턴 3: "'20.1~12", "2020.1~12"
            match3 = re.search(r"'?(\d{2,4})\.(\d+)\s*[~\-]\s*(\d+)", text)
            if match3:
                year_str = match3.group(1)
                full_year = 2000 + int(year_str) if len(year_str) == 2 else int(year_str)
                start_month = int(match3.group(2))
                end_month = int(match3.group(3))
                return (full_year, start_month, end_month)

            # 패턴 4: "21년 1월" (단일 월)
            match4 = re.search(r'(\d{2})년\s*(\d+)월', text)
            if match4:
                year_short = int(match4.group(1))
                month = int(match4.group(2))
                full_year = 2000 + year_short
                return (full_year, month, month)

            return None

        def extract_quarters(period_text):
            quarters = []
            if '~' in period_text and '분기' in period_text:
                quarter_match = re.search(r'(\d)/4\s*분기\s*~\s*(\d)/4\s*분기', period_text)
                if quarter_match:
                    start_q = int(quarter_match.group(1))
                    end_q = int(quarter_match.group(2))
                    quarters = list(range(start_q, end_q + 1))
            elif '연중' in period_text:
                quarters = [1, 2, 3, 4]
            elif '분기' in period_text:
                quarter_match = re.search(r'(\d)/4\s*분기', period_text)
                if quarter_match:
                    quarters = [int(quarter_match.group(1))]
            return quarters

        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')

        for task_item in task_items:
            task_item = task_item.strip()
            if not task_item:
                continue

            task_category = ""
            if '•' in task_item:
                first_line = task_item.split('\n')[0].replace('•', '').strip()
                task_category = first_line

            #  1순위: 세부일정에서 실제 날짜 추출
            month_info = extract_month_range_from_detail(task_item)

            if month_info:
                parsed_year, start_month, end_month = month_info
                import calendar
                last_day = calendar.monthrange(parsed_year, end_month)[1]

                record = {
                    'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                    'SCHEDULE_YEAR': parsed_year,
                    'QUARTER': f"{start_month}월~{end_month}월",
                    'TASK_NAME': task_category[:768] if task_category else None,
                    'TASK_CONTENT': task_item[:4000] if task_item else None,
                    'START_DATE': f"{parsed_year}-{start_month:02d}-01",
                    'END_DATE': f"{parsed_year}-{end_month:02d}-{last_day:02d}"
                }
                normalized.append(record)
            else:
                #  2순위: period의 분기로 대체
                quarters = extract_quarters(period)

                if quarters:
                    for quarter in quarters:
                        record = {
                            'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                            'SCHEDULE_YEAR': year,
                            'QUARTER': f"{quarter}/4분기",
                            'TASK_NAME': task_category[:768] if task_category else None,
                            'TASK_CONTENT': task_item[:4000] if task_item else None,
                            'START_DATE': f"{year}-{(quarter-1)*3+1:02d}-01",
                            'END_DATE': get_quarter_end_date(year, quarter)
                        }
                        normalized.append(record)
                else:
                    record = {
                        'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                        'SCHEDULE_YEAR': year,
                        'QUARTER': '연중',
                        'TASK_NAME': task_category[:768] if task_category else None,
                        'TASK_CONTENT': task_item[:4000] if task_item else None,
                        'START_DATE': f"{year}-01-01",
                        'END_DATE': f"{year}-12-31"
                    }
                    normalized.append(record)

        return normalized

    def _normalize_performance_table(self, rows: List[List], raw_data_id: int) -> List[Dict]:
        """성과 테이블 정규화 - PLAN_ID만 사용"""
        normalized = []
        year = self.current_context['performance_year']

        if not rows or len(rows) < 2:
            return []

        if not self.current_context.get('sub_project_id'):
            return []

        #  PLAN_ID 가져오기
        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')

        # 테이블 타입 감지
        header_text = ' '.join(str(c) for c in rows[0]).lower()

        # 1. 특허/논문 복합 테이블
        if '특허성과' in header_text and '논문성과' in header_text:
            if len(rows) >= 4:
                data_row = rows[-1]

                # 특허 데이터
                for indicator_type, idx in [('국내출원', 0), ('국내등록', 1), ('국외출원', 2), ('국외등록', 3)]:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0:
                                    normalized.append({
                                        'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                                        'PERFORMANCE_YEAR': year,
                                        'PERFORMANCE_TYPE': '특허',
                                        'CATEGORY': indicator_type,
                                        'VALUE': val,
                                        'UNIT': '건',
                                        'ORIGINAL_TEXT': str(rows)[:4000]
                                    })
                        except: pass

                # 논문 데이터
                for indicator_type, idx in [('IF20이상', 4), ('IF10이상', 5), ('SCIE', 6), ('비SCIE', 7)]:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0:
                                    normalized.append({
                                        'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                                        'PERFORMANCE_YEAR': year,
                                        'PERFORMANCE_TYPE': '논문',
                                        'CATEGORY': indicator_type,
                                        'VALUE': val,
                                        'UNIT': '편',
                                        'ORIGINAL_TEXT': str(rows)[:4000]
                                    })
                        except: pass

        # 2. 기술이전 테이블
        elif '기술이전' in header_text or '기술료' in header_text:
            if len(rows) >= 3:
                data_row = rows[-1]
                indicators = [('기술지도', 0, '건'), ('기술이전', 1, '건'), ('기술료', 3, '백만원')]

                for category, idx, unit in indicators:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0:
                                    normalized.append({
                                        'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                                        'PERFORMANCE_YEAR': year,
                                        'PERFORMANCE_TYPE': '기술이전',
                                        'CATEGORY': category,
                                        'VALUE': val,
                                        'UNIT': unit,
                                        'ORIGINAL_TEXT': str(rows)[:4000]
                                    })
                        except: pass

        # 3. 국제협력 테이블
        elif '국제협력' in header_text or '해외연구자' in header_text:
            if len(rows) >= 3:
                data_row = rows[-1]
                indicators = [('해외연구자유치', 0, '명'), ('국내연구자파견', 1, '명'), ('국제학술회의개최', 2, '건')]

                for category, idx, unit in indicators:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0:
                                    normalized.append({
                                        'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                                        'PERFORMANCE_YEAR': year,
                                        'PERFORMANCE_TYPE': '국제협력',
                                        'CATEGORY': category,
                                        'VALUE': val,
                                        'UNIT': unit,
                                        'ORIGINAL_TEXT': str(rows)[:4000]
                                    })
                        except: pass

        # 4. 인력양성 테이블
        elif '학위배출' in header_text or '박사' in header_text:
            if len(rows) >= 3:
                data_row = rows[-1]
                indicators = [('박사배출', 0, '명'), ('석사배출', 1, '명'), ('연구과제참여인력', 4, '명')]

                for category, idx, unit in indicators:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0:
                                    normalized.append({
                                        'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                                        'PERFORMANCE_YEAR': year,
                                        'PERFORMANCE_TYPE': '인력양성',
                                        'CATEGORY': category,
                                        'VALUE': val,
                                        'UNIT': unit,
                                        'ORIGINAL_TEXT': str(rows)[:4000]
                                    })
                        except: pass

        return normalized

    def _normalize_budget_data(self, rows: List[List], raw_data_id: int) -> List[Dict]:
        """예산 데이터 정규화 - Oracle TB_PLAN_BUDGET 스키마에 맞춤"""
        normalized = []

        if not rows or len(rows) < 2:
            return []

        if not self.current_context.get('sub_project_id'):
            return []

        #  PLAN_ID를 가져오되, 없으면 빈 문자열 (나중에 매칭)
        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')

        # 1단계: 연도 컬럼 파싱
        year_columns = {}  # {컬럼 인덱스: (연도, 실적/계획)}
        first_row = rows[0]

        for col_idx, cell in enumerate(first_row):
            cell_str = str(cell).strip()
            lines = cell_str.split('\n')
            year = None
            category = '계획'

            for line in lines:
                line = line.strip()
                year_match = re.search(r'(20\d{2})', line)
                if year_match:
                    year = int(year_match.group(1))
                if '실적' in line:
                    category = '실적'
                elif '계획' in line:
                    category = '계획'

            if year:
                year_columns[col_idx] = (year, category)

        if not year_columns:
            return []

        # 2단계: 예산 타입별로 금액 집계 (연도별로 그룹화)
        budget_by_year = {}  # {(year, category): {gov: amount, private: amount, ...}}

        for row_idx, row in enumerate(rows[1:], 1):
            if not any(cell for cell in row if cell and str(cell).strip()):
                continue

            # 예산 타입 추출 (보통 3번째 컬럼)
            budget_type_col_idx = 2
            for idx, cell in enumerate(first_row):
                if '구 분' in str(cell) or '구분' in str(cell):
                    budget_type_col_idx = idx
                    break

            if budget_type_col_idx >= len(row):
                continue

            budget_type_text = str(row[budget_type_col_idx]).strip()

            # 스킵 키워드
            if any(kw in budget_type_text for kw in ['소계', '합계', '총계', '사업명', '구분']):
                continue

            # 예산 타입 매핑
            budget_type_key = None
            if '정부' in budget_type_text or '국비' in budget_type_text:
                budget_type_key = 'gov'
            elif '민간' in budget_type_text:
                budget_type_key = 'private'
            elif '지방' in budget_type_text:
                budget_type_key = 'local'
            else:
                budget_type_key = 'etc'

            # 각 연도 컬럼의 금액 추출
            for col_idx, (year, category) in year_columns.items():
                if col_idx >= len(row):
                    continue

                cell_str = str(row[col_idx]).strip()
                if not cell_str or cell_str in ['-', '', 'nan']:
                    continue

                try:
                    amount = float(cell_str.replace(',', '').replace('백만원', '').strip().split('\n')[0])
                    if amount <= 0:
                        continue

                    key = (year, category)
                    if key not in budget_by_year:
                        budget_by_year[key] = {'gov': 0, 'private': 0, 'local': 0, 'etc': 0}
                    budget_by_year[key][budget_type_key] += amount

                except (ValueError, TypeError):
                    continue

        # 3단계: Oracle 스키마에 맞게 레코드 생성
        for (year, category), amounts in budget_by_year.items():
            total = amounts['gov'] + amounts['private'] + amounts['local'] + amounts['etc']

            record = {
                'PLAN_ID': plan_id,  #  매칭된 PLAN_ID 사용
                'BUDGET_YEAR': year,
                'CATEGORY': category,
                'TOTAL_AMOUNT': total if total > 0 else None,
                'GOV_AMOUNT': amounts['gov'] if amounts['gov'] > 0 else None,
                'PRIVATE_AMOUNT': amounts['private'] if amounts['private'] > 0 else None,
                'LOCAL_AMOUNT': amounts['local'] if amounts['local'] > 0 else None,
                'ETC_AMOUNT': amounts['etc'] if amounts['etc'] > 0 else None,
                'PERFORM_PRC': total if category == '실적' else None,
                'PLAN_PRC': total if category == '계획' else None
            }
            normalized.append(record)

        return normalized

    def _process_overview(self, full_text: str, tables: List[Dict], page_number: int, raw_data_id: int):
        """사업개요 처리 - TB_PLAN_DATA 업데이트 (개선 버전)"""

        if not self.current_context.get('sub_project_id'):
            return

        #  PLAN_ID를 가져오되, 없으면 None (업데이트 불가)
        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'])
        if not plan_id:
            # 빈 문자열이나 TEMP_로 시작하면 진행 (나중에 매칭)
            plan_id = ''

        # 테이블에서 기본 정보 추출 (다양한 키워드 매칭)
        overview_data = {}
        for table in tables:
            rows = table.get('data', [])
            for row in rows:
                if len(row) >= 2:
                    key = str(row[0]).strip()
                    value = str(row[1]).strip()
                    if key and value:
                        overview_data[key] = value

        # 대체 키워드 매핑
        def get_value_with_alternatives(data: dict, *keys):
            """여러 키워드를 시도하여 값 찾기"""
            for key in keys:
                if key in data and data[key]:
                    return data[key]
            return None

        # full_text에서 사업목표, 사업내용 추출 (다양한 패턴)
        objective = ""
        content = ""

        # 사업목표 추출 (여러 패턴 시도)
        obj_patterns = [
            r'○\s*사업목표\s*(.*?)(?:○\s*사업내용|○\s*추진내용|\(2\)|②|$)',
            r'사업\s*목표\s*[:\s]*(.*?)(?:사업\s*내용|추진\s*내용|\(2\)|②|$)',
            r'최종\s*목표\s*[:\s]*(.*?)(?:사업\s*내용|추진\s*내용|\(2\)|②|$)',
        ]
        for pattern in obj_patterns:
            match = re.search(pattern, full_text, re.DOTALL | re.IGNORECASE)
            if match:
                objective = match.group(1).strip()
                if len(objective) > 10:  # 최소 길이 체크
                    break

        # 사업내용 추출 (여러 패턴 시도)
        content_patterns = [
            r'○\s*사업내용\s*(.*?)(?:\(2\)|②|③|$)',
            r'사업\s*내용\s*[:\s]*(.*?)(?:\(2\)|②|③|$)',
            r'추진\s*내용\s*[:\s]*(.*?)(?:\(2\)|②|③|$)',
        ]
        for pattern in content_patterns:
            match = re.search(pattern, full_text, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1).strip()
                if len(content) > 10:
                    break

        # ✨ 신규: 텍스트 본문에서 주관기관, 관리기관 추출 (PDF 구조 반영)
        lead_organ_text = None
        mng_organ_text = None

        # 주관기관 패턴 (예: "○ 주관기관 : 과학기술정보통신부")
        lead_patterns = [
            r'○\s*주관기관\s*[:：]\s*([^\n○]+)',
            r'주관\s*기관\s*[:：]\s*([^\n○]+)',
            r'○\s*추진기관\s*[:：]\s*([^\n○]+)',
        ]
        for pattern in lead_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                lead_organ_text = match.group(1).strip()
                if len(lead_organ_text) > 2:
                    break

        # 관리기관 패턴 (예: "○ 관리기관 : 한국연구재단")
        mng_patterns = [
            r'○\s*관리기관\s*[:：]\s*([^\n○]+)',
            r'관리\s*기관\s*[:：]\s*([^\n○]+)',
            r'○\s*전담기관\s*[:：]\s*([^\n○]+)',
        ]
        for pattern in mng_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                mng_organ_text = match.group(1).strip()
                if len(mng_organ_text) > 2:
                    break

        # ✨ 신규: 대표분야 및 비중 텍스트 파싱 (예: "대표분야 생명과학 비중 생명과학(100), Red(10), Green(10), White(10)")
        rep_fld_text = None
        biology_wei = None
        red_wei = None
        green_wei = None
        white_wei = None

        # 대표분야 패턴
        rep_fld_patterns = [
            r'대표분야\s*[:：]?\s*([가-힣]+)',
            r'대표\s*분야\s*[:：]?\s*([가-힣]+)',
        ]
        for pattern in rep_fld_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                rep_fld_text = match.group(1).strip()
                break

        # 비중 패턴 (예: "생명과학(100), Red(10), Green(10), White(10)")
        weight_pattern = r'비중\s*[:：]?\s*([^\n○]+)'
        weight_match = re.search(weight_pattern, full_text, re.IGNORECASE)
        if weight_match:
            weight_text = weight_match.group(1).strip()

            # 개별 가중치 추출 (생명과학(100), Red(10) 형식)
            biology_match = re.search(r'생명과학\s*\((\d+)\)', weight_text)
            if biology_match:
                biology_wei = int(biology_match.group(1))

            red_match = re.search(r'Red\s*\((\d+)\)', weight_text, re.IGNORECASE)
            if red_match:
                red_wei = int(red_match.group(1))

            green_match = re.search(r'Green\s*\((\d+)\)', weight_text, re.IGNORECASE)
            if green_match:
                green_wei = int(green_match.group(1))

            white_match = re.search(r'White\s*\((\d+)\)', weight_text, re.IGNORECASE)
            if white_match:
                white_wei = int(white_match.group(1))

        # TB_PLAN_DATA 레코드 찾아서 업데이트
        for plan_data in self.data['plan_data']:
            if plan_data['PLAN_ID'] == plan_id:
                #  부처명 (NATION_ORGAN_NM) - 필수 필드!
                nation_organ = get_value_with_alternatives(
                    overview_data,
                    '부처명', '소관부처', '소관', '부처',
                    '주무부처', '담당부처'
                )
                if not nation_organ:
                    # 본문에서 『부처명』 패턴 추출
                    bracket_match = re.search(r'『\s*([^』]+)\s*』', full_text)
                    if bracket_match:
                        nation_organ = bracket_match.group(1).strip()

                if not nation_organ and lead_organ_text:
                    # 주관기관에서 "~부" 형태 추출
                    lead_organ_str = str(lead_organ_text) if lead_organ_text else ""
                    ministry_match = re.search(r'([가-힣]+부)(?:관리기관|전담기관|주관|[\s:]|$)', lead_organ_str)
                    if ministry_match:
                        nation_organ = ministry_match.group(1).strip()

                # 필수 필드 - NULL 방지
                if nation_organ:
                    plan_data['NATION_ORGAN_NM'] = self._clean_text(nation_organ, 768)
                else:
                    logger.warning(f" 부처명 미추출: {plan_data.get('BIZ_NM', 'unknown')}")
                    plan_data['NATION_ORGAN_NM'] = "미분류"

                #  DETAIL_BIZ_NM: 세부사업명 (overview 테이블의 "세부사업명")
                detail_biz_nm = get_value_with_alternatives(overview_data, '세부사업명', '사업명', '사업이름')
                if detail_biz_nm:
                    plan_data['DETAIL_BIZ_NM'] = self._clean_text(detail_biz_nm, 768)

                #  BIZ_NM: 내역사업명 (overview 테이블의 "내역사업명")
                biz_nm = get_value_with_alternatives(overview_data, '내역사업명')
                if biz_nm:
                    plan_data['BIZ_NM'] = self._clean_text(biz_nm, 768)

                biz_type = get_value_with_alternatives(overview_data, '사업성격', '사업유형', '유형')
                plan_data['BIZ_TYPE'] = self._clean_text(biz_type, 768) if biz_type else None

                rep_fld = rep_fld_text if rep_fld_text else get_value_with_alternatives(overview_data, '대표분야', '분야', '대표 분야')
                plan_data['REP_FLD'] = self._clean_text(rep_fld, 768) if rep_fld else None

                # 가중치 (텍스트에서 추출한 값 저장)
                if biology_wei is not None:
                    plan_data['BIOLOGY_WEI'] = biology_wei
                if red_wei is not None:
                    plan_data['RED_WEI'] = red_wei
                if green_wei is not None:
                    plan_data['GREEN_WEI'] = green_wei
                if white_wei is not None:
                    plan_data['WHITE_WEI'] = white_wei

                # 3대 영역
                area = get_value_with_alternatives(overview_data, '3대영역', '3대 영역', '영역')
                plan_data['AREA'] = area[:768] if area else None

                # 주관기관 (텍스트 추출 우선, 없으면 테이블에서)
                lead_organ = lead_organ_text if lead_organ_text else get_value_with_alternatives(
                    overview_data,
                    '주관기관', '주관 기관', '주관기관명',
                    '주관', '주관부처', '전담기관'
                )
                plan_data['LEAD_ORGAN_NM'] = self._clean_text(lead_organ, 768) if lead_organ else None

                # 관리기관 (텍스트 추출 우선, 없으면 테이블에서)
                mng_organ = mng_organ_text if mng_organ_text else get_value_with_alternatives(
                    overview_data,
                    '관리기관', '관리 기관', '관리기관명',
                    '총괄기관', '전문기관'
                )
                plan_data['MNG_ORGAN_NM'] = self._clean_text(mng_organ, 768) if mng_organ else None

                # 최종목표
                plan_data['LAST_GOAL'] = self._clean_text(objective, 4000) if objective else None

                # 사업내용
                plan_data['BIZ_CONTENTS'] = self._clean_text(content, 4000) if content else None

                # 디버그 로그 (처음 3개 사업만)
                if plan_data['NUM'] <= 3:
                    logger.debug(f"� PLAN_ID {plan_id} overview 업데이트:")
                    logger.debug(f"  - BIZ_TYPE: {'' if plan_data['BIZ_TYPE'] else ''}")
                    logger.debug(f"  - LEAD_ORGAN_NM: {'' if plan_data['LEAD_ORGAN_NM'] else ''}")
                    logger.debug(f"  - LAST_GOAL: {'' if plan_data['LAST_GOAL'] else ''} ({len(objective) if objective else 0}자)")
                    logger.debug(f"  - BIZ_CONTENTS: {'' if plan_data['BIZ_CONTENTS'] else ''} ({len(content) if content else 0}자)")

                break

    def _process_sub_project(self, text: str, tables: List[Dict]) -> bool:
        """내역사업 처리 - TB_PLAN_DATA 생성"""
        biz_name = None         # 세부사업명 → BIZ_NM (DB 구조에 맞게)
        detail_biz_name = None  # 내역사업명 → DETAIL_BIZ_NM (DB 구조에 맞게)

        # 테이블에서 찾기
        for table in tables:
            rows = table.get('data', [])
            for row in rows:
                if len(row) < 2:
                    continue

                key = str(row[0]).strip()
                value = str(row[1]).strip()

                if '세부사업명' in key and value:
                    biz_name = value         # 세부사업명 → BIZ_NM
                elif '내역사업명' in key and value:
                    detail_biz_name = value  # 내역사업명 → DETAIL_BIZ_NM
                elif '사업명' in key and value and '세부' not in key and '내역' not in key:
                    # "사업명"만 있으면 BIZ_NM으로 사용 (세부사업명이 없을 때 대체)
                    if not biz_name:
                        biz_name = value

        # 텍스트에서 찾기 (테이블에서 못 찾았을 경우)
        if not biz_name:
            match = re.search(r'세부사업명\s+([^\n]+)', text)
            if match:
                biz_name = match.group(1).strip()

        if not detail_biz_name:
            match = re.search(r'내역사업명\s+([^\n]+)', text)
            if match:
                detail_biz_name = match.group(1).strip()

        if not biz_name:
            return False

        #  DETAIL_BIZ_NM이 없으면 BIZ_NM을 기본값으로 사용
        if not detail_biz_name:
            detail_biz_name = biz_name
            logger.debug(f"   DETAIL_BIZ_NM 없음 → BIZ_NM 사용: {biz_name}")

        # 이미 등록된 내역사업인지 체크 (BIZ_NM + DETAIL_BIZ_NM 조합으로 정확히 매칭)
        for plan_data in self.data['plan_data']:
            # BIZ_NM과 DETAIL_BIZ_NM 둘 다 일치해야 재사용
            if (plan_data.get('BIZ_NM') == biz_name and
                plan_data.get('DETAIL_BIZ_NM') == detail_biz_name):
                self.current_context['sub_project_id'] = plan_data['_internal_id']
                logger.info(f"[REUSE] 기존 내역사업 재사용: {biz_name} / {detail_biz_name} (PLAN_ID: {plan_data['PLAN_ID']})")
                return True

        #  스마트 매칭으로 기존 Oracle DB에서 PLAN_ID 조회
        year = self.current_context['document_year']

        logger.info(f"[MATCH] 매칭 시도: YEAR={year}, BIZ_NM='{biz_name}', DETAIL_BIZ_NM='{detail_biz_name}'")

        # 스마트 매칭 수행 (원본 텍스트 그대로 사용)
        existing_plan_id, match_score, match_reason = self._find_best_match(year, biz_name, detail_biz_name)

        if existing_plan_id:
            if match_score == 100:
                logger.info(f"[OK] 완전 일치: {detail_biz_name} -> {existing_plan_id}")
            else:
                logger.info(f"[PARTIAL] 부분 일치 ({match_score}점, {match_reason}): {detail_biz_name} -> {existing_plan_id}")
        else:
            logger.warning(f"[FAIL] 매칭 실패 - BIZ_NM: '{biz_name}', DETAIL_BIZ_NM: '{detail_biz_name}'")

        # 새로운 내역사업 생성
        sub_id = self._get_next_id('sub_project')

        # PLAN_ID 결정
        if existing_plan_id:
            # ✅ DB에서 매칭 성공 → 기존 PLAN_ID 그대로 사용
            plan_id = existing_plan_id
            logger.info(f"[USE] 기존 PLAN_ID 사용: {plan_id}")
        else:
            # ❌ DB에서 매칭 실패 → 임시 PLAN_ID 부여
            plan_id = f"TEMP_{year}_{str(sub_id).zfill(4)}"
            logger.warning(f"[TEMP] 임시 PLAN_ID 생성: {plan_id}")

        # TB_PLAN_DATA 레코드 생성 (회사 기존 43개 컬럼)
        #  페이지 텍스트에서 부처명(NATION_ORGAN_NM) 추출
        nation_organ = None
        bracket_match = re.search(r'『\s*([^』]+)\s*』', text)
        if bracket_match:
            nation_organ = bracket_match.group(1).strip()

        # TB_PLAN_DATA 레코드 생성 (회사 기존 43개 컬럼)
        plan_data_record = {
            '_internal_id': sub_id,
            'PLAN_ID': plan_id,
            'YEAR': self.current_context['document_year'],
            'NUM': sub_id,
            'NATION_ORGAN_NM': self._clean_text(nation_organ, 768) if nation_organ else "미분류",
            'BIZ_NM': biz_name if biz_name else '',  # 내역사업명 → BIZ_NM
            'DETAIL_BIZ_NM': detail_biz_name if detail_biz_name else '',  # 세부사업명 → DETAIL_BIZ_NM
            'BIZ_TYPE': None,
            'AREA': None,
            'REP_FLD': None,
            'BIOLOGY_WEI': None,  # 가중치는 NULL (나중에 수동 입력)
            'RED_WEI': None,
            'GREEN_WEI': None,
            'WHITE_WEI': None,
            'FUSION_WEI': None,
            'LEAD_ORGAN_NM': None,
            'MNG_ORGAN_NM': None,
            'BIZ_SDT': None,
            'BIZ_EDT': None,
            'RESPERIOD': None,
            'CUR_RESPERIOD': None,
            'TOTAL_RESPRC': None,  # 나중에 예산 테이블에서 집계
            'TOTAL_RESPRC_GOV': None,
            'TOTAL_RESPRC_CIV': None,
            'CUR_RESPRC': None,
            'CUR_RESPRC_GOV': None,
            'CUR_RESPRC_CIV': None,
            'LAST_GOAL': None,
            'BIZ_CONTENTS': None,
            'BIZ_CONTENTS_KEYWORD': None,
            'REGUL_WEI': None,
            'WEI': None,
            'PERFORM_PRC': None,
            'PLAN_PRC': None
        }

        self.data['plan_data'].append(plan_data_record)
        self.current_context['sub_project_id'] = sub_id
        self.plan_id_mapping[sub_id] = plan_id  # 매핑 저장

        logger.info(f"[NEW] 내역사업 등록: {detail_biz_name} (ID: {sub_id}, PLAN_ID: {plan_id})")
        return True


    def normalize(self, json_data: Dict) -> bool:
        """JSON 데이터 정규화 (전체 처리)"""
        try:
            logger.info(f"[START] 정부 표준 정규화 시작")

            #  메타데이터에서 문서 연도 추출 (우선순위 1)
            metadata = json_data.get('metadata', {})
            if metadata and 'document_year' in metadata:
                self.current_context['document_year'] = metadata['document_year']
                logger.info(f"[YEAR] JSON metadata에서 연도 추출: {metadata['document_year']}년")
            #  JSON 파일명에서 추출한 연도 유지 (우선순위 2)
            else:
                logger.info(f"[YEAR] 파일명에서 연도 사용: {self.current_context['document_year']}년")

            self.current_context['performance_year'] = self.current_context['document_year'] - 1
            self.current_context['plan_year'] = self.current_context['document_year']

            # 페이지별 처리
            pages_data = json_data.get('pages', [])
            self.validation_stats['total_pages'] = len(pages_data)

            for page in pages_data:
                page_num = page.get('page_number', 1)
                page_category = page.get('category')
                page_sub_project = page.get('sub_project')
                page_full_text = page.get('full_text', '')
                page_tables = page.get('tables', [])

                self.validation_stats['total_tables'] += len(page_tables)

                # sub_project가 페이지에 명시되어 있으면 설정/전환 (null이 아닐 때만)
                if page_sub_project:
                    # 이미 등록된 내역사업인지 체크 (BIZ_NM = 내역사업명)
                    existing_project = None
                    for plan_data in self.data['plan_data']:
                        # BIZ_NM(내역사업명)으로 매칭
                        if plan_data['BIZ_NM'] == page_sub_project:
                            existing_project = plan_data
                            break

                    if existing_project:
                        # 기존 프로젝트로 전환
                        if self.current_context.get('sub_project_id') != existing_project['_internal_id']:
                            self.current_context['sub_project_id'] = existing_project['_internal_id']
                            logger.info(f"[SWITCH] 내역사업 전환: {page_sub_project} (PLAN_ID: {existing_project['PLAN_ID']})")
                    else:
                        # 새로운 내역사업 처리
                        self._process_sub_project(page_full_text, page_tables)
                elif '내역사업명' in page_full_text or '세부사업명' in page_full_text:
                    # 페이지에 sub_project 정보가 없지만 텍스트에 있으면 찾기
                    self._process_sub_project(page_full_text, page_tables)
                # else: 내역사업 정보가 없으면 이전 페이지의 sub_project_id를 유지

                # sub_project_id가 여전히 없으면 경고 후 건너뛰기
                if not self.current_context.get('sub_project_id'):
                    logger.debug(f" 페이지 {page_num}: sub_project_id 없음, 건너뜀")
                    continue

                # 원본 데이터 저장
                raw_data_id = self._save_raw_data(
                    page_category or 'unknown',
                    {'full_text': page_full_text, 'tables': page_tables},
                    page_num,
                    0
                )

                # ⭐ 대표성과는 모든 페이지에서 추출 (category와 무관)
                if self.current_context.get('sub_project_id'):
                    # 대표성과 추출
                    if '① 대표성과' in page_full_text:
                        achievements = self._extract_key_achievements(page_full_text, page_num)
                        self.data['achievements'].extend(achievements)

                # 카테고리별 처리
                if page_category == 'overview':
                    # 사업개요 처리
                    self._process_overview(page_full_text, page_tables, page_num, raw_data_id)

                elif page_category == 'performance':

                    # 페이지 텍스트에서 정성적 성과 추출
                    qualitative = self._extract_qualitative_achievements(page_full_text, page_num)
                    if qualitative:
                        self.data['performances'].extend(qualitative)
                        self.validation_stats['normalized_records'] += len(qualitative)

                    # 테이블 처리 (성과 또는 예산)
                    for idx, table in enumerate(page_tables):
                        rows = table.get('data', [])
                        if not rows:
                            continue

                        # 테이블 타입 감지
                        header_text = ' '.join(str(c) for c in rows[0]).lower()

                        # 예산 테이블인지 확인 (performance 카테고리에 예산 테이블이 있을 수 있음)
                        if '사업비' in header_text or ('구분' in header_text and '실적' in header_text and '계획' in header_text):
                            # 예산 테이블
                            table_raw_id = self._save_raw_data('plan', table, page_num, idx)
                            normalized = self._normalize_budget_data(rows, table_raw_id)
                            self.data['budgets'].extend(normalized)
                            self.validation_stats['normalized_records'] += len(normalized)
                        else:
                            # 성과 테이블
                            table_raw_id = self._save_raw_data('performance', table, page_num, idx)
                            normalized = self._normalize_performance_table(rows, table_raw_id)
                            self.data['performances'].extend(normalized)
                            self.validation_stats['normalized_records'] += len(normalized)

                        self.validation_stats['processed_tables'] += 1

                elif page_category == 'plan':

                    # 테이블 처리
                    for idx, table in enumerate(page_tables):
                        rows = table.get('data', [])
                        if not rows:
                            continue

                        table_raw_id = self._save_raw_data('plan', table, page_num, idx)

                        # 테이블 타입 감지
                        header_text = ' '.join(str(c) for c in rows[0]).lower()

                        if '일정' in header_text or '분기' in header_text or '추진' in header_text:
                            # 일정 테이블
                            for row in rows[1:]:
                                if len(row) >= 2:
                                    period = str(row[0]).strip()
                                    task = str(row[1]).strip() if len(row) > 1 else ""
                                    detail = str(row[2]).strip() if len(row) > 2 else ""

                                    if period and '구분' not in period:
                                        normalized = self._normalize_schedule_data(
                                            period, task, detail, table_raw_id
                                        )
                                        self.data['schedules'].extend(normalized)
                                        self.validation_stats['normalized_records'] += len(normalized)

                        elif '예산' in header_text or '사업비' in header_text:
                            # 예산 테이블
                            normalized = self._normalize_budget_data(rows, table_raw_id)
                            self.data['budgets'].extend(normalized)
                            self.validation_stats['normalized_records'] += len(normalized)

                        self.validation_stats['processed_tables'] += 1

            #  최종 단계: "미분류" NATION_ORGAN_NM 재검색
            logger.info(" 미분류 부처명 재검색 중...")
            for plan_data in self.data['plan_data']:
                if plan_data['NATION_ORGAN_NM'] == "미분류":
                    # 해당 내역사업의 모든 페이지에서 『』 패턴 찾기
                    sub_project_name = plan_data['BIZ_NM']  # 내역사업명

                    for page in pages_data:
                        page_full_text = page.get('full_text', '')

                        # 이 페이지가 해당 내역사업과 관련있는지 확인
                        if sub_project_name in page_full_text:
                            bracket_match = re.search(r'『\s*([^』]+)\s*』', page_full_text)
                            if bracket_match:
                                nation_organ = bracket_match.group(1).strip()
                                plan_data['NATION_ORGAN_NM'] = self._clean_text(nation_organ, 768)
                                logger.info(f" 부처명 발견: {sub_project_name} -> {nation_organ}")
                                break

            logger.info(f" 정규화 완료: {len(self.data['plan_data'])}개 내역사업")
            return True

        except Exception as e:
            logger.error(f"처리 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_csv(self):
        """CSV 저장 - TB_PLAN_DATA 기반 스키마"""

        #  1단계: 하위 테이블에 PLAN_ID 채우기 (plan_id_mapping 사용)
        for budget in self.data['budgets']:
            if not budget.get('PLAN_ID') or budget['PLAN_ID'] == '':
                # _internal_id를 통해 PLAN_ID 찾기 (이미 정규화 단계에서 채워짐)
                # 하지만 정규화 시 sub_project_id로 저장되었으므로, plan_data에서 찾아야 함
                pass  # 이미 정규화 단계에서 채워져 있어야 함

        for schedule in self.data['schedules']:
            if not schedule.get('PLAN_ID') or schedule['PLAN_ID'] == '':
                pass  # 이미 정규화 단계에서 채워져 있어야 함

        for performance in self.data['performances']:
            if not performance.get('PLAN_ID') or performance['PLAN_ID'] == '':
                pass  # 이미 정규화 단계에서 채워져 있어야 함

        for achievement in self.data['achievements']:
            if not achievement.get('PLAN_ID') or achievement['PLAN_ID'] == '':
                pass  # 이미 정규화 단계에서 채워져 있어야 함

        #  2단계: TB_PLAN_DATA 집계 필드 계산
        self._aggregate_plan_data_fields()


        # TB_PLAN_DATA (회사 기존 43개 컬럼, _internal_id 제외)
        if self.data['plan_data']:
            csv_path = self.output_dir / "TB_PLAN_DATA.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                # _internal_id 제외한 전체 컬럼
                fieldnames = [k for k in self.data['plan_data'][0].keys()
                            if k != '_internal_id']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in self.data['plan_data']:
                    row = {k: v for k, v in record.items()
                          if k != '_internal_id'}
                    writer.writerow(row)
            logger.info(f" TB_PLAN_DATA.csv 저장 ({len(self.data['plan_data'])}건)")

        # TB_PLAN_BUDGET
        if self.data['budgets']:
            csv_path = self.output_dir / "TB_PLAN_BUDGET.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                #  PLAN_ID를 첫 번째 컬럼으로
                fieldnames = ['PLAN_ID', 'BUDGET_YEAR', 'CATEGORY', 'TOTAL_AMOUNT',
                             'GOV_AMOUNT', 'PRIVATE_AMOUNT', 'LOCAL_AMOUNT', 'ETC_AMOUNT',
                             'PERFORM_PRC', 'PLAN_PRC']
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.data['budgets'])
            logger.info(f" TB_PLAN_BUDGET.csv 저장 ({len(self.data['budgets'])}건)")

        # TB_PLAN_SCHEDULE
        if self.data['schedules']:
            csv_path = self.output_dir / "TB_PLAN_SCHEDULE.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                #  PLAN_ID를 첫 번째 컬럼으로
                fieldnames = ['PLAN_ID', 'SCHEDULE_YEAR', 'QUARTER', 'TASK_NAME',
                             'TASK_CONTENT', 'START_DATE', 'END_DATE']
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.data['schedules'])
            logger.info(f" TB_PLAN_SCHEDULE.csv 저장 ({len(self.data['schedules'])}건)")

        # TB_PLAN_PERFORMANCE
        if self.data['performances']:
            csv_path = self.output_dir / "TB_PLAN_PERFORMANCE.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                #  PLAN_ID를 첫 번째 컬럼으로
                fieldnames = ['PLAN_ID', 'PERFORMANCE_YEAR', 'PERFORMANCE_TYPE', 'CATEGORY',
                             'INDICATOR_NAME', 'TARGET_VALUE', 'ACTUAL_VALUE', 'UNIT',
                             'ORIGINAL_TEXT', 'VALUE']
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.data['performances'])
            logger.info(f" TB_PLAN_PERFORMANCE.csv 저장 ({len(self.data['performances'])}건)")

        # TB_PLAN_ACHIEVEMENTS
        if self.data['achievements']:
            csv_path = self.output_dir / "TB_PLAN_ACHIEVEMENTS.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                #  PLAN_ID를 첫 번째 컬럼으로 명시적 순서 지정
                fieldnames = ['PLAN_ID', 'ACHIEVEMENT_YEAR', 'ACHIEVEMENT_TYPE',
                             'TITLE', 'DESCRIPTION', 'PAGE_NUMBER']
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(self.data['achievements'])
            logger.info(f" TB_PLAN_ACHIEVEMENTS.csv 저장 ({len(self.data['achievements'])}건)")

        # 원본 데이터 (감사용)
        if self.data['raw_data']:
            csv_path = self.output_dir / "raw_data.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['raw_data'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['raw_data'])
            logger.info(f" raw_data.csv 저장 ({len(self.data['raw_data'])}건)")

    def print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("� 정부 표준 정규화 완료 (TB_PLAN_DATA + 하위 테이블)")
        print("="*80)

        print(f"\n� 내역사업 (TB_PLAN_DATA): {len(self.data['plan_data'])}개")
        for plan_data in self.data['plan_data'][:10]:  # 처음 10개만 표시
            print(f"  - {plan_data['BIZ_NM']} (PLAN_ID: {plan_data['PLAN_ID']})")
        if len(self.data['plan_data']) > 10:
            print(f"  ... 외 {len(self.data['plan_data']) - 10}개")

        print(f"\n� Oracle 테이블별 데이터 통계:")
        print(f"  TB_PLAN_DATA:        {len(self.data['plan_data'])}건")
        print(f"  TB_PLAN_BUDGET:      {len(self.data['budgets'])}건")
        print(f"  TB_PLAN_SCHEDULE:    {len(self.data['schedules'])}건")
        print(f"  TB_PLAN_PERFORMANCE: {len(self.data['performances'])}건")
        print(f"  TB_PLAN_ACHIEVEMENTS: {len(self.data['achievements'])}건")
        print(f"  raw_data (감사용):    {len(self.data['raw_data'])}건")

        print("="*80 + "\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("사용법: python normalize_government_standard.py <JSON파일경로> [출력디렉토리]")
        print("예제: python normalize_government_standard.py output/2024년도_생명공학육성시행계획.json normalized_output_government")
        sys.exit(1)

    json_file = sys.argv[1]
    output_folder = sys.argv[2] if len(sys.argv) > 2 else "normalized_output_government"

    if not Path(json_file).exists():
        print(f" 파일을 찾을 수 없습니다: {json_file}")
        sys.exit(1)

    normalizer = GovernmentStandardNormalizer(json_file, output_folder)

    with open(json_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    success = normalizer.normalize(json_data)

    if success:
        normalizer.save_to_csv()
        normalizer.print_statistics()
        print(f"\n 정규화 완료! CSV 저장 위치: {output_folder}/")
    else:
        print(" 정규화 실패!")
        sys.exit(1)

"""
정부/공공기관 표준 데이터 정규화 시스템 - 완전 개선 버전
모든 데이터 누락 없이 정규화
"""
import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GovernmentStandardNormalizer:
    """정부 표준 정규화 클래스 - 모든 데이터 포함"""

    def __init__(self, json_path: str, output_dir: str):
        self.json_path = Path(json_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # 파일명에서 연도 추출 (예: "2024년도 생명공학육성시행계획.json" -> 2024)
        document_year = 2024  # 기본값
        filename = self.json_path.stem  # 확장자 제외한 파일명

        import re
        year_match = re.search(r'(20\d{2})', filename)
        if year_match:
            document_year = int(year_match.group(1))

        logger.info(f"📅 문서 연도 추출: {filename} -> {document_year}년")

        # ID 카운터 (Oracle DB 형식: 년도 + 일련번호)
        self.id_counters = {
            'sub_project': 1,
            'raw_data': 1,
        }

        # sub_project_id → PLAN_ID 매핑 (Oracle DB용)
        self.plan_id_mapping = {}  # {sub_project_id: PLAN_ID}

        # 데이터 저장소 (Oracle DB 스키마와 동일한 구조)
        self.data = {
            # 마스터 테이블 (TB_PLAN_MASTER용)
            'master': [],

            # 상세 정보 (TB_PLAN_DETAIL용)
            'details': [],

            # 예산 정보 (TB_PLAN_BUDGET용)
            'budgets': [],

            # 일정 정보 (TB_PLAN_SCHEDULE용)
            'schedules': [],

            # 성과 정보 (TB_PLAN_PERFORMANCE용)
            'performances': [],

            # 비중 정보 (TB_PLAN_WEIGHT용)
            'weights': [],

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
        """대표성과 추출"""
        achievements = []

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
                    'sub_project_id': self.current_context['sub_project_id'],
                    'achievement_year': self.current_context['performance_year'],
                    'achievement_order': idx + 1,
                    'description': achievement,
                    'page_number': page_number
                })

        return achievements

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
                                'PLAN_ID': plan_id,
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

        if not period or not task or period in ['구분', '추진일정', '추진사항', '항목', '주요내용']:
            return []

        # ✅ task와 detail을 합쳐서 전체 텍스트로 처리
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

        # ✅ 세부일정 텍스트에서 실제 날짜 추출
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

            # ✅ 1순위: 세부일정에서 실제 날짜 추출
            month_info = extract_month_range_from_detail(task_item)

            if month_info:
                parsed_year, start_month, end_month = month_info
                import calendar
                last_day = calendar.monthrange(parsed_year, end_month)[1]

                record = {
                    'PLAN_ID': plan_id,
                    'SCHEDULE_YEAR': parsed_year,
                    'QUARTER': f"{start_month}월~{end_month}월",
                    'TASK_NAME': task_category[:768] if task_category else None,
                    'TASK_CONTENT': task_item[:4000] if task_item else None,
                    'START_DATE': f"{parsed_year}-{start_month:02d}-01",
                    'END_DATE': f"{parsed_year}-{end_month:02d}-{last_day:02d}"
                }
                normalized.append(record)
            else:
                # ✅ 2순위: period의 분기로 대체
                quarters = extract_quarters(period)

                if quarters:
                    for quarter in quarters:
                        record = {
                            'PLAN_ID': plan_id,
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
                        'PLAN_ID': plan_id,
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
        """성과 테이블 정규화 - 모든 성과 지표 포함"""
        normalized = []
        year = self.current_context['performance_year']

        if not rows or len(rows) < 2:
            return []

        # 테이블 타입 감지
        header_text = ' '.join(str(c) for c in rows[0]).lower()

        # 1. 특허/논문 복합 테이블
        if '특허성과' in header_text and '논문성과' in header_text:
            if len(rows) >= 4:
                data_row = rows[-1]  # 마지막 행이 실제 데이터

                # 특허 데이터 추출 (0-3번 컬럼)
                patent_indicators = [
                    ('국내출원', 0), ('국내등록', 1),
                    ('국외출원', 2), ('국외등록', 3)
                ]

                for indicator_type, idx in patent_indicators:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0 and self.current_context.get('sub_project_id'):
                                    plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')
                                    normalized.append({
                                        'PLAN_ID': plan_id,
                                        'PERFORMANCE_YEAR': year,
                                        'PERFORMANCE_TYPE': '특허',
                                        'CATEGORY': indicator_type,
                                        'VALUE': val,
                                        'UNIT': '건',
                                        'ORIGINAL_TEXT': str(rows)[:4000]
                                    })
                        except: pass

                # 논문 데이터 추출 (4-7번 컬럼)
                paper_indicators = [
                    ('IF20이상', 4), ('IF10이상', 5),
                    ('SCIE', 6), ('비SCIE', 7)
                ]

                for indicator_type, idx in paper_indicators:
                    if idx < len(data_row):
                        try:
                            val_str = str(data_row[idx]).replace(',', '').strip()
                            if val_str and val_str != '-':
                                val = float(val_str)
                                if val > 0 and self.current_context.get('sub_project_id'):
                                    plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')
                                    normalized.append({
                                        'PLAN_ID': plan_id,
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

                # 기술지도 (0번 컬럼)
                if len(data_row) > 0:
                    try:
                        val_str = str(data_row[0]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '기술이전',
                                    'indicator_type': '기술지도',
                                    'value': val,
                                    'unit': '건',
                                    'original_text': str(rows)
                                })
                    except: pass

                # 기술이전 (1번 컬럼)
                if len(data_row) > 1:
                    try:
                        val_str = str(data_row[1]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '기술이전',
                                    'indicator_type': '기술이전',
                                    'value': val,
                                    'unit': '건',
                                    'original_text': str(rows)
                                })
                    except: pass

                # 기술료 금액 (3번 컬럼)
                if len(data_row) > 3:
                    try:
                        val_str = str(data_row[3]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '기술이전',
                                    'indicator_type': '기술료',
                                    'value': val,
                                    'unit': '백만원',
                                    'original_text': str(rows)
                                })
                    except: pass

        # 3. 국제협력 테이블
        elif '국제협력' in header_text or '해외연구자' in header_text:
            if len(rows) >= 3:
                data_row = rows[-1]

                # 해외연구자 유치 (0번 컬럼)
                if len(data_row) > 0:
                    try:
                        val_str = str(data_row[0]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '국제협력',
                                    'indicator_type': '해외연구자유치',
                                    'value': val,
                                    'unit': '명',
                                    'original_text': str(rows)
                                })
                    except: pass

                # 국내연구자 파견 (1번 컬럼)
                if len(data_row) > 1:
                    try:
                        val_str = str(data_row[1]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '국제협력',
                                    'indicator_type': '국내연구자파견',
                                    'value': val,
                                    'unit': '명',
                                    'original_text': str(rows)
                                })
                    except: pass

                # 국제학술회의 개최 (2번 컬럼)
                if len(data_row) > 2:
                    try:
                        val_str = str(data_row[2]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '국제협력',
                                    'indicator_type': '국제학술회의개최',
                                    'value': val,
                                    'unit': '건',
                                    'original_text': str(rows)
                                })
                    except: pass

        # 4. 인력양성 테이블
        elif '학위배출' in header_text or '박사' in header_text:
            if len(rows) >= 3:
                data_row = rows[-1]

                # 박사 (0번 컬럼)
                if len(data_row) > 0:
                    try:
                        val_str = str(data_row[0]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '인력양성',
                                    'indicator_type': '박사배출',
                                    'value': val,
                                    'unit': '명',
                                    'original_text': str(rows)
                                })
                    except: pass

                # 석사 (1번 컬럼)
                if len(data_row) > 1:
                    try:
                        val_str = str(data_row[1]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '인력양성',
                                    'indicator_type': '석사배출',
                                    'value': val,
                                    'unit': '명',
                                    'original_text': str(rows)
                                })
                    except: pass

                # 연구과제 참여인력 (4번 컬럼)
                if len(data_row) > 4:
                    try:
                        val_str = str(data_row[4]).replace(',', '').strip()
                        if val_str and val_str != '-':
                            val = float(val_str)
                            if val > 0:
                                normalized.append({
                                    'id': self._get_next_id('performance'),
                                    'sub_project_id': self.current_context['sub_project_id'],
                                    'raw_data_id': raw_data_id,
                                    'document_year': self.current_context['document_year'],
                                    'performance_year': year,
                                    'indicator_category': '인력양성',
                                    'indicator_type': '연구과제참여인력',
                                    'value': val,
                                    'unit': '명',
                                    'original_text': str(rows)
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

        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'], '')
        if not plan_id:
            return []

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
                'PLAN_ID': plan_id,
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
        """사업개요 처리 - TB_PLAN_DETAIL 업데이트"""

        if not self.current_context.get('sub_project_id'):
            return

        plan_id = self.plan_id_mapping.get(self.current_context['sub_project_id'])
        if not plan_id:
            return

        # 테이블에서 기본 정보 추출
        overview_data = {}
        for table in tables:
            rows = table.get('data', [])
            for row in rows:
                if len(row) >= 2:
                    key = str(row[0]).strip()
                    value = str(row[1]).strip()
                    if key and value:
                        overview_data[key] = value

        # full_text에서 사업목표, 사업내용 추출
        objective = ""
        content = ""

        obj_match = re.search(r'○\s*사업목표\s*(.*?)(?:○\s*사업내용|$)', full_text, re.DOTALL)
        if obj_match:
            objective = obj_match.group(1).strip()

        content_match = re.search(r'○\s*사업내용\s*(.*?)(?:\(2\)|②|$)', full_text, re.DOTALL)
        if content_match:
            content = content_match.group(1).strip()

        # TB_PLAN_DETAIL 레코드 찾아서 업데이트
        for detail in self.data['details']:
            if detail['PLAN_ID'] == plan_id:
                detail['BIZ_TYPE'] = overview_data.get('사업성격', '')[:768] if overview_data.get('사업성격') else None
                detail['REP_FLD'] = overview_data.get('대표분야', '')[:768] if overview_data.get('대표분야') else None
                detail['LEAD_ORGAN_NM'] = overview_data.get('주관기관', '')[:768] if overview_data.get('주관기관') else None
                detail['MNG_ORGAN_NM'] = overview_data.get('관리기관', '')[:768] if overview_data.get('관리기관') else None
                detail['LAST_GOAL'] = objective[:4000] if objective else None
                detail['BIZ_CONTENTS'] = content[:4000] if content else None
                break

    def _process_sub_project(self, text: str, tables: List[Dict]) -> bool:
        """내역사업 처리"""
        sub_project_name = None
        main_project_name = None

        # 테이블에서 찾기
        for table in tables:
            rows = table.get('data', [])
            for row in rows:
                if len(row) < 2:
                    continue

                key = str(row[0]).strip()
                value = str(row[1]).strip()

                if '내역사업명' in key and value:
                    sub_project_name = value
                elif '세부사업명' in key:
                    main_project_name = value

        # 텍스트에서 찾기 (테이블에서 못 찾았을 경우)
        if not sub_project_name:
            match = re.search(r'내역사업명\s+([^\n]+)', text)
            if match:
                sub_project_name = match.group(1).strip()

        if not main_project_name:
            match = re.search(r'세부사업명\s+([^\n]+)', text)
            if match:
                main_project_name = match.group(1).strip()

        if not sub_project_name:
            return False

        # 이미 등록된 내역사업인지 체크
        for master in self.data['master']:
            if master['DETAIL_BIZ_NM'] == sub_project_name:
                self.current_context['sub_project_id'] = master['_internal_id']
                logger.info(f"📌 기존 내역사업 재사용: {sub_project_name} (PLAN_ID: {master['PLAN_ID']})")
                return True

        # 새로운 내역사업 생성
        sub_id = self._get_next_id('sub_project')

        # Oracle DB PLAN_ID 형식: 년도 + 3자리 일련번호 (예: 2024001)
        plan_id = f"{self.current_context['document_year']}{str(sub_id).zfill(3)}"

        # TB_PLAN_MASTER 레코드 생성
        master_record = {
            '_internal_id': sub_id,  # 내부 매핑용 (CSV 저장 안함)
            'PLAN_ID': plan_id,
            'YEAR': self.current_context['document_year'],
            'NUM': sub_id,
            'NATION_ORGAN_NM': '과학기술정보통신부',
            'BIZ_NM': main_project_name or '바이오·의료기술개발사업',
            'DETAIL_BIZ_NM': sub_project_name
        }

        # TB_PLAN_DETAIL 레코드 생성 (초기값, 나중에 overview에서 업데이트)
        detail_record = {
            'DETAIL_ID': f"{plan_id}D01",
            'PLAN_ID': plan_id,
            'BIZ_TYPE': None,
            'REP_FLD': None,
            'AREA': None,
            'LEAD_ORGAN_NM': None,
            'MNG_ORGAN_NM': None,
            'BIZ_SDT': None,
            'BIZ_EDT': None,
            'RESPERIOD': None,
            'CUR_RESPERIOD': None,
            'LAST_GOAL': None,
            'BIZ_CONTENTS': None,
            'BIZ_CONTENTS_KEYWORD': None
        }

        self.data['master'].append(master_record)
        self.data['details'].append(detail_record)
        self.current_context['sub_project_id'] = sub_id
        self.plan_id_mapping[sub_id] = plan_id  # 매핑 저장

        logger.info(f"✅ 내역사업 등록: {sub_project_name} (ID: {sub_id}, PLAN_ID: {plan_id})")
        return True


    def normalize(self, json_data: Dict) -> bool:
        """JSON 데이터 정규화 (전체 처리)"""
        try:
            logger.info(f"🚀 정부 표준 정규화 시작")

            # 메타데이터에서 문서 연도 추출
            metadata = json_data.get('metadata', {})
            self.current_context['document_year'] = metadata.get('document_year', datetime.now().year)
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
                    # 이미 등록된 내역사업인지 체크
                    existing_project = None
                    for master in self.data['master']:
                        if master['DETAIL_BIZ_NM'] == page_sub_project:
                            existing_project = master
                            break

                    if existing_project:
                        # 기존 프로젝트로 전환
                        if self.current_context.get('sub_project_id') != existing_project['_internal_id']:
                            self.current_context['sub_project_id'] = existing_project['_internal_id']
                            logger.info(f"📌 내역사업 전환: {page_sub_project} (PLAN_ID: {existing_project['PLAN_ID']})")
                    else:
                        # 새로운 내역사업 처리
                        self._process_sub_project(page_full_text, page_tables)
                elif '내역사업명' in page_full_text or '세부사업명' in page_full_text:
                    # 페이지에 sub_project 정보가 없지만 텍스트에 있으면 찾기
                    self._process_sub_project(page_full_text, page_tables)
                # else: 내역사업 정보가 없으면 이전 페이지의 sub_project_id를 유지

                # sub_project_id가 여전히 없으면 경고 후 건너뛰기
                if not self.current_context.get('sub_project_id'):
                    logger.debug(f"⚠️ 페이지 {page_num}: sub_project_id 없음, 건너뜀")
                    continue

                # 원본 데이터 저장
                raw_data_id = self._save_raw_data(
                    page_category or 'unknown',
                    {'full_text': page_full_text, 'tables': page_tables},
                    page_num,
                    0
                )

                # ⭐ 대표성과와 주요계획은 모든 페이지에서 추출 (category와 무관)
                if self.current_context.get('sub_project_id'):
                    # 대표성과 추출
                    if '① 대표성과' in page_full_text:
                        achievements = self._extract_key_achievements(page_full_text, page_num)
                        self.data['key_achievements'].extend(achievements)

                    # 주요 추진계획 추출 (여러 패턴 지원)
                    if ('① 주요 추진계획' in page_full_text or
                        '① 주요추진계획' in page_full_text or
                        re.search(r'\(3\)\s*\d{4}년도\s*추진계획', page_full_text)):
                        plan_details = self._extract_plan_details(page_full_text, page_num)
                        self.data['plan_details'].extend(plan_details)

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

            logger.info(f"✅ 정규화 완료: {len(self.data['master'])}개 내역사업")
            return True

        except Exception as e:
            logger.error(f"처리 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_csv(self):
        """CSV 저장 - Oracle DB 스키마에 맞춤"""

        # TB_PLAN_MASTER (내부 ID 제외)
        if self.data['master']:
            csv_path = self.output_dir / "TB_PLAN_MASTER.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                fieldnames = ['PLAN_ID', 'YEAR', 'NUM', 'NATION_ORGAN_NM', 'BIZ_NM', 'DETAIL_BIZ_NM']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in self.data['master']:
                    # _internal_id는 저장하지 않음
                    row = {k: v for k, v in record.items() if k != '_internal_id'}
                    writer.writerow(row)
            logger.info(f"✅ TB_PLAN_MASTER.csv 저장 ({len(self.data['master'])}건)")

        # TB_PLAN_DETAIL
        if self.data['details']:
            csv_path = self.output_dir / "TB_PLAN_DETAIL.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['details'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['details'])
            logger.info(f"✅ TB_PLAN_DETAIL.csv 저장 ({len(self.data['details'])}건)")

        # TB_PLAN_BUDGET
        if self.data['budgets']:
            csv_path = self.output_dir / "TB_PLAN_BUDGET.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['budgets'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['budgets'])
            logger.info(f"✅ TB_PLAN_BUDGET.csv 저장 ({len(self.data['budgets'])}건)")

        # TB_PLAN_SCHEDULE
        if self.data['schedules']:
            csv_path = self.output_dir / "TB_PLAN_SCHEDULE.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['schedules'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['schedules'])
            logger.info(f"✅ TB_PLAN_SCHEDULE.csv 저장 ({len(self.data['schedules'])}건)")

        # TB_PLAN_PERFORMANCE
        if self.data['performances']:
            csv_path = self.output_dir / "TB_PLAN_PERFORMANCE.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['performances'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['performances'])
            logger.info(f"✅ TB_PLAN_PERFORMANCE.csv 저장 ({len(self.data['performances'])}건)")

        # TB_PLAN_WEIGHT (현재는 비어있을 수 있음)
        if self.data['weights']:
            csv_path = self.output_dir / "TB_PLAN_WEIGHT.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['weights'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['weights'])
            logger.info(f"✅ TB_PLAN_WEIGHT.csv 저장 ({len(self.data['weights'])}건)")

        # 원본 데이터 (감사용)
        if self.data['raw_data']:
            csv_path = self.output_dir / "raw_data.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.data['raw_data'][0].keys())
                writer.writeheader()
                writer.writerows(self.data['raw_data'])
            logger.info(f"✅ raw_data.csv 저장 ({len(self.data['raw_data'])}건)")

    def print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("📊 정부 표준 정규화 완료 (Oracle DB 스키마)")
        print("="*80)

        print(f"\n📁 내역사업 (TB_PLAN_MASTER): {len(self.data['master'])}개")
        for master in self.data['master'][:10]:  # 처음 10개만 표시
            print(f"  - {master['DETAIL_BIZ_NM']} (PLAN_ID: {master['PLAN_ID']})")
        if len(self.data['master']) > 10:
            print(f"  ... 외 {len(self.data['master']) - 10}개")

        print(f"\n📋 Oracle 테이블별 데이터 통계:")
        print(f"  TB_PLAN_MASTER:      {len(self.data['master'])}건")
        print(f"  TB_PLAN_DETAIL:      {len(self.data['details'])}건")
        print(f"  TB_PLAN_BUDGET:      {len(self.data['budgets'])}건")
        print(f"  TB_PLAN_SCHEDULE:    {len(self.data['schedules'])}건")
        print(f"  TB_PLAN_PERFORMANCE: {len(self.data['performances'])}건")
        print(f"  TB_PLAN_WEIGHT:      {len(self.data['weights'])}건")
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
        print(f"❌ 파일을 찾을 수 없습니다: {json_file}")
        sys.exit(1)

    normalizer = GovernmentStandardNormalizer(json_file, output_folder)

    with open(json_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    success = normalizer.normalize(json_data)

    if success:
        normalizer.save_to_csv()
        normalizer.print_statistics()
        print(f"\n✅ 정규화 완료! CSV 저장 위치: {output_folder}/")
    else:
        print("❌ 정규화 실패!")
        sys.exit(1)

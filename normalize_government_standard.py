"""
정부/공공기관 표준 데이터 정규화 시스템 - 완전 개선 버전
모든 데이터 누락 없이 정규화
"""
import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from decimal import Decimal
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

        # ID 카운터
        self.id_counters = {
            'sub_project': 1,
            'raw_data': 1,
            'schedule': 1,
            'performance': 1,
            'budget': 1,
            'overview': 1,
            'achievement': 1,
            'plan_detail': 1
        }

        # 데이터 저장소
        self.data = {
            # 마스터
            'sub_projects': [],

            # 원본 데이터 (감사용)
            'raw_data': [],

            # 정규화 데이터 (분석용)
            'normalized_schedules': [],
            'normalized_performances': [],
            'normalized_budgets': [],
            'normalized_overviews': [],

            # 텍스트 데이터
            'key_achievements': [],  # 대표성과
            'plan_details': [],  # 주요 추진계획 내용
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
        """원본 데이터 저장"""
        raw_id = self._get_next_id('raw_data')

        self.data['raw_data'].append({
            'id': raw_id,
            'sub_project_id': self.current_context['sub_project_id'],
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
                    'id': self._get_next_id('achievement'),
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
                    'id': self._get_next_id('plan_detail'),
                    'sub_project_id': self.current_context['sub_project_id'],
                    'plan_year': self.current_context['plan_year'],
                    'plan_order': idx + 1,
                    'description': plan,
                    'page_number': page_number
                })

        return plans

    def _normalize_schedule_data(self, period: str, task: str, detail: str,
                                raw_data_id: int) -> List[Dict]:
        """일정 데이터 정규화 - 분기별로 철저히 분리"""
        normalized = []
        year = self.current_context['plan_year']

        # 헤더나 빈 행 필터링
        if not period or not task or period in ['구분', '추진일정', '추진사항', '항목', '주요내용']:
            return []

        # task를 개별 항목으로 분리 (• 기준)
        task_items = []
        if '•' in task:
            # "• 리더연구\n- 내용\n• 중견연구\n- 내용" 형태를 분리
            parts = task.split('•')
            for part in parts:
                part = part.strip()
                if part:
                    task_items.append('• ' + part)
        else:
            task_items = [task]

        # 분기별 종료일 계산
        def get_quarter_end_date(year: int, quarter: int) -> str:
            month_end = quarter * 3
            if month_end == 3:
                return f"{year}-03-31"
            elif month_end == 6:
                return f"{year}-06-30"
            elif month_end == 9:
                return f"{year}-09-30"
            else:  # 12월
                return f"{year}-12-31"

        # 분기 추출 함수
        def extract_quarters(period_text):
            quarters = []
            # Case 1: 병합된 분기 (1/4분기 ~ 2/4분기)
            if '~' in period_text and '분기' in period_text:
                quarter_match = re.search(r'(\d)/4\s*분기\s*~\s*(\d)/4\s*분기', period_text)
                if quarter_match:
                    start_q = int(quarter_match.group(1))
                    end_q = int(quarter_match.group(2))
                    quarters = list(range(start_q, end_q + 1))
            # Case 2: 연중
            elif '연중' in period_text:
                quarters = [1, 2, 3, 4]
            # Case 3: 단일 분기
            elif '분기' in period_text:
                quarter_match = re.search(r'(\d)/4\s*분기', period_text)
                if quarter_match:
                    quarters = [int(quarter_match.group(1))]
            return quarters

        quarters = extract_quarters(period)

        # 각 항목별로 레코드 생성
        for task_item in task_items:
            task_item = task_item.strip()
            if not task_item:
                continue

            # 작업 카테고리 추출 (• 리더연구 등)
            task_category = ""
            if '•' in task_item:
                # "• 리더연구" 부분 추출
                first_line = task_item.split('\n')[0].replace('•', '').strip()
                task_category = first_line

            # 각 분기별로 레코드 생성
            if quarters:
                for quarter in quarters:
                    record = {
                        'id': self._get_next_id('schedule'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'document_year': self.current_context['document_year'],
                        'year': year,
                        'quarter': quarter,
                        'month_start': (quarter - 1) * 3 + 1,
                        'month_end': quarter * 3,
                        'start_date': f"{year}-{(quarter-1)*3+1:02d}-01",
                        'end_date': get_quarter_end_date(year, quarter),
                        'task_category': task_category,
                        'task_description': task_item,
                        'original_period': period
                    }
                    normalized.append(record)
            else:
                # 분기 정보가 없으면 기본값
                record = {
                    'id': self._get_next_id('schedule'),
                    'sub_project_id': self.current_context['sub_project_id'],
                    'raw_data_id': raw_data_id,
                    'document_year': self.current_context['document_year'],
                    'year': year,
                    'quarter': 0,
                    'month_start': 1,
                    'month_end': 12,
                    'start_date': f"{year}-01-01",
                    'end_date': f"{year}-12-31",
                    'task_category': task_category,
                    'task_description': task_item,
                    'original_period': period
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
                                if val > 0:
                                    normalized.append({
                                        'id': self._get_next_id('performance'),
                                        'sub_project_id': self.current_context['sub_project_id'],
                                        'raw_data_id': raw_data_id,
                                        'document_year': self.current_context['document_year'],
                                        'performance_year': year,
                                        'indicator_category': '특허',
                                        'indicator_type': indicator_type,
                                        'value': val,
                                        'unit': '건',
                                        'original_text': str(rows)
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
                                if val > 0:
                                    normalized.append({
                                        'id': self._get_next_id('performance'),
                                        'sub_project_id': self.current_context['sub_project_id'],
                                        'raw_data_id': raw_data_id,
                                        'document_year': self.current_context['document_year'],
                                        'performance_year': year,
                                        'indicator_category': '논문',
                                        'indicator_type': indicator_type,
                                        'value': val,
                                        'unit': '편',
                                        'original_text': str(rows)
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
        """예산 데이터 정규화 - 셀 내부 줄바꿈 처리 (완전 개선 버전)"""
        normalized = []

        if not rows or len(rows) < 2:
            return []

        # 1단계: 첫 행에서 연도 정보 추출 (셀 내부 줄바꿈 고려)
        year_columns = {}  # {컬럼 인덱스: (연도, 실적/계획)}

        first_row = rows[0]
        for col_idx, cell in enumerate(first_row):
            cell_str = str(cell).strip()

            # 셀 내부에 줄바꿈이 있는 경우: "2020년\n실적" 형태
            lines = cell_str.split('\n')

            year = None
            category = '계획'  # 기본값

            for line in lines:
                line = line.strip()
                # 연도 찾기
                year_match = re.search(r'(20\d{2})', line)
                if year_match:
                    year = int(year_match.group(1))

                # 실적/계획 판단
                if '실적' in line:
                    category = '실적'
                elif '계획' in line:
                    category = '계획'

            if year:
                year_columns[col_idx] = (year, category)

        if not year_columns:
            return []

        # 2단계: 데이터 행 처리 (첫 행 이후)
        for row_idx, row in enumerate(rows[1:], 1):
            # 빈 행 건너뛰기
            if not any(cell for cell in row if cell and str(cell).strip()):
                continue

            # "사업비\n구 분" 컬럼(보통 3번째 컬럼)에서 예산 타입 추출
            # 테이블 구조: [사업명, 사업기간, 구분, 2020년, 2021년, ...]
            budget_type_col_idx = -1

            # "구분" 또는 "사업비" 컬럼 찾기
            for idx, cell in enumerate(first_row):
                cell_lower = str(cell).lower()
                if '구 분' in cell_lower or '구분' in cell_lower or '사업비' in cell_lower:
                    budget_type_col_idx = idx
                    break

            # 기본값: 3번째 컬럼 (인덱스 2)
            if budget_type_col_idx == -1:
                budget_type_col_idx = 2

            # 예산 타입 추출
            if budget_type_col_idx >= len(row):
                continue

            budget_type_text = str(row[budget_type_col_idx]).strip().lower()

            # 스킵 키워드 체크
            skip_keywords = ['소계', '합계', '총계', '사업명', '사업기간', '구분', '사업비']
            if any(skip in budget_type_text for skip in skip_keywords):
                continue

            # 빈 셀 건너뛰기
            if not budget_type_text or budget_type_text in ['-', '']:
                continue

            # 예산 타입 결정
            budget_type = None
            if '정부' in budget_type_text or '국비' in budget_type_text:
                budget_type = '정부'
            elif '민간' in budget_type_text:
                budget_type = '민간'
            elif '지방' in budget_type_text or '지자체' in budget_type_text:
                budget_type = '지방비'
            else:
                # 알 수 없는 타입은 건너뛰기
                continue

            # 3단계: 각 연도 컬럼의 금액 추출
            for col_idx, (year, category) in year_columns.items():
                if col_idx >= len(row):
                    continue

                cell_str = str(row[col_idx]).strip()

                # 빈 값이나 "-" 제외
                if not cell_str or cell_str in ['-', '', 'nan']:
                    continue

                try:
                    # 숫자 추출 (쉼표, 단위 제거)
                    amount_str = cell_str.replace(',', '').replace('백만원', '').replace('억원', '').strip()

                    # 줄바꿈이 있을 수 있으니 첫 번째 라인만
                    amount_str = amount_str.split('\n')[0].strip()

                    if not amount_str or amount_str == '0':
                        continue

                    amount = float(amount_str)

                    if amount <= 0:
                        continue

                    # 실적/계획 구분
                    current_year = self.current_context.get('plan_year', self.current_context.get('document_year', datetime.now().year))
                    is_actual = year < current_year or category == '실적'

                    record = {
                        'id': self._get_next_id('budget'),
                        'sub_project_id': self.current_context['sub_project_id'],
                        'raw_data_id': raw_data_id,
                        'document_year': self.current_context['document_year'],
                        'budget_year': year,
                        'budget_category': category,
                        'budget_type': budget_type,
                        'amount': amount,
                        'currency': 'KRW',
                        'is_actual': is_actual,
                        'original_text': f"{budget_type}: {cell_str}"
                    }
                    normalized.append(record)

                except (ValueError, TypeError) as e:
                    # 파싱 실패는 무시
                    continue

        return normalized

    def _process_overview(self, full_text: str, tables: List[Dict], page_number: int, raw_data_id: int):
        """사업개요 처리 - 전체 텍스트와 테이블 모두 사용"""

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

        # 사업목표 추출
        obj_match = re.search(r'○\s*사업목표\s*(.*?)(?:○\s*사업내용|$)', full_text, re.DOTALL)
        if obj_match:
            objective = obj_match.group(1).strip()

        # 사업내용 추출
        content_match = re.search(r'○\s*사업내용\s*(.*?)(?:\(2\)|②|$)', full_text, re.DOTALL)
        if content_match:
            content = content_match.group(1).strip()

        # DB 스키마에 맞게 데이터 저장
        self.data['normalized_overviews'].append({
            'id': self._get_next_id('overview'),
            'sub_project_id': self.current_context['sub_project_id'],
            'raw_data_id': raw_data_id,
            'overview_type': '사업개요',
            'document_year': self.current_context['document_year'],
            'main_project': overview_data.get('세부사업명', ''),
            'sub_project': overview_data.get('내역사업명', ''),
            'field': overview_data.get('대표분야', ''),
            'project_type': overview_data.get('사업성격', ''),
            'objective': objective,
            'content': content,
            'managing_dept': overview_data.get('주관기관', ''),
            'managing_org': overview_data.get('관리기관', '')
        })

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
        for proj in self.data['sub_projects']:
            if proj['sub_project_name'] == sub_project_name:
                self.current_context['sub_project_id'] = proj['id']
                logger.info(f"📌 기존 내역사업 재사용: {sub_project_name} (ID: {proj['id']})")
                return True

        # 새로운 내역사업 생성
        sub_id = self._get_next_id('sub_project')
        project = {
            'id': sub_id,
            'project_code': f"GOV-{self.current_context['document_year']}-{sub_id:03d}",
            'department_name': '과학기술정보통신부',
            'main_project_name': main_project_name or '바이오·의료기술개발',
            'sub_project_name': sub_project_name,
            'document_year': self.current_context['document_year']
        }

        self.data['sub_projects'].append(project)
        self.current_context['sub_project_id'] = sub_id

        logger.info(f"✅ 내역사업 등록: {sub_project_name} (ID: {sub_id})")
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
                    for proj in self.data['sub_projects']:
                        if proj['sub_project_name'] == page_sub_project:
                            existing_project = proj
                            break

                    if existing_project:
                        # 기존 프로젝트로 전환
                        if self.current_context.get('sub_project_id') != existing_project['id']:
                            self.current_context['sub_project_id'] = existing_project['id']
                            logger.info(f"📌 내역사업 전환: {page_sub_project} (ID: {existing_project['id']})")
                    else:
                        # 새로운 내역사업 처리
                        self._process_sub_project(page_full_text, page_tables)
                else:
                    # 페이지에 sub_project 정보가 없으면 텍스트/테이블에서 찾기
                    if '내역사업명' in page_full_text:
                        self._process_sub_project(page_full_text, page_tables)

                # sub_project_id가 없으면 건너뛰기
                if not self.current_context.get('sub_project_id'):
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
                            self.data['normalized_budgets'].extend(normalized)
                            self.validation_stats['normalized_records'] += len(normalized)
                        else:
                            # 성과 테이블
                            table_raw_id = self._save_raw_data('performance', table, page_num, idx)
                            normalized = self._normalize_performance_table(rows, table_raw_id)
                            self.data['normalized_performances'].extend(normalized)
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
                                        self.data['normalized_schedules'].extend(normalized)
                                        self.validation_stats['normalized_records'] += len(normalized)

                        elif '예산' in header_text or '사업비' in header_text:
                            # 예산 테이블
                            normalized = self._normalize_budget_data(rows, table_raw_id)
                            self.data['normalized_budgets'].extend(normalized)
                            self.validation_stats['normalized_records'] += len(normalized)

                        self.validation_stats['processed_tables'] += 1

            logger.info(f"✅ 정규화 완료: {len(self.data['sub_projects'])}개 내역사업")
            return True

        except Exception as e:
            logger.error(f"처리 실패: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_csv(self):
        """CSV 저장"""
        for table_name, records in self.data.items():
            if not records or table_name == 'data_statistics':
                continue

            csv_path = self.output_dir / f"{table_name}.csv"

            with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
                if records:
                    writer = csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)

            logger.info(f"✅ {table_name}.csv 저장 ({len(records)}건)")

    def print_statistics(self):
        """통계 출력"""
        print("\n" + "="*80)
        print("📊 정부 표준 정규화 완료")
        print("="*80)

        print(f"\n📁 내역사업: {len(self.data['sub_projects'])}개")
        for project in self.data['sub_projects']:
            print(f"  - {project['sub_project_name']} ({project['project_code']})")

        print(f"\n📋 데이터 통계:")
        print(f"  원본 데이터: {len(self.data['raw_data'])}건")
        print(f"  정규화 일정: {len(self.data['normalized_schedules'])}건")
        print(f"  정규화 성과: {len(self.data['normalized_performances'])}건")
        print(f"  정규화 예산: {len(self.data['normalized_budgets'])}건")
        print(f"  사업개요: {len(self.data['normalized_overviews'])}건")
        print(f"  대표성과: {len(self.data['key_achievements'])}건")
        print(f"  주요계획: {len(self.data['plan_details'])}건")

        print("="*80 + "\n")


if __name__ == "__main__":
    json_file = "output/2024년도 생명공학육성시행계획(안) 부록_내역사업_테스트.json"
    output_folder = "normalized_government"

    if Path(json_file).exists():
        normalizer = GovernmentStandardNormalizer(json_file, output_folder)

        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        success = normalizer.normalize(json_data)

        if success:
            normalizer.save_to_csv()
            normalizer.print_statistics()
        else:
            print("❌ 정규화 실패!")


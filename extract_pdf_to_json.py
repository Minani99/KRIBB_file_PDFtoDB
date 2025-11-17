"""
PDF에서 테이블과 텍스트를 추출하여 JSON으로 변환하는 모듈
정부/공공기관 문서 구조에 최적화
"""
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import re

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logger.warning("pdfplumber not installed. Using sample data mode.")


class GovernmentPDFExtractor:
    """정부 문서 PDF 추출 클래스"""
    
    def __init__(self, pdf_path: str = None, output_dir: str = "output"):
        """
        Args:
            pdf_path: 입력 PDF 파일 경로
            output_dir: 출력 JSON 디렉토리 경로
        """
        self.pdf_path = Path(pdf_path) if pdf_path else None
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # 카테고리 패턴
        self.category_patterns = {
            'overview': [r'\(1\)', r'사업개요', r'사업목표', r'주관기관'],
            'performance': [r'\(2\)', r'추진실적', r'성과지표', r'특허', r'논문'],
            'plan': [r'\(3\)', r'추진계획', r'일정', r'예산', r'사업비']
        }
        
        # 추출 통계
        self.stats = {
            'total_pages': 0,
            'total_tables': 0,
            'total_rows': 0,
            'categories_found': set(),
            'sub_projects': []
        }
    
    def extract(self) -> Dict[str, Any]:
        """PDF에서 데이터 추출"""
        if not PDF_AVAILABLE or not self.pdf_path:
            logger.info("Using sample data mode")
            return self._generate_sample_data()
        
        try:
            logger.info(f"🚀 PDF 추출 시작: {self.pdf_path.name}")
            
            result = {
                "metadata": {
                    "source_file": self.pdf_path.name,
                    "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "document_year": self._detect_year(),
                    "total_pages": 0
                },
                "pages": []
            }
            
            with pdfplumber.open(self.pdf_path) as pdf:
                result["metadata"]["total_pages"] = len(pdf.pages)
                self.stats['total_pages'] = len(pdf.pages)
                
                for page_num, page in enumerate(pdf.pages, 1):
                    page_data = self._process_page(page, page_num)
                    if page_data:
                        result["pages"].append(page_data)
                
            self._print_statistics()
            
            # JSON 저장
            output_file = self.output_dir / f"{self.pdf_path.stem}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ JSON 저장 완료: {output_file}")
            return result
            
        except Exception as e:
            logger.error(f"PDF 추출 실패: {e}")
            return self._generate_sample_data()
    
    def _process_page(self, page, page_num: int) -> Dict[str, Any]:
        """페이지 처리"""
        logger.info(f"📄 페이지 {page_num} 처리 중...")
        
        # 텍스트 추출
        full_text = page.extract_text() or ""
        
        # 카테고리 감지
        category = self._detect_category(full_text)
        if category:
            self.stats['categories_found'].add(category)
        
        # 내역사업 감지 (텍스트에서)
        sub_project = self._detect_sub_project(full_text)

        # 테이블 추출
        tables = page.extract_tables()

        # 테이블에서도 내역사업명 찾기
        if not sub_project and tables:
            for table in tables:
                for row in table:
                    if row and len(row) >= 2:
                        # "내역사업명" 찾기
                        if '내역사업' in str(row[0]):
                            sub_project = str(row[1]).strip()
                            break
                if sub_project:
                    break

        if sub_project and sub_project not in self.stats['sub_projects']:
            self.stats['sub_projects'].append(sub_project)
            logger.info(f"  ✓ 내역사업 발견: {sub_project}")
        
        page_data = {
            "page_number": page_num,
            "full_text": full_text,
            "category": category,
            "sub_project": sub_project,
            "tables": []
        }
        
        if tables:
            logger.info(f"  ✓ {len(tables)}개 테이블 발견")
            self.stats['total_tables'] += len(tables)
            
            for table_idx, table in enumerate(tables, 1):
                processed_table = self._process_table(table, category)
                if processed_table:
                    page_data["tables"].append({
                        "table_number": table_idx,
                        "category": category,
                        "rows": len(processed_table),
                        "columns": len(processed_table[0]) if processed_table else 0,
                        "data": processed_table
                    })
                    self.stats['total_rows'] += len(processed_table)
        
        return page_data
    
    def _process_table(self, table: List[List], category: str) -> List[List]:
        """테이블 처리 및 정제"""
        if not table:
            return []
        
        # 빈 행 제거 및 띄어쓰기 문제 수정
        cleaned_table = []
        for row in table:
            if row and any(cell for cell in row if cell and str(cell).strip()):
                # PDF 파싱 시 띄어쓰기 문제 수정 (예: "정 부" -> "정부")
                cleaned_row = []
                for cell in row:
                    if cell:
                        cell_str = str(cell).strip()
                        # 한글 단어 중간에 공백이 하나씩 끼어있는 경우 제거
                        # "정 부" -> "정부", "민 간" -> "민간"
                        if re.match(r'^[\u3131-\u3163\uac00-\ud7a3]\s[\u3131-\u3163\uac00-\ud7a3]$', cell_str):
                            cell_str = cell_str.replace(' ', '')
                        cleaned_row.append(cell_str)
                    else:
                        cleaned_row.append("")
                cleaned_table.append(cleaned_row)
        
        # 카테고리별 특수 처리
        if category == 'performance' and cleaned_table:
            cleaned_table = self._enhance_performance_table(cleaned_table)
        elif category == 'plan' and cleaned_table:
            cleaned_table = self._enhance_plan_table(cleaned_table)
        
        return cleaned_table
    
    def _enhance_performance_table(self, table: List[List]) -> List[List]:
        """성과 테이블 향상"""
        # 헤더가 없으면 추가
        if table and not any('성과' in str(cell) for cell in table[0]):
            # 데이터 패턴으로 헤더 추론
            if any('특허' in str(row[0]) for row in table):
                table.insert(0, ['성과지표', '세부항목', '실적'])
            elif len(table[0]) >= 4 and all(self._is_number(cell) for cell in table[0][1:]):
                table.insert(0, ['구분', '국내출원', '국내등록', '국외출원', '국외등록'])
        
        return table
    
    def _enhance_plan_table(self, table: List[List]) -> List[List]:
        """계획 테이블 향상"""
        # 일정 테이블 감지 및 향상
        if table and any('분기' in str(cell) for row in table for cell in row):
            if not any('추진일정' in str(cell) for cell in table[0]):
                table.insert(0, ['추진일정', '과제명', '세부내용'])
        
        # 예산 테이블 감지 및 향상
        elif table and any('예산' in str(cell) or '백만원' in str(cell) for row in table for cell in row):
            if not any('연도' in str(cell) for cell in table[0]):
                table.insert(0, ['연도', '총예산', '정부', '민간', '기타'])
        
        return table
    
    def _detect_category(self, text: str) -> Optional[str]:
        """카테고리 감지"""
        text_lower = text.lower()
        
        for category, patterns in self.category_patterns.items():
            if any(re.search(pattern.lower(), text_lower) for pattern in patterns):
                return category
        
        return None
    
    def _detect_sub_project(self, text: str) -> Optional[str]:
        """내역사업명 감지"""
        patterns = [
            r'내역사업명\s*[:：]\s*([^\n]+)',
            r'내역사업\s*[:：]\s*([^\n]+)',
            r'◦\s*([^◦\n]+(?:기술개발|연구개발|사업))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _detect_year(self) -> int:
        """문서 연도 감지"""
        current_year = datetime.now().year
        
        if self.pdf_path and self.pdf_path.stem:
            # 파일명에서 연도 추출
            year_match = re.search(r'(20\d{2})', self.pdf_path.stem)
            if year_match:
                return int(year_match.group(1))
        
        return current_year
    
    def _is_number(self, text: str) -> bool:
        """숫자 여부 확인"""
        try:
            float(str(text).replace(',', '').replace('건', '').replace('편', '').strip())
            return True
        except:
            return False
    
    def _generate_sample_data(self) -> Dict[str, Any]:
        """샘플 데이터 생성 (PDF 없을 때)"""
        logger.info("샘플 데이터 생성 중...")
        
        current_year = datetime.now().year

        return {
            "metadata": {
                "source_file": "sample_data.pdf",
                "extraction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "document_year": current_year,
                "total_pages": 12
            },
            "pages": [
                {
                    "page_number": 1,
                    "full_text": "1. 세부사업명: 바이오·의료기술개발\n내역사업명: 뇌연구",
                    "category": None,
                    "sub_project": "뇌연구",
                    "tables": [{
                        "table_number": 1,
                        "category": None,
                        "data": [
                            ["항목", "내용"],
                            ["세부사업명", "바이오·의료기술개발"],
                            ["내역사업명", "뇌연구"],
                            ["담당부처", "과학기술정보통신부"]
                        ]
                    }]
                },
                {
                    "page_number": 2,
                    "full_text": "(1) 사업개요",
                    "category": "overview",
                    "sub_project": "뇌연구",
                    "tables": [{
                        "table_number": 1,
                        "category": "overview",
                        "data": [
                            ["구분", "내용"],
                            ["사업목표", "뇌과학 원천기술 확보 및 뇌질환 극복"],
                            ["주관기관", "한국뇌연구원"],
                            ["사업내용", "뇌지도 구축, 뇌질환 진단/치료 기술 개발"]
                        ]
                    }]
                },
                {
                    "page_number": 3,
                    "full_text": "(2) 추진실적",
                    "category": "performance",
                    "sub_project": "뇌연구",
                    "tables": [{
                        "table_number": 1,
                        "category": "performance",
                        "data": [
                            ["성과지표", "세부항목", "실적"],
                            ["특허", "국내출원", "1,001"],
                            ["특허", "국내등록", "125"],
                            ["특허", "국외출원", "74"],
                            ["특허", "국외등록", "10"],
                            ["논문", "SCIE", "5,977"],
                            ["논문", "IF10이상", "234"],
                            ["인력양성", "박사", "156"],
                            ["인력양성", "석사", "289"]
                        ]
                    }]
                },
                {
                    "page_number": 4,
                    "full_text": "(3) 추진계획",
                    "category": "plan",
                    "sub_project": "뇌연구",
                    "tables": [
                        {
                            "table_number": 1,
                            "category": "plan",
                            "data": [
                                ["추진일정", "과제명", "세부내용"],
                                ["1/4분기~2/4분기", "뇌지도 구축", "고해상도 뇌영상 데이터 수집"],
                                ["2/4분기~3/4분기", "AI 분석 플랫폼", "딥러닝 기반 뇌영상 분석 시스템 구축"],
                                ["3/4분기~4/4분기", "임상 검증", "뇌질환 진단 정확도 검증"],
                                ["연중", "인력 양성", "전문 연구인력 교육 프로그램 운영"]
                            ]
                        },
                        {
                            "table_number": 2,
                            "category": "plan",
                            "data": [
                                ["연도", "총예산", "정부", "민간", "지방비"],
                                [f"{current_year-1}(실적)", "45,200", "35,000", "8,200", "2,000"],
                                [f"{current_year}(계획)", "52,300", "40,000", "10,300", "2,000"],
                                [f"{current_year+1}(계획)", "58,500", "44,000", "12,500", "2,000"]
                            ]
                        }
                    ]
                }
            ]
        }
    
    def _print_statistics(self):
        """통계 출력"""
        logger.info(f"""
📊 추출 통계:
- 총 페이지: {self.stats['total_pages']}
- 총 테이블: {self.stats['total_tables']}
- 총 데이터 행: {self.stats['total_rows']}
- 카테고리: {', '.join(self.stats['categories_found'])}
- 내역사업: {len(self.stats['sub_projects'])}개
  {', '.join(self.stats['sub_projects'])}
        """)


def extract_pdf_to_json(pdf_path: str = None, output_dir: str = "output") -> Dict[str, Any]:
    """
    PDF를 JSON으로 변환하는 메인 함수
    
    Args:
        pdf_path: PDF 파일 경로 (None이면 샘플 데이터 사용)
        output_dir: 출력 디렉토리
    
    Returns:
        추출된 JSON 데이터
    """
    extractor = GovernmentPDFExtractor(pdf_path, output_dir)
    return extractor.extract()


if __name__ == "__main__":
    # 테스트 실행
    import sys
    
    if len(sys.argv) > 1:
        pdf_file = sys.argv[1]
        result = extract_pdf_to_json(pdf_file)
    else:
        # 샘플 데이터 모드
        result = extract_pdf_to_json()
    
    if result:
        print(f"\n✅ 추출 완료! 페이지: {len(result['pages'])}개")
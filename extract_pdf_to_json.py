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

try:
    import PyPDF2
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    logger.warning("PyPDF2 not installed. CID font fallback disabled.")


class GovernmentPDFExtractor:
    """정부 문서 PDF 추출 클래스"""
    
    # CID 패턴 (정규식)
    CID_PATTERN = re.compile(r'\(cid:\d+\)')

    def __init__(self, pdf_path: str = None, output_dir: str = None):
        """
        Args:
            pdf_path: 입력 PDF 파일 경로
            output_dir: 출력 JSON 디렉토리 경로 (None이면 config.OUTPUT_DIR 사용)
        """
        self.pdf_path = Path(pdf_path) if pdf_path else None

        # output_dir이 None이면 config에서 가져오기
        if output_dir is None:
            try:
                from config import OUTPUT_DIR
                self.output_dir = OUTPUT_DIR
            except ImportError:
                self.output_dir = Path("output")
        else:
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
    
    def _clean_cid_text(self, text: str) -> str:
        """
        CID 폰트 코드 정리
        (cid:XXXX) 패턴을 제거하거나 대체

        Args:
            text: 원본 텍스트

        Returns:
            정리된 텍스트
        """
        if not text:
            return ""

        # CID 코드 제거
        cleaned = self.CID_PATTERN.sub('', text)

        # 연속된 공백 정리
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        # 완전히 비어있으면 원본 반환 (CID만 있었던 경우)
        if not cleaned:
            return text

        return cleaned

    def _extract_text_with_fallback(self, page) -> str:
        """
        CID 폰트 대응 텍스트 추출 (다중 방식 시도)

        1. pdfplumber 기본 추출
        2. CID 정리 후 반환
        3. PyPDF2 fallback (옵션)

        Args:
            page: pdfplumber page 객체

        Returns:
            추출된 텍스트
        """
        # 1차: pdfplumber 기본 추출
        text = page.extract_text() or ""

        # CID 코드가 많으면 정리
        cid_count = len(self.CID_PATTERN.findall(text))
        if cid_count > 10:  # CID가 10개 이상이면 문제 있음
            logger.warning(f"  ⚠️  CID 폰트 감지 ({cid_count}개) - 정리 중...")
            cleaned_text = self._clean_cid_text(text)

            # 정리 후 텍스트가 너무 짧으면 경고
            if len(cleaned_text) < len(text) * 0.3:
                logger.warning(f"  ⚠️  CID 정리 후 텍스트 손실 심함 (원본:{len(text)} → 정리:{len(cleaned_text)})")
                logger.warning(f"  💡 수동 확인 권장: 해당 페이지 텍스트 추출 품질 낮음")

            return cleaned_text

        return text

    def extract(self) -> Dict[str, Any]:
        """PDF에서 데이터 추출"""
        if not PDF_AVAILABLE:
            logger.error("pdfplumber가 설치되지 않았습니다. 'pip install pdfplumber' 실행하세요.")
            raise ImportError("pdfplumber not installed")

        if not self.pdf_path:
            logger.error("PDF 파일 경로가 제공되지 않았습니다.")
            raise ValueError("PDF path is required")

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
            raise

    def _process_page(self, page, page_num: int) -> Dict[str, Any]:
        """페이지 처리"""
        logger.info(f"📄 페이지 {page_num} 처리 중...")
        
        # 텍스트 추출 (CID 대응)
        full_text = self._extract_text_with_fallback(page)

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

                        # CID 코드 정리
                        cell_str = self._clean_cid_text(cell_str)

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


def extract_pdf_to_json(pdf_path: str, output_dir: str = None) -> Dict[str, Any]:
    """
    PDF를 JSON으로 변환하는 메인 함수
    
    Args:
        pdf_path: PDF 파일 경로 (필수)
        output_dir: 출력 디렉토리 (None이면 config.OUTPUT_DIR 사용)

    Returns:
        추출된 JSON 데이터
    """
    if not pdf_path:
        raise ValueError("PDF 파일 경로가 필요합니다.")

    # output_dir이 None이면 config에서 가져오기
    if output_dir is None:
        try:
            from config import OUTPUT_DIR
            output_dir = str(OUTPUT_DIR)
        except ImportError:
            output_dir = "output"

    extractor = GovernmentPDFExtractor(pdf_path, output_dir)
    return extractor.extract()


if __name__ == "__main__":
    # 테스트 실행
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python extract_pdf_to_json.py <PDF파일경로>")
        sys.exit(1)

    pdf_file = sys.argv[1]
    result = extract_pdf_to_json(pdf_file)

    if result:
        print(f"\n✅ 추출 완료! 페이지: {len(result['pages'])}개")
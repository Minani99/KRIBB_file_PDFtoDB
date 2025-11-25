#!/usr/bin/env python3
"""
배치 PDF 처리 모듈
"""
from pathlib import Path
from typing import Callable, List, Dict, Any
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

logger = logging.getLogger(__name__)


class BatchPDFProcessor:
    """배치 PDF 처리 클래스"""

    def __init__(self, input_dir: str, output_dir: str,
                 batch_size: int = 10, max_workers: int = 5,
                 use_multiprocessing: bool = False):
        """
        Args:
            input_dir: 입력 PDF 디렉토리
            output_dir: 출력 디렉토리
            batch_size: 배치당 파일 수
            max_workers: 병렬 작업자 수
            use_multiprocessing: 멀티프로세싱 사용 여부
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.use_multiprocessing = use_multiprocessing

        self.summary = {
            'total': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0
        }

    def process_all(self, processor_func: Callable,
                   recursive: bool = False,
                   save_results: bool = True) -> Dict[str, Any]:
        """
        모든 PDF 파일 배치 처리

        Args:
            processor_func: PDF 처리 함수 (pdf_path를 받아 bool 반환)
            recursive: 하위 디렉토리 포함 여부
            save_results: 결과 저장 여부

        Returns:
            처리 결과 요약
        """
        # PDF 파일 찾기
        if recursive:
            pdf_files = list(self.input_dir.rglob("*.pdf"))
        else:
            pdf_files = list(self.input_dir.glob("*.pdf"))

        if not pdf_files:
            logger.warning(f"PDF 파일이 없습니다: {self.input_dir}")
            return self.summary

        self.summary['total'] = len(pdf_files)
        logger.info(f"📄 총 {len(pdf_files)}개 PDF 파일 발견")

        # 병렬 처리
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(processor_func, str(pdf_file)): pdf_file
                for pdf_file in pdf_files
            }

            # 진행 상황 표시
            with tqdm(total=len(pdf_files), desc="PDF 처리 중") as pbar:
                for future in as_completed(futures):
                    pdf_file = futures[future]
                    try:
                        result = future.result()
                        if result:
                            self.summary['processed'] += 1
                        else:
                            self.summary['failed'] += 1
                            logger.error(f"처리 실패: {pdf_file.name}")
                    except Exception as e:
                        self.summary['failed'] += 1
                        logger.error(f"에러 ({pdf_file.name}): {e}")

                    pbar.update(1)

        return self.summary

    def print_summary(self):
        """처리 결과 요약 출력"""
        logger.info("\n" + "="*80)
        logger.info("📊 배치 처리 결과")
        logger.info("="*80)
        logger.info(f"총 파일: {self.summary['total']}개")
        logger.info(f"성공: {self.summary['processed']}개")
        logger.info(f"실패: {self.summary['failed']}개")
        logger.info(f"건너뜀: {self.summary['skipped']}개")
        logger.info("="*80)


def create_pdf_processor_func(output_dir: str) -> Callable:
    """PDF 처리 함수 생성"""
    def processor(pdf_path: str) -> bool:
        """단일 PDF 처리"""
        try:
            from extract_pdf_to_json import extract_pdf_to_json
            result = extract_pdf_to_json(pdf_path, output_dir)
            return result is not None
        except Exception as e:
            logger.error(f"PDF 처리 에러 ({Path(pdf_path).name}): {e}")
            return False

    return processor

#!/usr/bin/env python3
"""
대량 PDF 배치 처리 모듈
메모리 효율적인 병렬 처리 지원
"""

import gc
import logging
from pathlib import Path
from typing import List, Dict, Any, Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
import json

# 프로그레스 바
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    tqdm = None

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchPDFProcessor:
    """대량 PDF 배치 처리 클래스"""

    def __init__(
        self,
        input_dir: str = None,
        output_dir: str = None,
        batch_size: int = None,
        max_workers: int = None,
        use_multiprocessing: bool = None
    ):
        """
        Args:
            input_dir: 입력 PDF 디렉토리 (None이면 config 사용)
            output_dir: 출력 디렉토리 (None이면 config 사용)
            batch_size: 배치당 처리할 파일 수 (None이면 config 사용)
            max_workers: 병렬 작업자 수 (None이면 config 사용)
            use_multiprocessing: True=프로세스, False=쓰레드 (None이면 config 사용)
        """
        # config에서 기본값 가져오기
        try:
            from config import INPUT_DIR, OUTPUT_DIR, BATCH_SIZE, MAX_WORKERS, USE_MULTIPROCESSING
            self.input_dir = Path(input_dir) if input_dir else INPUT_DIR
            self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR
            self.batch_size = batch_size if batch_size is not None else BATCH_SIZE
            self.max_workers = max_workers if max_workers is not None else MAX_WORKERS
            self.use_multiprocessing = use_multiprocessing if use_multiprocessing is not None else USE_MULTIPROCESSING
        except ImportError:
            # config 없으면 기본값 사용
            self.input_dir = Path(input_dir) if input_dir else Path("input")
            self.output_dir = Path(output_dir) if output_dir else Path("output")
            self.batch_size = batch_size if batch_size is not None else 10
            self.max_workers = max_workers if max_workers is not None else 4
            self.use_multiprocessing = use_multiprocessing if use_multiprocessing is not None else False

        # 통계
        self.stats = {
            'total_files': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,  # type: Optional[datetime]
            'end_time': None,  # type: Optional[datetime]
            'errors': []
        }

    def get_pdf_files(self, recursive: bool = False) -> List[Path]:
        """PDF 파일 목록 가져오기"""
        if recursive:
            pdf_files = list(self.input_dir.rglob("*.pdf"))
        else:
            pdf_files = list(self.input_dir.glob("*.pdf"))

        return sorted(pdf_files)

    def process_single_pdf(
        self,
        pdf_path: Path,
        processor_func: Callable,
        **kwargs
    ) -> Dict[str, Any]:
        """
        단일 PDF 처리 (별도 프로세스/쓰레드에서 실행)

        Args:
            pdf_path: PDF 파일 경로
            processor_func: 처리 함수
            **kwargs: 처리 함수에 전달할 추가 인자

        Returns:
            처리 결과 딕셔너리
        """
        result = {
            'file': pdf_path.name,
            'path': str(pdf_path),
            'status': 'processing',
            'error': None,
            'data': None
        }

        try:
            # 처리 함수 실행
            result['data'] = processor_func(pdf_path, **kwargs)
            result['status'] = 'success'

        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"처리 실패 {pdf_path.name}: {e}")

        finally:
            # 메모리 정리
            gc.collect()

        return result

    def process_batch(
        self,
        pdf_files: List[Path],
        processor_func: Callable,
        show_progress: bool = True,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        배치 처리

        Args:
            pdf_files: 처리할 PDF 파일 목록
            processor_func: 각 PDF를 처리할 함수
            show_progress: 진행률 표시 여부
            **kwargs: processor_func에 전달할 추가 인자

        Returns:
            처리 결과 리스트
        """
        results = []
        total = len(pdf_files)

        # Executor 선택
        ExecutorClass = ProcessPoolExecutor if self.use_multiprocessing else ThreadPoolExecutor

        # 진행률 표시 준비
        if show_progress and TQDM_AVAILABLE:
            progress = tqdm(total=total, desc="PDF 처리 중")
        else:
            progress = None

        try:
            with ExecutorClass(max_workers=self.max_workers) as executor:
                # 작업 제출
                futures = {
                    executor.submit(
                        self.process_single_pdf,
                        pdf_path,
                        processor_func,
                        **kwargs
                    ): pdf_path
                    for pdf_path in pdf_files
                }

                # 결과 수집
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)

                    # 통계 업데이트
                    if result['status'] == 'success':
                        self.stats['processed'] += 1
                    elif result['status'] == 'failed':
                        self.stats['failed'] += 1
                        self.stats['errors'].append({
                            'file': result['file'],
                            'error': result['error']
                        })

                    # 진행률 업데이트
                    if progress:
                        progress.update(1)
                        progress.set_postfix({
                            'success': self.stats['processed'],
                            'failed': self.stats['failed']
                        })

        finally:
            if progress:
                progress.close()

            # 배치 간 메모리 정리
            gc.collect()

        return results

    def process_all(
        self,
        processor_func: Callable,
        recursive: bool = False,
        save_results: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        모든 PDF 파일 처리

        Args:
            processor_func: PDF 처리 함수
            recursive: 하위 폴더 포함 여부
            save_results: 결과 저장 여부
            **kwargs: processor_func에 전달할 추가 인자

        Returns:
            전체 처리 결과
        """
        self.stats['start_time'] = datetime.now()

        # PDF 파일 찾기
        pdf_files = self.get_pdf_files(recursive)
        self.stats['total_files'] = len(pdf_files)

        if not pdf_files:
            logger.warning(f"PDF 파일을 찾을 수 없습니다: {self.input_dir}")
            return self.get_summary()

        logger.info(f"총 {len(pdf_files)}개 PDF 파일 발견")
        logger.info(f"배치 크기: {self.batch_size}, 작업자: {self.max_workers}")

        all_results = []

        # 배치 단위 처리
        for i in range(0, len(pdf_files), self.batch_size):
            batch = pdf_files[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (len(pdf_files) + self.batch_size - 1) // self.batch_size

            logger.info(f"\n배치 {batch_num}/{total_batches} 처리 중 ({len(batch)}개 파일)")

            # 배치 처리
            batch_results = self.process_batch(
                batch,
                processor_func,
                show_progress=True,
                **kwargs
            )

            all_results.extend(batch_results)

            # 중간 저장 (선택)
            if save_results and batch_num % 5 == 0:
                self._save_intermediate_results(all_results, batch_num)

        self.stats['end_time'] = datetime.now()

        # 최종 결과 저장
        if save_results:
            self._save_final_results(all_results)

        return self.get_summary(all_results)

    def _save_intermediate_results(self, results: List[Dict], batch_num: int):
        """중간 결과 저장"""
        try:
            output_file = self.output_dir / f"batch_results_{batch_num:04d}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"중간 결과 저장: {output_file}")
        except Exception as e:
            logger.error(f"중간 결과 저장 실패: {e}")

    def _save_final_results(self, results: List[Dict]):
        """최종 결과 저장"""
        try:
            # 전체 결과
            output_file = self.output_dir / f"batch_results_final_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, default=str)

            # 요약 리포트
            summary = self.get_summary(results)
            report_file = self.output_dir / f"batch_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"최종 결과 저장: {output_file}")
            logger.info(f"요약 리포트: {report_file}")

        except Exception as e:
            logger.error(f"최종 결과 저장 실패: {e}")

    def get_summary(self, results: List[Dict] = None) -> Dict[str, Any]:
        """처리 요약 정보"""
        duration = None
        if self.stats['start_time'] and self.stats['end_time']:
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        summary = {
            'total_files': self.stats['total_files'],
            'processed': self.stats['processed'],
            'failed': self.stats['failed'],
            'skipped': self.stats['skipped'],
            'success_rate': (
                f"{(self.stats['processed'] / self.stats['total_files'] * 100):.1f}%"
                if self.stats['total_files'] > 0 else "0%"
            ),
            'duration_seconds': duration,
            'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
            'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None,
            'errors': self.stats['errors'][:10],  # 최대 10개만
            'total_errors': len(self.stats['errors'])
        }

        if results:
            summary['results'] = results

        return summary

    def print_summary(self):
        """요약 정보 출력"""
        summary = self.get_summary()

        print("\n" + "="*80)
        print("📊 배치 처리 요약")
        print("="*80)
        print(f"총 파일:     {summary['total_files']:,}개")
        print(f"처리 성공:   {summary['processed']:,}개")
        print(f"처리 실패:   {summary['failed']:,}개")
        print(f"성공률:      {summary['success_rate']}")

        if summary['duration_seconds']:
            print(f"소요 시간:   {summary['duration_seconds']:.1f}초")
            if summary['processed'] > 0:
                avg_time = summary['duration_seconds'] / summary['processed']
                print(f"평균 처리:   {avg_time:.2f}초/파일")

        if summary['total_errors'] > 0:
            print(f"\n⚠️ 오류 발생: {summary['total_errors']}건")
            print("최근 오류:")
            for error in summary['errors'][:5]:
                print(f"  - {error['file']}: {error['error'][:60]}...")

        print("="*80 + "\n")


# 헬퍼 함수: 기존 파이프라인과 통합
def _pdf_processor_worker(pdf_path: Path, output_dir: str) -> Dict[str, Any]:
    """PDF → JSON 변환 (최상위 레벨 함수 - 멀티프로세싱용)"""
    from extract_pdf_to_json import extract_pdf_to_json

    try:
        json_data = extract_pdf_to_json(str(pdf_path), output_dir)
        return {
            'status': 'success',
            'json_file': str(Path(output_dir) / f"{pdf_path.stem}.json"),
            'data': json_data
        }
    except Exception as e:
        raise Exception(f"PDF 변환 실패: {e}")


def create_pdf_processor_func(output_dir: str):
    """PDF 처리 함수 팩토리"""
    from functools import partial
    return partial(_pdf_processor_worker, output_dir=output_dir)


if __name__ == "__main__":
    """테스트 실행"""
    import argparse

    parser = argparse.ArgumentParser(description="대량 PDF 배치 처리")
    parser.add_argument('--input', default='input', help='입력 디렉토리')
    parser.add_argument('--output', default='output', help='출력 디렉토리')
    parser.add_argument('--batch-size', type=int, default=10, help='배치 크기')
    parser.add_argument('--workers', type=int, default=4, help='병렬 작업자 수')
    parser.add_argument('--recursive', action='store_true', help='하위 폴더 포함')

    args = parser.parse_args()

    # 배치 프로세서 생성
    processor = BatchPDFProcessor(
        input_dir=args.input,
        output_dir=args.output,
        batch_size=args.batch_size,
        max_workers=args.workers
    )

    # 처리 함수 생성
    pdf_processor = create_pdf_processor_func(args.output)

    # 실행
    summary = processor.process_all(
        pdf_processor,
        recursive=args.recursive
    )

    # 결과 출력
    processor.print_summary()


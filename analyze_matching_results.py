"""
매칭 결과 상세 분석 스크립트
- 전체 매칭률 통계
- 연도별 매칭률
- 매칭 실패 원인 분석
- 상세 리포트 생성
"""

import sys
import os
import json
import logging
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List, Tuple
import csv

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('matching_analysis.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 경로 설정
PROJECT_ROOT = Path(__file__).parent
OUTPUT_DIR = PROJECT_ROOT / "output"
NORMALIZED_DIR = PROJECT_ROOT / "normalized_output_government"
REPORT_DIR = NORMALIZED_DIR / "matching_reports"
REPORT_DIR.mkdir(exist_ok=True)


class MatchingAnalyzer:
    """매칭 결과 분석기"""

    def __init__(self):
        self.json_data = []
        self.csv_data = []
        self.stats = {
            'total': 0,
            'matched': 0,
            'unmatched': 0,
            'match_rate': 0.0,
            'by_year': {},
            'by_ministry': {},
            'unmatched_details': []
        }

    def load_json_files(self):
        """JSON 파일 로드"""
        logger.info("📂 JSON 파일 로딩 중...")

        json_files = sorted(OUTPUT_DIR.glob("*.json"))

        for json_file in json_files:
            logger.info(f"  - {json_file.name}")
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            year = json_file.stem.split('년도')[0]

            for item in data:
                item['source_year'] = year
                item['source_file'] = json_file.name
                self.json_data.append(item)

        logger.info(f"✅ JSON 로딩 완료: {len(self.json_data)}건")

    def load_csv_file(self):
        """정규화된 CSV 파일 로드"""
        logger.info("📂 정규화 CSV 로딩 중...")

        csv_file = NORMALIZED_DIR / "TB_PLAN_DATA.csv"

        if not csv_file.exists():
            logger.error(f"❌ CSV 파일 없음: {csv_file}")
            return False

        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            self.csv_data = list(reader)

        logger.info(f"✅ CSV 로딩 완료: {len(self.csv_data)}건")
        return True

    def analyze(self):
        """매칭 결과 분석"""
        logger.info("\n" + "="*80)
        logger.info("🔍 매칭 결과 분석 시작")
        logger.info("="*80)

        total = len(self.csv_data)
        matched = 0
        unmatched = 0

        by_year = defaultdict(lambda: {'total': 0, 'matched': 0, 'unmatched': 0})
        by_ministry = defaultdict(lambda: {'total': 0, 'matched': 0, 'unmatched': 0})
        unmatched_details = []

        for row in self.csv_data:
            year = row.get('YEAR', 'UNKNOWN')
            ministry = row.get('MINISTRY_NM', 'UNKNOWN')
            plan_id = row.get('PLAN_ID', '')
            biz_nm = row.get('BIZ_NM', '')
            detail_biz_nm = row.get('DETAIL_BIZ_NM', '')

            # 전체 통계
            by_year[year]['total'] += 1
            by_ministry[ministry]['total'] += 1

            # 매칭 여부 판단
            if plan_id.startswith('TEMP_'):
                unmatched += 1
                by_year[year]['unmatched'] += 1
                by_ministry[ministry]['unmatched'] += 1

                # 매칭 실패 상세 정보
                unmatched_details.append({
                    'YEAR': year,
                    'MINISTRY': ministry,
                    'BIZ_NM': biz_nm,
                    'DETAIL_BIZ_NM': detail_biz_nm,
                    'TEMP_ID': plan_id,
                    'REASON': '정규화 후 DB에서 (BIZ_NM + DETAIL_BIZ_NM) 조합 매칭 실패'
                })
            else:
                matched += 1
                by_year[year]['matched'] += 1
                by_ministry[ministry]['matched'] += 1

        # 통계 저장
        self.stats['total'] = total
        self.stats['matched'] = matched
        self.stats['unmatched'] = unmatched
        self.stats['match_rate'] = (matched / total * 100) if total > 0 else 0
        self.stats['by_year'] = dict(by_year)
        self.stats['by_ministry'] = dict(by_ministry)
        self.stats['unmatched_details'] = unmatched_details

        return True

    def print_summary(self):
        """요약 통계 출력"""
        logger.info("\n" + "="*80)
        logger.info("📊 매칭 결과 요약")
        logger.info("="*80)

        logger.info(f"\n총 레코드: {self.stats['total']:,}건")
        logger.info(f"  ✅ DB 매칭 성공: {self.stats['matched']:,}건 ({self.stats['match_rate']:.1f}%)")
        logger.info(f"  ❌ 매칭 실패 (TEMP): {self.stats['unmatched']:,}건 ({100 - self.stats['match_rate']:.1f}%)")

        # 연도별
        logger.info("\n📅 연도별 매칭률:")
        for year in sorted(self.stats['by_year'].keys()):
            stats = self.stats['by_year'][year]
            rate = (stats['matched'] / stats['total'] * 100) if stats['total'] > 0 else 0
            logger.info(f"  {year}년: {stats['matched']}/{stats['total']}건 ({rate:.1f}%)")

        # 부처별 (상위 10개)
        logger.info("\n🏛️  부처별 매칭률 (상위 10개):")
        ministry_sorted = sorted(
            self.stats['by_ministry'].items(),
            key=lambda x: x[1]['total'],
            reverse=True
        )[:10]

        for ministry, stats in ministry_sorted:
            rate = (stats['matched'] / stats['total'] * 100) if stats['total'] > 0 else 0
            logger.info(f"  {ministry[:30]:30s}: {stats['matched']:3d}/{stats['total']:3d}건 ({rate:5.1f}%)")

    def save_detailed_report(self):
        """상세 리포트 저장"""
        logger.info("\n📝 상세 리포트 생성 중...")

        # 1. 매칭 실패 목록
        unmatched_file = REPORT_DIR / "matching_failed_details.csv"
        with open(unmatched_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'YEAR', 'MINISTRY', 'BIZ_NM', 'DETAIL_BIZ_NM', 'TEMP_ID', 'REASON'
            ])
            writer.writeheader()
            writer.writerows(self.stats['unmatched_details'])

        logger.info(f"  ✅ 매칭 실패 목록: {unmatched_file}")

        # 2. 연도별 통계
        year_stats_file = REPORT_DIR / "matching_stats_by_year.csv"
        with open(year_stats_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['연도', '총 건수', '매칭 성공', '매칭 실패', '매칭률(%)'])

            for year in sorted(self.stats['by_year'].keys()):
                stats = self.stats['by_year'][year]
                rate = (stats['matched'] / stats['total'] * 100) if stats['total'] > 0 else 0
                writer.writerow([
                    year,
                    stats['total'],
                    stats['matched'],
                    stats['unmatched'],
                    f"{rate:.1f}"
                ])

        logger.info(f"  ✅ 연도별 통계: {year_stats_file}")

        # 3. 부처별 통계
        ministry_stats_file = REPORT_DIR / "matching_stats_by_ministry.csv"
        with open(ministry_stats_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['부처명', '총 건수', '매칭 성공', '매칭 실패', '매칭률(%)'])

            ministry_sorted = sorted(
                self.stats['by_ministry'].items(),
                key=lambda x: x[1]['total'],
                reverse=True
            )

            for ministry, stats in ministry_sorted:
                rate = (stats['matched'] / stats['total'] * 100) if stats['total'] > 0 else 0
                writer.writerow([
                    ministry,
                    stats['total'],
                    stats['matched'],
                    stats['unmatched'],
                    f"{rate:.1f}"
                ])

        logger.info(f"  ✅ 부처별 통계: {ministry_stats_file}")

        # 4. JSON 요약 리포트
        summary_file = REPORT_DIR / "matching_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': str(Path(summary_file).stat().st_mtime if summary_file.exists() else ''),
                'summary': {
                    'total': self.stats['total'],
                    'matched': self.stats['matched'],
                    'unmatched': self.stats['unmatched'],
                    'match_rate': round(self.stats['match_rate'], 2)
                },
                'by_year': {
                    year: {
                        'total': stats['total'],
                        'matched': stats['matched'],
                        'unmatched': stats['unmatched'],
                        'match_rate': round((stats['matched'] / stats['total'] * 100) if stats['total'] > 0 else 0, 2)
                    }
                    for year, stats in self.stats['by_year'].items()
                }
            }, f, ensure_ascii=False, indent=2)

        logger.info(f"  ✅ JSON 요약: {summary_file}")


def main():
    """메인 실행"""
    try:
        analyzer = MatchingAnalyzer()

        # CSV 로드
        if not analyzer.load_csv_file():
            logger.error("❌ CSV 파일이 없습니다. 먼저 정규화를 실행하세요.")
            return

        # 분석
        analyzer.analyze()

        # 요약 출력
        analyzer.print_summary()

        # 상세 리포트 저장
        analyzer.save_detailed_report()

        logger.info("\n" + "="*80)
        logger.info("✅ 매칭 분석 완료!")
        logger.info("="*80)
        logger.info(f"\n📁 리포트 위치: {REPORT_DIR}")

    except Exception as e:
        logger.error(f"❌ 오류 발생: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())


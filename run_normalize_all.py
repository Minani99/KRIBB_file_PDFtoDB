#!/usr/bin/env python3
"""메인 파이프라인 - 전체 JSON 파일 정규화 (DB 적재 없음)"""
import sys
from pathlib import Path
from oracle_db_manager import OracleDBManager
from normalize_government_standard import GovernmentStandardNormalizer
from config import ORACLE_CONFIG
import json

# 모든 JSON 파일 찾기
output_dir = Path("output")
json_files = sorted(output_dir.glob("*년도*.json"))

if not json_files:
    print(f"[ERROR] JSON 파일이 없습니다: {output_dir}")
    sys.exit(1)

print(f"[START] 전체 정규화 시작")
print(f"[INFO] 처리할 파일: {len(json_files)}개")
for f in json_files:
    print(f"  - {f.name}")

# DB 연결 (매칭용, 적재는 안함)
db = OracleDBManager(ORACLE_CONFIG)
db.connect()
print(f"\n[OK] DB 연결 완료 (읽기 전용)")

# 전체 데이터를 누적할 통합 Normalizer 생성
combined_normalizer = GovernmentStandardNormalizer(
    "통합",
    "normalized_output_government",
    db_manager=db
)

# 전체 통계
total_plan_data = 0
total_matched = 0
total_temp = 0
total_budgets = 0
total_schedules = 0
total_performances = 0

# 각 파일 처리
for idx, json_file in enumerate(json_files, 1):
    print(f"\n{'='*80}")
    print(f"[{idx}/{len(json_files)}] 처리 중: {json_file.name}")
    print(f"{'='*80}")

    # 각 파일용 임시 Normalizer 생성
    temp_normalizer = GovernmentStandardNormalizer(
        str(json_file),
        "normalized_output_government",
        db_manager=db
    )

    # JSON 로드
    with open(json_file, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    # 정규화 실행
    success = temp_normalizer.normalize(json_data)

    if success:
        # 통계 집계
        plan_count = len(temp_normalizer.data['plan_data'])
        matched = sum(1 for r in temp_normalizer.data['plan_data'] if not r.get('PLAN_ID', '').startswith('TEMP_'))
        temp = plan_count - matched

        total_plan_data += plan_count
        total_matched += matched
        total_temp += temp
        total_budgets += len(temp_normalizer.data['budgets'])
        total_schedules += len(temp_normalizer.data['schedules'])
        total_performances += len(temp_normalizer.data['performances'])

        print(f"[OK] 정규화 완료!")
        print(f"  내역사업: {plan_count}건 (매칭: {matched}, 임시: {temp})")
        print(f"  예산: {len(temp_normalizer.data['budgets'])}건")
        print(f"  일정: {len(temp_normalizer.data['schedules'])}건")
        print(f"  성과: {len(temp_normalizer.data['performances'])}건")

        # 통합 Normalizer에 데이터 누적
        combined_normalizer.data['plan_data'].extend(temp_normalizer.data['plan_data'])
        combined_normalizer.data['budgets'].extend(temp_normalizer.data['budgets'])
        combined_normalizer.data['schedules'].extend(temp_normalizer.data['schedules'])
        combined_normalizer.data['performances'].extend(temp_normalizer.data['performances'])
        combined_normalizer.data['raw_data'].extend(temp_normalizer.data['raw_data'])

        print(f"[MERGE] 데이터 통합 완료")
    else:
        print(f"[ERROR] 정규화 실패: {json_file.name}")

# 최종 통계
print(f"\n{'='*80}")
print(f"[RESULT] 전체 정규화 완료")
print(f"{'='*80}")
print(f"처리한 파일: {len(json_files)}개")
print(f"\n[통계]")
print(f"  내역사업: {total_plan_data}건")
print(f"    - 매칭 성공: {total_matched}건 ({total_matched/total_plan_data*100:.1f}%)")
print(f"    - 임시 ID: {total_temp}건 ({total_temp/total_plan_data*100:.1f}%)")
print(f"  예산: {total_budgets}건")
print(f"  일정: {total_schedules}건")
print(f"  성과: {total_performances}건")

# 통합된 CSV 저장
print(f"\n[SAVE] 통합 CSV 저장 중...")
combined_normalizer.save_to_csv()
print(f"[OK] CSV 저장 완료")

print(f"\n[INFO] CSV 파일 위치: normalized_output_government/")
print(f"[INFO] DB 적재 없음 (테스트 완료)")

db.close()
print(f"\n[DONE] 완료!")


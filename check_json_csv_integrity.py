"""
JSON과 CSV 데이터를 비교하여 정규화 과정에서 정보 누락이 있는지 체크하는 스크립트
"""
import json
import pandas as pd
from pathlib import Path
from collections import defaultdict

def load_json_data(json_path):
    """JSON 파일 로드"""
    print(f"JSON 파일 로딩 중: {json_path}")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def load_csv_data(csv_dir):
    """모든 CSV 파일 로드"""
    print(f"\nCSV 파일 로딩 중: {csv_dir}")
    csv_data = {}
    csv_files = [
        'normalized_overviews.csv',
        'sub_projects.csv',
        'normalized_budgets.csv',
        'normalized_schedules.csv',
        'normalized_performances.csv',
        'raw_data.csv'
    ]

    for csv_file in csv_files:
        csv_path = Path(csv_dir) / csv_file
        if csv_path.exists():
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            csv_data[csv_file] = df
            print(f"  - {csv_file}: {len(df)} rows")
        else:
            print(f"  - {csv_file}: 파일 없음")

    return csv_data

def analyze_json_structure(data):
    """JSON 데이터 구조 분석"""
    print("\n=== JSON 데이터 구조 분석 ===")

    # JSON 구조 파악
    print(f"JSON 최상위 키: {list(data.keys())}")

    stats = {
        'total_pages': 0,
        'total_tables': 0,
        'total_rows': 0,
        'pages_with_tables': 0
    }

    if 'pages' in data:
        pages = data['pages']
        stats['total_pages'] = len(pages)

        for page in pages:
            if 'tables' in page and page['tables']:
                stats['pages_with_tables'] += 1
                tables = page['tables']
                stats['total_tables'] += len(tables)

                for table in tables:
                    if 'data' in table and isinstance(table['data'], list):
                        stats['total_rows'] += len(table['data'])

    for key, value in stats.items():
        print(f"{key}: {value}")

    return stats

def analyze_csv_structure(csv_data):
    """CSV 데이터 구조 분석"""
    print("\n=== CSV 데이터 구조 분석 ===")

    stats = {}
    for csv_name, df in csv_data.items():
        stats[csv_name] = {
            'total_rows': len(df),
            'columns': list(df.columns)
        }
        print(f"\n{csv_name}:")
        print(f"  - 행 수: {len(df)}")
        print(f"  - 컬럼: {', '.join(df.columns)}")

    return stats

def compare_project_counts(json_data, csv_data):
    """데이터 수 비교"""
    print("\n=== 데이터 수 비교 ===")

    # JSON에서 총 테이블 행 수 계산
    json_row_count = 0
    if 'pages' in json_data:
        for page in json_data['pages']:
            if 'tables' in page:
                for table in page['tables']:
                    if 'data' in table and isinstance(table['data'], list):
                        json_row_count += len(table['data'])

    # CSV에서 데이터 수 확인
    csv_counts = {}
    for csv_name, df in csv_data.items():
        csv_counts[csv_name] = len(df)

    print(f"JSON 총 테이블 행 수: {json_row_count}")
    print("\nCSV 파일별 행 수:")
    for key, count in csv_counts.items():
        print(f"  {key}: {count}")

    return json_row_count, csv_counts

def check_budget_data_integrity(json_data, csv_data):
    """예산 데이터 무결성 체크"""
    print("\n=== 예산 데이터 무결성 체크 ===")

    issues = []

    if 'normalized_budgets.csv' not in csv_data:
        print("❌ normalized_budgets.csv 파일이 없습니다.")
        return issues

    budget_df = csv_data['normalized_budgets.csv']
    raw_df = csv_data.get('raw_data.csv')

    # raw_data에서 예산 관련 행 수 확인
    if raw_df is not None:
        budget_raw_count = len(raw_df[raw_df['data_type'] == 'budget'])
        print(f"raw_data의 예산 행 수: {budget_raw_count}")

    print(f"normalized_budgets 행 수: {len(budget_df)}")

    # 실제 금액 값이 있는지 체크
    if 'amount' in budget_df.columns:
        non_null_amounts = budget_df['amount'].notna().sum()
        print(f"금액이 있는 행: {non_null_amounts}/{len(budget_df)}")

        if non_null_amounts < len(budget_df) * 0.8:  # 80% 미만이면 경고
            issues.append(f"⚠️  예산 데이터의 {len(budget_df) - non_null_amounts}개 행에 금액 누락")

    return issues

def check_schedule_data_integrity(json_data, csv_data):
    """일정 데이터 무결성 체크"""
    print("\n=== 일정 데이터 무결성 체크 ===")

    issues = []

    if 'normalized_schedules.csv' not in csv_data:
        print("❌ normalized_schedules.csv 파일이 없습니다.")
        return issues

    schedule_df = csv_data['normalized_schedules.csv']
    raw_df = csv_data.get('raw_data.csv')

    # raw_data에서 일정 관련 행 수 확인
    if raw_df is not None:
        schedule_raw_count = len(raw_df[raw_df['data_type'] == 'schedule'])
        print(f"raw_data의 일정 행 수: {schedule_raw_count}")

    print(f"normalized_schedules 행 수: {len(schedule_df)}")

    # 일정 설명이 있는지 체크
    if 'task_description' in schedule_df.columns:
        non_null_tasks = schedule_df['task_description'].notna().sum()
        print(f"업무 설명이 있는 행: {non_null_tasks}/{len(schedule_df)}")

    return issues

def check_performance_data_integrity(json_data, csv_data):
    """성과지표 데이터 무결성 체크"""
    print("\n=== 성과지표 데이터 무결성 체크 ===")

    issues = []

    if 'normalized_performances.csv' not in csv_data:
        print("❌ normalized_performances.csv 파일이 없습니다.")
        return issues

    performance_df = csv_data['normalized_performances.csv']
    raw_df = csv_data.get('raw_data.csv')

    # raw_data에서 성과지표 관련 행 수 확인
    if raw_df is not None:
        performance_raw_count = len(raw_df[raw_df['data_type'] == 'performance'])
        print(f"raw_data의 성과지표 행 수: {performance_raw_count}")

    print(f"normalized_performances 행 수: {len(performance_df)}")

    # 지표 값이 있는지 체크
    if 'value' in performance_df.columns:
        non_null_values = performance_df['value'].notna().sum()
        print(f"값이 있는 행: {non_null_values}/{len(performance_df)}")


    return issues

def check_field_completeness(json_data, csv_data):
    """필드 완전성 체크 - 중요 필드가 CSV에 포함되었는지 확인"""
    print("\n=== 필드 완전성 체크 ===")

    issues = []

    # sub_projects 필드 체크
    if 'sub_projects.csv' in csv_data:
        sub_df = csv_data['sub_projects.csv']
        print(f"✅ sub_projects.csv: {', '.join(sub_df.columns)}")
    else:
        issues.append("⚠️  sub_projects.csv 파일 없음")

    # 예산 필드 체크
    if 'normalized_budgets.csv' in csv_data:
        budget_df = csv_data['normalized_budgets.csv']
        required_fields = ['sub_project_id', 'budget_year', 'amount']
        missing_fields = [f for f in required_fields if f not in budget_df.columns]

        if missing_fields:
            issues.append(f"⚠️  Budget에 누락된 필드: {', '.join(missing_fields)}")
            print(issues[-1])
        else:
            print("✅ Budget 필수 필드 모두 존재")

    # 일정 필드 체크
    if 'normalized_schedules.csv' in csv_data:
        schedule_df = csv_data['normalized_schedules.csv']
        required_fields = ['sub_project_id', 'year', 'task_description']
        missing_fields = [f for f in required_fields if f not in schedule_df.columns]

        if missing_fields:
            issues.append(f"⚠️  Schedule에 누락된 필드: {', '.join(missing_fields)}")
            print(issues[-1])
        else:
            print("✅ Schedule 필수 필드 모두 존재")

    # 성과지표 필드 체크
    if 'normalized_performances.csv' in csv_data:
        perf_df = csv_data['normalized_performances.csv']
        required_fields = ['sub_project_id', 'performance_year', 'value']
        missing_fields = [f for f in required_fields if f not in perf_df.columns]

        if missing_fields:
            issues.append(f"⚠️  Performance에 누락된 필드: {', '.join(missing_fields)}")
            print(issues[-1])
        else:
            print("✅ Performance 필수 필드 모두 존재")

    return issues

def sample_data_comparison(json_data, csv_data, num_samples=3):
    """샘플 데이터 비교 - 실제 값이 제대로 매핑되었는지 확인"""
    print(f"\n=== 샘플 데이터 비교 (상위 {num_samples}개) ===")

    if 'sub_projects.csv' not in csv_data:
        print("❌ CSV 데이터가 없어 비교할 수 없습니다.")
        return

    sub_df = csv_data['sub_projects.csv']
    raw_df = csv_data.get('raw_data.csv')

    print(f"\n세부사업 샘플 ({num_samples}개):")
    for i in range(min(num_samples, len(sub_df))):
        row = sub_df.iloc[i]
        print(f"\n{i+1}. {row['main_project_name']} - {row['sub_project_name']}")
        print(f"   부처: {row['department_name']}")
        print(f"   프로젝트 코드: {row.get('project_code', 'N/A')}")

        # 관련된 raw_data 확인
        if raw_df is not None:
            related_raw = raw_df[raw_df['sub_project_id'] == row['id']]
            if not related_raw.empty:
                print(f"   관련 raw_data: {len(related_raw)}개")
                data_types = related_raw['data_type'].value_counts().to_dict()
                print(f"   데이터 유형: {data_types}")


def generate_report(all_issues):
    """최종 보고서 생성"""
    print("\n" + "="*60)
    print("최종 무결성 체크 보고서")
    print("="*60)

    if not all_issues:
        print("✅ 모든 체크 통과! 데이터 누락 없음.")
    else:
        print(f"⚠️  총 {len(all_issues)}개의 이슈 발견:")
        for i, issue in enumerate(all_issues, 1):
            print(f"{i}. {issue}")

    print("="*60)

def main():
    """메인 실행 함수"""
    # 경로 설정
    json_path = Path('output/2023년도 생명공학육성시행계획.json')
    csv_dir = Path('csv_output')

    print("="*60)
    print("JSON-CSV 데이터 무결성 체크 시작")
    print("="*60)

    # 데이터 로드
    try:
        json_data = load_json_data(json_path)
        csv_data = load_csv_data(csv_dir)
    except Exception as e:
        print(f"❌ 데이터 로딩 실패: {e}")
        return

    # 구조 분석
    json_stats = analyze_json_structure(json_data)
    csv_stats = analyze_csv_structure(csv_data)

    # 프로젝트 수 비교
    compare_project_counts(json_data, csv_data)

    # 무결성 체크
    all_issues = []
    all_issues.extend(check_budget_data_integrity(json_data, csv_data))
    all_issues.extend(check_schedule_data_integrity(json_data, csv_data))
    all_issues.extend(check_performance_data_integrity(json_data, csv_data))
    all_issues.extend(check_field_completeness(json_data, csv_data))

    # 샘플 데이터 비교
    sample_data_comparison(json_data, csv_data, num_samples=3)

    # 최종 보고서
    generate_report(all_issues)

if __name__ == '__main__':
    main()


import pandas as pd
from oracle_db_manager import OracleDBManager
from config import ORACLE_CONFIG
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# 매칭 리포트 읽기
unmatched = pd.read_csv('normalized_output_government/matching_reports/unmatched_records.csv', encoding='utf-8-sig')
print(f'총 매칭 실패: {len(unmatched)}건\n')
print('=' * 150)

# DB 연결
db = OracleDBManager(ORACLE_CONFIG)
db.connect()
cursor = db.connection.cursor()

# 패턴 분석
print('\n🔍 매칭 실패 패턴 분석:\n')

# 패턴 1: BIZ_NM = DETAIL_BIZ_NM (동일한 경우)
same_name = unmatched[unmatched['biz_nm'] == unmatched['detail_biz_nm']]
print(f'패턴 1: BIZ_NM = DETAIL_BIZ_NM (동일): {len(same_name)}건')
if len(same_name) > 0:
    print('  샘플:')
    for i, row in same_name.head(5).iterrows():
        print(f'    - {row["biz_nm"]}')

        # DB에 있는지 확인
        cursor.execute("""
            SELECT PLAN_ID FROM TB_PLAN_DATA
            WHERE YEAR = :year AND BIZ_NM = :biz_nm AND DELETE_YN = 'N'
            FETCH FIRST 1 ROWS ONLY
        """, {'year': int(row['year']), 'biz_nm': row['biz_nm']})
        result = cursor.fetchone()
        if result:
            print(f'      ✅ DB에 있음: {result[0]} (매칭 로직 문제)')
        else:
            print(f'      ❌ DB에 없음 (신규 사업)')

# 패턴 2: DETAIL_BIZ_NM에 "사업" 유무 차이
print(f'\n패턴 2: "사업" 접미사 차이')
for i, row in unmatched.head(10).iterrows():
    biz = row['biz_nm']
    detail = row['detail_biz_nm']

    # "사업" 제거해서 검색
    detail_without_suffix = detail.replace('사업', '').strip()

    cursor.execute("""
        SELECT PLAN_ID, DETAIL_BIZ_NM FROM TB_PLAN_DATA
        WHERE YEAR = :year 
        AND BIZ_NM = :biz_nm
        AND (DETAIL_BIZ_NM = :detail1 OR DETAIL_BIZ_NM = :detail2)
        AND DELETE_YN = 'N'
        FETCH FIRST 1 ROWS ONLY
    """, {
        'year': int(row['year']),
        'biz_nm': biz,
        'detail1': detail,
        'detail2': detail_without_suffix
    })

    result = cursor.fetchone()
    if result:
        print(f'  ✅ 매칭 가능: {biz[:30]} / {detail[:30]} → DB: {result[1]}')

# 패턴 3: 완전히 새로운 사업
print(f'\n패턴 3: DB에 완전히 없는 신규 사업')
new_count = 0
for i, row in unmatched.head(20).iterrows():
    cursor.execute("""
        SELECT COUNT(*) FROM TB_PLAN_DATA
        WHERE YEAR = :year AND BIZ_NM = :biz_nm AND DELETE_YN = 'N'
    """, {'year': int(row['year']), 'biz_nm': row['biz_nm']})

    count = cursor.fetchone()[0]
    if count == 0:
        new_count += 1
        if new_count <= 5:
            print(f'  ❌ 신규: [{row["year"]}] {row["biz_nm"][:50]}')

print(f'\n신규 사업 (BIZ_NM 자체가 DB에 없음): 최소 {new_count}건')

db.close()

# 요약
print('\n' + '=' * 150)
print('📊 요약:')
print(f'  - 총 매칭 실패: {len(unmatched)}건')
print(f'  - BIZ_NM = DETAIL_BIZ_NM: {len(same_name)}건')
print(f'  - 신규 사업 (추정): {new_count}건 이상')
print('=' * 150)


"""
CSV를 DB에 밀어넣는 스크립트 - 테이블 DROP 후 재생성
"""

import pandas as pd
import sys
from pathlib import Path
from oracle_db_manager import OracleDBManager
from config import ORACLE_CONFIG

def load_all_csv_to_db():
    """CSV를 DB에 밀어넣기"""

    csv_dir = Path("normalized_output_government")
    budget_csv = csv_dir / "TB_PLAN_BUDGET.csv"
    schedule_csv = csv_dir / "TB_PLAN_SCHEDULE.csv"
    performance_csv = csv_dir / "TB_PLAN_PERFORMANCE.csv"

    for csv_file in [budget_csv, schedule_csv, performance_csv]:
        if not csv_file.exists():
            print(f"[ERROR] CSV 파일 없음: {csv_file}")
            return

    print("[DB] 연결 중...")
    sys.stdout.flush()
    db = OracleDBManager(ORACLE_CONFIG)
    db.connect()
    print("[OK] DB 연결 성공")
    sys.stdout.flush()

    conn = db.connection
    cursor = conn.cursor()

    try:
        # 1. 테이블 DROP
        print("\n[DROP] 기존 테이블 DROP 중...")
        sys.stdout.flush()
        for table_name in ['TB_PLAN_PERFORMANCE', 'TB_PLAN_SCHEDULE', 'TB_PLAN_BUDGET']:
            try:
                cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
                print(f"   [OK] {table_name} DROP 완료")
                sys.stdout.flush()
            except Exception as e:
                print(f"   [WARN] {table_name} DROP 실패 (없을 수 있음)")
                sys.stdout.flush()
        conn.commit()

        # 2. TB_PLAN_BUDGET 생성 및 적재
        print("\n[TABLE] TB_PLAN_BUDGET 생성 및 적재 중...")
        sys.stdout.flush()
        df_budget = pd.read_csv(budget_csv, encoding='utf-8-sig')
        print(f"   CSV 컬럼: {df_budget.columns.tolist()}")
        print(f"   데이터 건수: {len(df_budget)}건")
        sys.stdout.flush()

        # CSV 컬럼에 맞춰 동적으로 CREATE TABLE
        cols = df_budget.columns.tolist()
        col_defs = []
        for col in cols:
            if 'AMOUNT' in col or 'PRC' in col:
                col_defs.append(f"{col} NUMBER")
            elif 'YEAR' in col:
                col_defs.append(f"{col} NUMBER")
            else:
                col_defs.append(f"{col} VARCHAR2(500)")

        create_sql = f"CREATE TABLE TB_PLAN_BUDGET ({', '.join(col_defs)})"
        cursor.execute(create_sql)
        print("   [OK] 테이블 생성 완료")
        sys.stdout.flush()

        # 동적 INSERT
        placeholders = ', '.join([f':{i+1}' for i in range(len(cols))])
        insert_sql = f"INSERT INTO TB_PLAN_BUDGET ({', '.join(cols)}) VALUES ({placeholders})"

        count = 0
        for _, row in df_budget.iterrows():
            values = [row[col] if pd.notna(row[col]) else None for col in cols]
            cursor.execute(insert_sql, values)
            count += 1
            if count % 500 == 0:
                print(f"   {count}건 처리중...")
                sys.stdout.flush()
        conn.commit()
        print(f"   [OK] {len(df_budget)}건 INSERT 완료")
        sys.stdout.flush()

        # 3. TB_PLAN_SCHEDULE 생성 및 적재
        print("\n[TABLE] TB_PLAN_SCHEDULE 생성 및 적재 중...")
        sys.stdout.flush()
        df_schedule = pd.read_csv(schedule_csv, encoding='utf-8-sig')
        print(f"   CSV 컬럼: {df_schedule.columns.tolist()}")
        print(f"   데이터 건수: {len(df_schedule)}건")
        sys.stdout.flush()

        # CSV 컬럼에 맞춰 동적으로 CREATE TABLE
        cols = df_schedule.columns.tolist()
        col_defs = []
        for col in cols:
            if col == 'PLAN_ID':
                col_defs.append(f"{col} VARCHAR2(50)")
            else:
                col_defs.append(f"{col} CLOB")

        create_sql = f"CREATE TABLE TB_PLAN_SCHEDULE ({', '.join(col_defs)})"
        cursor.execute(create_sql)
        print("   [OK] 테이블 생성 완료")
        sys.stdout.flush()

        # 동적 INSERT
        placeholders = ', '.join([f':{i+1}' for i in range(len(cols))])
        insert_sql = f"INSERT INTO TB_PLAN_SCHEDULE ({', '.join(cols)}) VALUES ({placeholders})"

        count = 0
        for _, row in df_schedule.iterrows():
            values = [row[col] if pd.notna(row[col]) else None for col in cols]
            cursor.execute(insert_sql, values)
            count += 1
            if count % 500 == 0:
                print(f"   {count}건 처리중...")
                sys.stdout.flush()
        conn.commit()
        print(f"   [OK] {len(df_schedule)}건 INSERT 완료")
        sys.stdout.flush()

        # 4. TB_PLAN_PERFORMANCE 생성 및 적재
        print("\n[TABLE] TB_PLAN_PERFORMANCE 생성 및 적재 중...")
        sys.stdout.flush()
        df_performance = pd.read_csv(performance_csv, encoding='utf-8-sig')
        print(f"   CSV 컬럼: {df_performance.columns.tolist()}")
        print(f"   데이터 건수: {len(df_performance)}건")
        sys.stdout.flush()

        # CSV 컬럼에 맞춰 동적으로 CREATE TABLE
        cols = df_performance.columns.tolist()
        col_defs = []
        for col in cols:
            if col == 'PLAN_ID':
                col_defs.append(f"{col} VARCHAR2(50)")
            else:
                col_defs.append(f"{col} CLOB")

        create_sql = f"CREATE TABLE TB_PLAN_PERFORMANCE ({', '.join(col_defs)})"
        cursor.execute(create_sql)
        print("   [OK] 테이블 생성 완료")
        sys.stdout.flush()

        # 동적 INSERT
        placeholders = ', '.join([f':{i+1}' for i in range(len(cols))])
        insert_sql = f"INSERT INTO TB_PLAN_PERFORMANCE ({', '.join(cols)}) VALUES ({placeholders})"

        count = 0
        for _, row in df_performance.iterrows():
            values = [row[col] if pd.notna(row[col]) else None for col in cols]
            cursor.execute(insert_sql, values)
            count += 1
            if count % 500 == 0:
                print(f"   {count}건 처리중...")
                sys.stdout.flush()
        conn.commit()
        print(f"   [OK] {len(df_performance)}건 INSERT 완료")
        sys.stdout.flush()

        print(f"\n[DONE] 전체 적재 완료!")
        print(f"   TB_PLAN_BUDGET: {len(df_budget)}건")
        print(f"   TB_PLAN_SCHEDULE: {len(df_schedule)}건")
        print(f"   TB_PLAN_PERFORMANCE: {len(df_performance)}건")
        sys.stdout.flush()

    except Exception as e:
        print(f"\n[ERROR] 오류 발생: {e}")
        sys.stdout.flush()
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    load_all_csv_to_db()


# Oracle TB_PLAN_DATA 회사 스키마 적용 가이드

## 1. 회사 실제 스키마 확인 사항

### 필수 확인 컬럼들
회사의 실제 TB_PLAN_DATA 테이블에서 다음 컬럼들의 정확한 정의를 확인하세요:

```sql
-- 회사 스키마 확인 쿼리
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    DATA_LENGTH,
    DATA_PRECISION,
    DATA_SCALE,
    NULLABLE
FROM ALL_TAB_COLUMNS
WHERE TABLE_NAME = 'TB_PLAN_DATA'
AND OWNER = 'YOUR_SCHEMA'  -- 실제 스키마명으로 변경
ORDER BY COLUMN_ID;
```

### 주요 차이점 확인
1. **금액 관련 컬럼**
   - `TOTAL_RESPRC`: VARCHAR2 vs NUMBER?
   - `CUR_RESPRC`: VARCHAR2 vs NUMBER?
   - 다른 금액 컬럼들의 타입

2. **누락된 컬럼**
   - `AREA` (3대영역)
   - `RESPERIOD` (연구기간)
   - `CUR_RESPERIOD` (현 연구기간)
   - `BIZ_CONTENTS_KEYWORD` (사업 내용 키워드)
   - `BIOLOGY_WEI`, `RED_WEI`, `GREEN_WEI`, `WHITE_WEI`, `FUSION_WEI` (가중치)
   - `REGUL_WEI` (규제 비중)
   - `WEI` (비중)

3. **관리 컬럼**
   - `DELETE_YN`, `DELETE_ID`, `DELETE_DT`
   - `MODIFY_ID`, `MODIFY_DT`

## 2. 스키마 적용 전략

### Option 1: 기존 테이블 사용 (권장)
```python
# config.py 설정
USE_EXISTING_TABLES = True  # 기존 테이블 사용
TRUNCATE_BEFORE_LOAD = False  # 데이터 보존하면서 MERGE 사용
```

### Option 2: TRUNCATE 후 재적재
```python
# config.py 설정
USE_EXISTING_TABLES = True
TRUNCATE_BEFORE_LOAD = True  # 기존 데이터 삭제 후 적재
```

### Option 3: 임시 테이블 사용
```python
# config.py 설정
USE_TEMP_TABLES = True  # TB_PLAN_DATA_TEMP 등에 먼저 적재
MERGE_TO_PRODUCTION = True  # 검증 후 운영 테이블로 MERGE
```

## 3. 데이터 타입 매핑 규칙

### 금액 필드
```python
# 회사 스키마가 VARCHAR2인 경우
def format_amount_for_varchar(amount):
    if amount is None:
        return None
    return str(int(amount))  # 문자열로 변환

# 회사 스키마가 NUMBER인 경우  
def format_amount_for_number(amount):
    if amount is None:
        return None
    return float(amount)  # 숫자로 유지
```

### 날짜 필드
```python
# DATE 타입 처리
def format_date(date_str):
    if not date_str:
        return None
    # YYYY-MM-DD 형식 보장
    return date_str[:10]
```

## 4. 필수 데이터 소스 매핑

### AREA (3대영역) 데이터
```json
// project_details.json 구조 예시
{
  "sub_project_id": {
    "area": "레드바이오",  // 또는 "그린바이오", "화이트바이오"
    ...
  }
}
```

### 가중치 데이터
```json
// project_details.json 구조 예시
{
  "sub_project_id": {
    "weights": {
      "biology": 30,
      "red": 20,
      "green": 15,
      "white": 10,
      "fusion": 25,
      "regulation": 5
    }
  }
}
```

### 키워드 데이터
```json
// project_details.json 구조 예시
{
  "sub_project_id": {
    "keywords": ["바이오", "의약품", "신약개발"]
  }
}
```

## 5. 실행 전 체크리스트

### 데이터 파일 확인
- [ ] `sub_projects.csv` 존재 및 형식 확인
- [ ] `normalized_overviews.csv` 존재 및 형식 확인
- [ ] `normalized_budgets.csv` 존재 및 형식 확인
- [ ] `normalized_schedules.csv` 존재 및 형식 확인
- [ ] `normalized_performances.csv` 존재 및 형식 확인
- [ ] `key_achievements.csv` 존재 및 형식 확인
- [ ] `plan_details.csv` 존재 및 형식 확인
- [ ] `project_details.json` (추가 데이터용) 존재 확인

### 데이터베이스 확인
- [ ] Oracle 접속 정보 확인
- [ ] 대상 스키마 권한 확인 (SELECT, INSERT, UPDATE, DELETE)
- [ ] 테이블 존재 여부 확인
- [ ] 기존 데이터 백업 여부 확인

### 설정 확인
- [ ] `config.py` Oracle 접속 정보 설정
- [ ] 문자 인코딩 설정 (UTF-8)
- [ ] 로그 레벨 설정

## 6. 실행 방법

### 테스트 실행
```bash
# 1. 드라이런 모드로 확인
python load_oracle_db_improved.py --dry-run

# 2. 소량 데이터로 테스트
python load_oracle_db_improved.py --limit 10

# 3. 검증
python validate_loaded_data.py
```

### 운영 실행
```bash
# 1. 백업
python backup_oracle_tables.py

# 2. 실제 적재
python load_oracle_db_improved.py

# 3. 검증
python validate_loaded_data.py

# 4. 문제 발생 시 롤백
python restore_oracle_tables.py
```

## 7. 트러블슈팅

### 인코딩 문제
```python
# UTF-8 강제 설정
import os
os.environ['NLS_LANG'] = 'KOREAN_KOREA.AL32UTF8'
```

### 금액 타입 불일치
```python
# 동적 타입 체크
def get_column_type(table_name, column_name):
    query = """
        SELECT DATA_TYPE 
        FROM USER_TAB_COLUMNS 
        WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2
    """
    result = db.execute_query(query, (table_name, column_name))
    return result[0][0] if result else None

# 타입에 따른 처리
column_type = get_column_type('TB_PLAN_DATA', 'TOTAL_RESPRC')
if column_type == 'VARCHAR2':
    value = str(amount) if amount else None
elif column_type == 'NUMBER':
    value = float(amount) if amount else None
```

### PLAN_ID 매핑 실패
```python
# 대체 매핑 전략
def find_plan_id(sub_project_id, sub_project_name, department_name):
    # 1차: 매핑 테이블
    if sub_project_id in plan_id_mapping:
        return plan_id_mapping[sub_project_id]
    
    # 2차: DB 조회
    plan_id = db.lookup_plan_id(sub_project_id)
    if plan_id:
        return plan_id
    
    # 3차: 이름으로 매칭
    plan_id = db.lookup_plan_id_by_name(sub_project_name, department_name)
    if plan_id:
        return plan_id
    
    # 4차: 새로 생성
    return db.get_next_plan_id(year)
```

## 8. 검증 쿼리

### 적재 결과 확인
```sql
-- 테이블별 레코드 수
SELECT 'TB_PLAN_DATA' AS TABLE_NAME, COUNT(*) AS CNT FROM TB_PLAN_DATA
UNION ALL
SELECT 'TB_PLAN_SCHEDULES', COUNT(*) FROM TB_PLAN_SCHEDULES
UNION ALL
SELECT 'TB_PLAN_PERFORMANCES', COUNT(*) FROM TB_PLAN_PERFORMANCES
UNION ALL
SELECT 'TB_PLAN_BUDGETS', COUNT(*) FROM TB_PLAN_BUDGETS
UNION ALL
SELECT 'TB_PLAN_ACHIEVEMENTS', COUNT(*) FROM TB_PLAN_ACHIEVEMENTS
UNION ALL
SELECT 'TB_PLAN_DETAILS', COUNT(*) FROM TB_PLAN_DETAILS;

-- NULL 값 체크
SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    SUM(CASE WHEN AREA IS NULL THEN 1 ELSE 0 END) AS AREA_NULL,
    SUM(CASE WHEN BIZ_CONTENTS_KEYWORD IS NULL THEN 1 ELSE 0 END) AS KEYWORD_NULL,
    SUM(CASE WHEN TOTAL_RESPRC IS NULL THEN 1 ELSE 0 END) AS TOTAL_RESPRC_NULL
FROM TB_PLAN_DATA;

-- 금액 데이터 검증
SELECT 
    PLAN_ID,
    TOTAL_RESPRC,
    TOTAL_RESPRC_GOV,
    TOTAL_RESPRC_CIV,
    (NVL(TOTAL_RESPRC_GOV, 0) + NVL(TOTAL_RESPRC_CIV, 0)) AS CALCULATED_TOTAL
FROM TB_PLAN_DATA
WHERE TOTAL_RESPRC IS NOT NULL;
```

## 9. 주의사항

1. **운영 데이터베이스 작업 시**
   - 반드시 백업 먼저 수행
   - 업무 시간 외 작업 권장
   - DBA와 사전 협의

2. **대용량 데이터 처리 시**
   - 배치 크기 조정 (기본 100건 → 1000건)
   - 커밋 주기 조정
   - 인덱스 임시 비활성화 고려

3. **문자 인코딩**
   - 한글 깨짐 현상 발생 시 NLS_LANG 확인
   - CSV 파일 인코딩 확인 (UTF-8-SIG 권장)

## 10. 연락처

문제 발생 시 연락처:
- DBA팀: xxx-xxxx
- 개발팀: xxx-xxxx
- 긴급 연락처: xxx-xxxx
# Oracle 데이터베이스 적재 가이드

## 📁 프로젝트 구조
```
PythonProject/
├── normalized/                      # 정규화된 CSV 파일
│   ├── sub_projects.csv            # 마스터 프로젝트 데이터
│   ├── normalized_overviews.csv    # 사업 개요
│   ├── normalized_budgets.csv      # 예산 정보
│   ├── normalized_schedules.csv    # 일정 정보
│   ├── normalized_performances.csv # 성과 정보
│   ├── key_achievements.csv        # 대표 성과
│   └── plan_details.csv           # 추진 계획
├── normalized_output_government/    # 정부 출력 (선택)
└── load_oracle_final.py            # 메인 적재 스크립트

```

## 🔧 사용 방법

### 1. 설정 파일 수정
`config.py` 파일에서 Oracle 연결 정보 설정:
```python
ORACLE_CONFIG = {
    'host': 'your_oracle_host',
    'port': 1521,
    'sid': 'your_sid',
    'user': 'your_username',
    'password': 'your_password'
}
```

### 2. 실행
```bash
python load_oracle_final.py
```

### 3. 데이터 삭제 후 재적재 (옵션)
```python
# load_oracle_final.py의 main() 함수에서 주석 해제
loader.truncate_tables()  # 기존 데이터 삭제
```

## 📊 회사 DDL과의 매핑

### TB_PLAN_DATA 컬럼 매핑
| 컬럼명 | 타입 | 데이터 소스 | 비고 |
|--------|------|------------|------|
| PLAN_ID | CHAR(30) | 생성 (YYYYNNN) | PK |
| YEAR | NUMBER | sub_projects.csv | |
| NUM | NUMBER | 순번 생성 | |
| NATION_ORGAN_NM | VARCHAR2(768) | department_name | |
| DETAIL_BIZ_NM | VARCHAR2(768) | sub_project_name | |
| BIZ_NM | VARCHAR2(768) | main_project_name | |
| BIZ_TYPE | VARCHAR2(768) | overviews: project_type | |
| AREA | VARCHAR2(768) | NULL | 추후 입력 |
| REP_FLD | VARCHAR2(768) | overviews: field | |
| BIOLOGY_WEI | NUMBER | NULL | 추후 입력 |
| RED_WEI | NUMBER | NULL | 추후 입력 |
| GREEN_WEI | NUMBER | NULL | 추후 입력 |
| WHITE_WEI | NUMBER | NULL | 추후 입력 |
| FUSION_WEI | NUMBER | NULL | 추후 입력 |
| LEAD_ORGAN_NM | VARCHAR2(768) | overviews: managing_dept | |
| MNG_ORGAN_NM | VARCHAR2(768) | overviews: managing_org | |
| BIZ_SDT | DATE | schedules: min(start_date) | |
| BIZ_EDT | DATE | schedules: max(end_date) | |
| RESPERIOD | VARCHAR2(768) | 날짜 범위 생성 | |
| CUR_RESPERIOD | VARCHAR2(768) | 현재 연도 | |
| **TOTAL_RESPRC** | **VARCHAR2(768)** | 총 예산 합계 | **문자열로 저장** |
| TOTAL_RESPRC_GOV | NUMBER | 정부 예산 | |
| TOTAL_RESPRC_CIV | NUMBER | 민간 예산 | |
| **CUR_RESPRC** | **VARCHAR2(768)** | 현재 연도 예산 | **문자열로 저장** |
| CUR_RESPRC_GOV | NUMBER | 현재 연도 정부 | |
| CUR_RESPRC_CIV | NUMBER | 현재 연도 민간 | |
| LAST_GOAL | VARCHAR2(4000) | overviews: objective | |
| BIZ_CONTENTS | VARCHAR2(4000) | overviews: content | |
| BIZ_CONTENTS_KEYWORD | VARCHAR2(4000) | NULL | 추후 입력 |
| REGUL_WEI | NUMBER | NULL | 추후 입력 |
| WEI | VARCHAR2(768) | NULL | 추후 입력 |
| PERFORM_PRC | NUMBER | 실적 금액 | |
| PLAN_PRC | NUMBER | 계획 금액 | |

## ⚠️ 주의사항

### 1. 금액 필드 타입
- `TOTAL_RESPRC`, `CUR_RESPRC`: **VARCHAR2** (문자열로 저장)
- 나머지 금액 필드: **NUMBER** (숫자로 저장)

### 2. 인코딩 문제
- '민간' 문자가 깨지는 경우 자동 처리
- UTF-8 인코딩 사용

### 3. NULL 처리
- AREA, 가중치 관련 필드는 추후 입력 예정으로 NULL 허용
- 필수 필드: PLAN_ID, YEAR, NUM, 부처/사업명

### 4. PLAN_ID 매핑
- 형식: YYYYNNN (예: 2024001)
- sub_project_id → PLAN_ID 매핑 자동 생성
- 하위 테이블에서 참조

## 📈 적재 통계
적재 완료 후 다음 정보 출력:
- 총 적재 레코드 수
- 테이블별 적재 건수
- 스킵된 레코드 수
- 발생한 오류

## 🔍 검증
```sql
-- 적재 결과 확인
SELECT COUNT(*) FROM TB_PLAN_DATA;
SELECT COUNT(*) FROM TB_PLAN_SCHEDULES;
SELECT COUNT(*) FROM TB_PLAN_BUDGETS;

-- NULL 체크
SELECT 
    COUNT(*) AS TOTAL,
    SUM(CASE WHEN AREA IS NULL THEN 1 ELSE 0 END) AS AREA_NULL,
    SUM(CASE WHEN BIZ_CONTENTS_KEYWORD IS NULL THEN 1 ELSE 0 END) AS KEYWORD_NULL
FROM TB_PLAN_DATA;

-- 금액 데이터 확인
SELECT 
    PLAN_ID,
    TOTAL_RESPRC,  -- VARCHAR2
    TOTAL_RESPRC_GOV,  -- NUMBER
    TOTAL_RESPRC_CIV   -- NUMBER
FROM TB_PLAN_DATA 
WHERE ROWNUM <= 10;
```

## 🛠️ 트러블슈팅

### 문제: ORA-01400: NULL을 삽입할 수 없습니다
- 원인: 필수 컬럼에 NULL 값
- 해결: CSV 파일의 필수 데이터 확인

### 문제: ORA-01722: 수치가 부적합합니다
- 원인: 숫자 필드에 문자열
- 해결: 금액 필드 타입 확인 (VARCHAR2 vs NUMBER)

### 문제: 한글 깨짐
- 원인: 인코딩 설정
- 해결: NLS_LANG='KOREAN_KOREA.AL32UTF8' 설정 확인

## 📞 문의
- 데이터베이스 관련: DBA팀
- 데이터 내용 관련: 데이터팀
- 스크립트 관련: 개발팀
# 생명공학육성시행계획 PDF → Oracle DB 처리 시스템

정부 생명공학육성시행계획 PDF 파일을 자동으로 파싱하여 정규화된 CSV를 생성하고, 기존 BICS DB와 매칭하여 Oracle DB에 적재하는 통합 시스템입니다.

## 시스템 개요

```
┌─────────────────────────────────────────────────────────────────────┐
│                         처리 파이프라인                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   📄 PDF 파일      →    📋 JSON        →    📊 CSV         →    🗄️ Oracle DB │
│   (input/)             (output/)           (normalized_        (BICS_DEV)     │
│                                             output_government/)               │
│                                                                     │
│   extract_pdf_      normalize_          load_oracle_              │
│   to_json.py        government_         direct.py                 │
│                     standard.py                                    │
└─────────────────────────────────────────────────────────────────────┘
```

## 핵심 기능

### 1. PDF → JSON 추출 (`extract_pdf_to_json.py`)
- **pdfplumber** 기반 테이블 및 텍스트 추출
- 페이지별 내역사업 자동 구분
- 3가지 카테고리 자동 분류: `overview`, `plan`, `performance`
- 폰트 문제 없이 안정적 추출

### 2. JSON → CSV 정규화 (`normalize_government_standard.py`)
- **스마트 PLAN_ID 매칭**: BICS DB의 기존 데이터와 자동 매칭
  - 완전 일치 → 정규화 일치 → 교차 일치 → 유사도 매칭
- **5개 정규화 테이블 생성**:
  | 테이블명 | 설명 |
  |---------|------|
  | `TB_PLAN_DATA.csv` | 내역사업 메인 (부처, 사업명, 예산 등) |
  | `TB_PLAN_BUDGET.csv` | 연도별 예산 상세 (실적/계획) |
  | `TB_PLAN_SCHEDULE.csv` | 세부 일정 (분기별) |
  | `TB_PLAN_PERFORMANCE.csv` | 성과 상세 (특허, 논문, 인력) |
  | `TB_PLAN_ACHIEVEMENTS.csv` | 대표성과 |

### 3. CSV → Oracle DB 적재 (`load_oracle_direct.py`)
- **2개 DB 연결 구조**:
  - `BICS` (읽기): 기존 TB_PLAN_DATA 조회 및 PLAN_ID 매칭
  - `BICS_DEV` (쓰기): 하위 테이블 적재
- FK 제약조건 자동 처리
- 매칭 리포트 자동 생성

## 설치

### 1. 필수 요구사항
- Python 3.11+
- Oracle Database 11g+ (BICS, BICS_DEV 스키마)

### 2. 패키지 설치
```bash
pip install -r requirements.txt
```

### 3. 환경 설정
`config.py` 파일에서 Oracle 접속 정보 설정:
```python
ORACLE_CONFIG = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics",
    "password": "your_password"
}

ORACLE_CONFIG_DEV = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics_dev",
    "password": "your_password"
}
```

## 사용 방법

### 방법 1: 커맨드라인 (권장)

```bash
# 1. input 폴더에 PDF 파일 넣기
cp 2024년도_생명공학육성시행계획.pdf input/

# 2. 전체 파이프라인 실행
python main.py

# 3. DB 적재 없이 CSV만 생성
python main.py --skip-db
```

### 방법 2: Streamlit 웹 UI

```bash
streamlit run streamlit_app.py
```

브라우저에서 `http://localhost:8501` 접속:
1. PDF 파일 업로드
2. "전체 파이프라인 실행" 클릭
3. 결과 CSV 다운로드 또는 DB 적재 확인

## 프로젝트 구조

```
KRIBB_file_PDFtoDB/
├── config.py                       # 환경 설정 (DB 접속 정보)
├── extract_pdf_to_json.py          # PDF → JSON 추출
├── normalize_government_standard.py # JSON → CSV 정규화
├── load_oracle_direct.py           # CSV → Oracle DB 적재
├── oracle_db_manager.py            # Oracle DB 연결 관리
├── main.py                         # CLI 메인 프로그램
├── streamlit_app.py                # Streamlit 웹 UI
├── requirements.txt                # 패키지 목록
│
├── input/                          # PDF 입력 폴더
├── output/                         # JSON 출력 폴더
└── normalized_output_government/   # CSV 정규화 결과
    ├── TB_PLAN_DATA.csv
    ├── TB_PLAN_BUDGET.csv
    ├── TB_PLAN_SCHEDULE.csv
    ├── TB_PLAN_PERFORMANCE.csv
    ├── TB_PLAN_ACHIEVEMENTS.csv
    └── matching_reports/           # 매칭 리포트
        ├── matching_report.csv
        └── unmatched_records.csv
```

## PLAN_ID 매칭 로직

### 매칭 전략 (순차 적용)

1. **완전 일치** (score: 100)
   - YEAR + BIZ_NM + DETAIL_BIZ_NM 모두 완전 일치

2. **정규화 일치** (score: 99)
   - 특수문자/공백 제거 후 일치

3. **교차 일치** (score: 98)
   - BIZ_NM ↔ DETAIL_BIZ_NM 서로 바뀐 경우

4. **유사도 매칭** (score: 80~97)
   - fuzzywuzzy 기반 유사도 80% 이상

5. **매칭 실패**
   - 임시 ID 부여: `TEMP_YEAR_XXXX`
   - `unmatched_records.csv`에 기록

### 매칭 결과 확인

```bash
# 매칭 성공 리포트
cat normalized_output_government/matching_reports/matching_report.csv

# 매칭 실패 리포트 (수동 검토 필요)
cat normalized_output_government/matching_reports/unmatched_records.csv
```

## 데이터베이스 스키마

### TB_PLAN_DATA (메인 테이블)
```sql
PLAN_ID             CHAR(30) PRIMARY KEY
YEAR                NUMBER(4)
NUM                 NUMBER
NATION_ORGAN_NM     VARCHAR2(768)   -- 부처명
BIZ_NM              VARCHAR2(768)   -- 세부사업명
DETAIL_BIZ_NM       VARCHAR2(768)   -- 내역사업명
...
```

### TB_PLAN_BUDGET (예산 하위 테이블)
```sql
BUDGET_ID           CHAR(30) PRIMARY KEY
PLAN_ID             CHAR(30) FK → TB_PLAN_DATA
BUDGET_YEAR         NUMBER(4)
CATEGORY            VARCHAR2(50)    -- 실적/계획
TOTAL_AMOUNT        NUMBER
GOV_AMOUNT          NUMBER          -- 정부
PRIVATE_AMOUNT      NUMBER          -- 민간
LOCAL_AMOUNT        NUMBER          -- 지방
ETC_AMOUNT          NUMBER          -- 기타
```

### TB_PLAN_SCHEDULE, TB_PLAN_PERFORMANCE, TB_PLAN_ACHIEVEMENTS
- 모두 `PLAN_ID`로 TB_PLAN_DATA와 FK 연결

## 문제 해결

### Oracle 연결 오류
```
ORA-12541: TNS:no listener
```
**해결**: `config.py`의 DB 정보 확인 및 Oracle 서버 상태 점검

### 중복 키 에러
```
ORA-00001: unique constraint violated
```
**해결**: Streamlit 사이드바에서 "DB 데이터 초기화" 버튼 클릭

### 매칭률이 낮은 경우
```bash
# 매칭 실패 레코드 확인
cat normalized_output_government/matching_reports/unmatched_records.csv
```
**해결**: DB의 기존 데이터와 PDF의 사업명 표기 차이 확인

### PDF 파싱 오류
```bash
# JSON 구조 확인
python -m json.tool output/2024년도_생명공학육성시행계획.json
```
**해결**: PDF 구조가 변경된 경우 `extract_pdf_to_json.py` 수정

## Streamlit Cloud 배포

1. GitHub에 코드 푸시
2. [share.streamlit.io](https://share.streamlit.io) 접속
3. 저장소 연결 및 `streamlit_app.py` 선택
4. **주의**: DB 접속은 네트워크 환경에 따라 제한될 수 있음

## 개발자

- **개발**: MINANI
- **버전**: 2.0
- **최종 업데이트**: 2025-11-25

## 라이선스

MIT License

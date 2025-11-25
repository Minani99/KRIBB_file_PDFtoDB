# 🧬 생명공학육성시행계획 데이터 처리 시스템

정부 생명공학육성시행계획 PDF 파일을 자동으로 파싱하여 정규화된 CSV를 생성하고 Oracle DB에 적재하는 통합 시스템입니다.

## 🎯 주요 기능

### 1. PDF → JSON 추출 (`extract_pdf_to_json.py`)
- **pdfplumber** 기반 지능형 테이블 추출
- 페이지별 내역사업 자동 구분
- 3가지 카테고리 자동 분류 (overview, plan, performance)
- JSON 형식으로 구조화 저장

### 2. 정부 표준 정규화 (`normalize_government_standard.py`)
- **스마트 PLAN_ID 매칭** (YEAR + BIZ_NM + DETAIL_BIZ_NM)
  - 완전 일치 → 부분 일치 (95점 이상) → 임시 ID 순차 적용
- **5개 정규화 테이블 생성**:
  - `TB_PLAN_DATA`: 내역사업 메인 (부처, 사업명, 예산 등)
  - `TB_PLAN_BUDGET`: 연도별 예산 상세 (실적/계획)
  - `TB_PLAN_SCHEDULE`: 세부 일정
  - `TB_PLAN_PERFORMANCE`: 성과 상세 (특허, 논문, 인력)
  - `raw_data`: 원본 데이터
- **다년도 데이터 통합**: 2020~2024년 5개 파일 → 단일 CSV

### 3. Oracle DB 자동 적재 (`oracle_db_manager.py`)
- 정규화된 CSV → Oracle DB 자동 적재
- FK 제약조건 자동 처리
- 트랜잭션 롤백 지원

### 4. 통합 실행 파이프라인
- **전체 처리**: `run_normalize_all.py` - PDF 5개 → CSV 통합
- **메인 파이프라인**: `main.py` - PDF → JSON → CSV → DB 전체 흐름
- **Streamlit UI**: `streamlit_app.py` - 웹 기반 업로드 & 실행

## 📦 설치

### 1. 필수 요구사항
- Python 3.11+
- Oracle Database 11g+
- Oracle Instant Client

### 2. 패키지 설치
```bash
pip install -r requirements.txt
```

### 3. 환경 설정
`.env` 파일 생성 (`.env.example` 참고):
```bash
# Oracle 데이터베이스 설정
ORACLE_HOST=192.168.73.208
ORACLE_PORT=1521
ORACLE_SID=bics
ORACLE_USER=bics
ORACLE_PASSWORD=your_password
```

## 🚀 사용 방법

### 방법 1: 통합 정규화 (권장) ✨
```bash
# input/ 폴더의 모든 JSON을 하나의 CSV로 통합
python run_normalize_all.py
```
**출력**: `normalized_output_government/*.csv` (5개년 통합)

### 방법 2: 전체 파이프라인 (PDF → JSON → CSV → DB)
```bash
# 단일 파일 처리
python main.py input/2024년도_생명공학육성시행계획.pdf

# 전체 파일 처리
python main.py
```

### 방법 3: Streamlit 웹 UI
```bash
streamlit run streamlit_app.py
```
브라우저에서 `http://localhost:8501` 접속
- PDF 업로드 → JSON 추출 → CSV 생성 → DB 적재 일괄 처리
- 실시간 진행률 표시 및 결과 다운로드

## 📁 프로젝트 구조

```
PythonProject/
├── 📄 핵심 파일
│   ├── config.py                          # 환경 설정
│   ├── extract_pdf_to_json.py             # PDF → JSON 추출
│   ├── normalize_government_standard.py   # JSON → CSV 정규화 (스마트 매칭)
│   ├── oracle_db_manager.py               # Oracle DB 연결/적재
│   ├── oracle_table_ddl.py                # DB 테이블 DDL
│   │
│   ├── run_normalize_all.py               # ⭐ 통합 실행 (5개 파일 → 1개 CSV)
│   ├── main.py                            # 전체 파이프라인 (PDF→JSON→CSV→DB)
│   └── streamlit_app.py                   # 웹 UI
│
├── 📂 데이터 폴더
│   ├── input/                             # PDF 입력 (2020~2024년)
│   ├── output/                            # JSON 출력
│   └── normalized_output_government/      # CSV 정규화 결과
│       ├── TB_PLAN_DATA.csv              # 내역사업 (1,558건)
│       ├── TB_PLAN_BUDGET.csv            # 예산
│       ├── TB_PLAN_SCHEDULE.csv          # 일정
│       ├── TB_PLAN_PERFORMANCE.csv       # 성과
│       └── raw_data.csv                  # 원본
│
└── 📋 기타
    ├── requirements.txt                   # 패키지 목록
    ├── README.md                          # 프로젝트 설명
    ├── NORMALIZATION_STRATEGY.md          # 정규화 전략 문서
    └── .env.example                       # 환경 설정 예시
```
└── normalized_output_government/        # CSV 출력 폴더
    ├── TB_PLAN_DATA.csv
    ├── TB_PLAN_BUDGET.csv
    ├── TB_PLAN_SCHEDULE.csv
    ├── TB_PLAN_PERFORMANCE.csv
    ├── TB_PLAN_ACHIEVEMENTS.csv
    └── matching_reports/                # 매칭 리포트
        ├── matching_report.csv
        ├── unmatched_records.csv
        └── diff_report.csv
```

## 🔧 핵심 모듈 설명

### 1. `extract_pdf_to_json.py`
PDF 파일을 JSON으로 변환
- pdfplumber로 테이블 추출
- 페이지별 내역사업 구분
- 테이블 카테고리 자동 분류

### 2. `normalize_government_standard.py`
JSON을 정부 표준 CSV로 변환
- TB_PLAN_DATA 43개 컬럼 생성
- 예산/일정/성과 데이터 추출
- PLAN_ID 자동 매칭

### 3. `load_oracle_direct.py`
CSV를 Oracle DB에 적재
- **2개 DB 연결**:
  - BICS (읽기): 기존 데이터 매칭
  - BICS_DEV (쓰기): 신규 데이터 적재
- **자동 TB_PLAN_DATA 복사** (BICS → BICS_DEV)
- **PLAN_ID 매칭** (YEAR, BIZ_NM, DETAIL_BIZ_NM)
- **FK 제약조건 처리**
- **매칭 리포트 생성**

## 📊 데이터베이스 스키마

### TB_PLAN_DATA (메인 테이블)
```sql
PLAN_ID             CHAR(30) PRIMARY KEY
YEAR                NUMBER(4)
NUM                 NUMBER
NATION_ORGAN_NM     VARCHAR2(768)   -- 부처명
BIZ_NM              VARCHAR2(768)   -- 내역사업명
DETAIL_BIZ_NM       VARCHAR2(768)   -- 세부사업명
... (43개 컬럼)
```

### TB_PLAN_BUDGET (예산 상세)
```sql
BUDGET_ID           CHAR(30) PRIMARY KEY
PLAN_ID             CHAR(30) FK → TB_PLAN_DATA
BUDGET_YEAR         NUMBER(4)
CATEGORY            VARCHAR2(50)    -- 실적/계획
TOTAL_AMOUNT        NUMBER
GOV_AMOUNT          NUMBER
...
```

### TB_PLAN_SCHEDULE (일정 상세)
```sql
SCHEDULE_ID         CHAR(30) PRIMARY KEY
PLAN_ID             CHAR(30) FK → TB_PLAN_DATA
SCHEDULE_YEAR       NUMBER(4)
QUARTER             VARCHAR2(50)
TASK_NAME           VARCHAR2(768)
START_DATE          DATE
END_DATE            DATE
```

### TB_PLAN_PERFORMANCE (성과 상세)
```sql
PERFORMANCE_ID      CHAR(30) PRIMARY KEY
PLAN_ID             CHAR(30) FK → TB_PLAN_DATA
PERFORMANCE_YEAR    NUMBER(4)
PERFORMANCE_TYPE    VARCHAR2(100)   -- 특허/논문/인력양성
CATEGORY            VARCHAR2(200)
VALUE               NUMBER
```

### TB_PLAN_ACHIEVEMENTS (대표 성과)
```sql
ACHIEVEMENT_ID      CHAR(30) PRIMARY KEY
PLAN_ID             CHAR(30) FK → TB_PLAN_DATA
ACHIEVEMENT_YEAR    NUMBER(4)
ACHIEVEMENT_ORDER   NUMBER
DESCRIPTION         VARCHAR2(4000)
```


## 🔍 스마트 매칭 로직

### PLAN_ID 자동 매칭 알고리즘
1. **정규화 키 생성**: `(YEAR, 정규화된_BIZ_NM, 정규화된_DETAIL_BIZ_NM)`
   - 공백/특수문자 제거
   - 가운뎃점(·, ∙, ･, •) 통일
   
2. **매칭 전략** (순차 적용):
   ```
   ① 완전 일치 (BIZ ↔ BIZ, DETAIL ↔ DETAIL)
   ② 교차 일치 (BIZ ↔ DETAIL, DETAIL ↔ BIZ)
   ③ 부분 일치 (유사도 95점 이상)
   ④ 임시 ID 생성 (TEMP_YEAR_XXX)
   ```

3. **매칭 결과**:
   - ✅ **성공**: 기존 PLAN_ID 재사용
   - ⚠️ **부분 일치**: 유사도 점수와 함께 기록
   - ❌ **실패**: 임시 ID 부여 (수동 검토 필요)

### 매칭률
- **2020~2023년 통합**: 1,558건 중 **1,555건 매칭 (99.8%)**
- **임시 ID**: 3건 (0.2%)

## 💡 주요 특징

### ✨ 개선된 정규화 전략
- **다년도 데이터 통합**: 5개 파일 → 단일 CSV 세트
- **중복 제거**: 동일 PLAN_ID는 자동으로 병합
- **부처명 자동 추출**: 정규표현식 기반 패턴 매칭
- **집계 필드 자동 계산**: 총예산, 총성과 등

### 🚀 성능 최적화
- **배치 쿼리**: DB 조회 최소화
- **메모리 효율**: 스트리밍 방식 CSV 쓰기
- **로깅**: 상세한 진행 상황 추적

## 🐛 문제 해결

### Oracle 연결 오류
```bash
ORA-12541: TNS:no listener
```
**해결**: `.env` 파일의 DB 정보 확인 및 Oracle 서버 상태 점검

### 매칭률이 낮은 경우
```bash
# 정규화 로그 확인
grep "FAIL" normalized_output_government/normalize.log

# 임시 ID 레코드 확인
grep "TEMP_" normalized_output_government/TB_PLAN_DATA.csv
```
**해결**: DB의 기존 데이터와 JSON의 사업명 표기 차이 수동 매핑

### PDF 파싱 실패
```bash
# JSON 구조 확인
python -m json.tool output/2024년도_생명공학육성시행계획.json
```
**해결**: PDF 테이블 구조 변경 시 `extract_pdf_to_json.py` 로직 수정

## 📝 라이센스

MIT License

## 👥 기여

이슈 및 풀 리퀘스트 환영합니다!

---

**마지막 업데이트**: 2025-11-24  
**버전**: 2.0 (통합 정규화 지원)

**해결**: 자동으로 TB_PLAN_DATA가 BICS_DEV로 복사됩니다. 수동 확인:
```sql
SELECT COUNT(*) FROM BICS_DEV.TB_PLAN_DATA;
```

### 3. 중복 키 에러
```
ORA-00001: unique constraint violated
```
**해결**: Streamlit 사이드바에서 "DB 데이터 초기화" 버튼 클릭

### 4. 인코딩 에러
```
UnicodeDecodeError: 'cp949' codec can't decode
```
**해결**: `config.py`에서 `charset: "UTF8"` 확인

## 📈 처리 성능

### 테스트 환경
- CPU: Intel i7-9700K
- RAM: 32GB
- PDF: 15페이지, 35개 테이블

### 결과
- PDF → JSON: ~2초
- JSON → CSV: ~1초
- CSV → DB: ~3초
- **총 처리 시간: ~6초**

## 🔐 보안

### 비밀번호 관리
`config.py`는 `.gitignore`에 추가하세요:
```bash
echo "config.py" >> .gitignore
```

환경변수 사용 예시:
```python
import os

ORACLE_CONFIG = {
    "user": os.getenv("ORACLE_USER", "bics"),
    "password": os.getenv("ORACLE_PASSWORD"),
}
```

## 📝 로그

로그는 콘솔과 파일에 동시 저장됩니다:
```
2025-11-20 15:05:16 - INFO - ✅ 기존 DB에서 PLAN_ID 발견: 우수연구,생애기본연구 -> 20240007
2025-11-20 15:05:16 - INFO - ✅ TB_PLAN_BUDGET 적재 완료: 20건
2025-11-20 15:05:16 - INFO - ✅ TB_PLAN_SCHEDULE 적재 완료: 38건
2025-11-20 15:05:16 - INFO - ✅ TB_PLAN_PERFORMANCE 적재 완료: 75건
2025-11-20 15:05:16 - INFO - ✅ TB_PLAN_ACHIEVEMENTS 적재 완료: 8건
```

## 🤝 기여

버그 리포트 및 기능 제안은 이슈로 등록해주세요.

## 📄 라이선스

이 프로젝트는 MIT 라이선스를 따릅니다.

## 👥 개발자

- **개발**: MINANI
- 
## 🎉 버전 히스토리

### v1.0.0 (2025-11-20)
- ✅ PDF → JSON → CSV → Oracle DB 전체 파이프라인 완성
- ✅ 2개 DB 연결 구조 (BICS + BICS_DEV)
- ✅ 자동 PLAN_ID 매칭 (100%)
- ✅ TB_PLAN_DATA 자동 복사
- ✅ 하위 4개 테이블 완전 적재
- ✅ Streamlit 웹 UI
- ✅ 매칭 리포트 생성
- ✅ 특수문자 처리 (가운뎃점 등)
- ✅ FK 제약조건 자동 처리

---

**🚀 Happy Coding!**


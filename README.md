# 생명공학육성시행계획 PDF → Oracle DB 처리 시스템

정부 생명공학육성시행계획 PDF 파일을 자동으로 파싱하여 Oracle 데이터베이스에 적재하는 시스템입니다.

## 🎯 주요 기능

### 1. PDF 자동 파싱
- **pdfplumber** 기반 테이블 추출
- 페이지별 내역사업 자동 구분
- 3가지 카테고리 자동 분류 (overview, plan, performance)

### 2. 정부 표준 정규화
- **TB_PLAN_DATA**: 내역사업 메인 정보 (43개 컬럼)
- **TB_PLAN_BUDGET**: 연도별 예산 상세 (실적/계획 구분)
- **TB_PLAN_SCHEDULE**: 세부 일정
- **TB_PLAN_PERFORMANCE**: 성과 상세 (특허, 논문, 인력양성)
- **TB_PLAN_ACHIEVEMENTS**: 대표 성과

### 3. Oracle DB 자동 적재
- **2개 DB 연결 구조**
  - `BICS` (읽기): 기존 TB_PLAN_DATA 매칭용
  - `BICS_DEV` (쓰기): 하위 테이블 적재용
- **자동 PLAN_ID 매칭** (YEAR + BIZ_NM + DETAIL_BIZ_NM)
- **TB_PLAN_DATA 자동 복사** (BICS → BICS_DEV)
- **FK 제약조건 자동 처리**
- **매칭 리포트 생성** (성공/실패/차이점)

### 4. Streamlit 웹 UI
- 드래그 앤 드롭 파일 업로드
- 실시간 처리 진행률 표시
- CSV 데이터 미리보기 및 다운로드
- DB 통계 대시보드

## 📦 설치

### 1. 필수 요구사항
- Python 3.11+
- Oracle Database 11g+
- Oracle Instant Client

### 2. 패키지 설치
```bash
pip install -r requirements.txt
```

### 3. Oracle Instant Client 설정
Windows:
```bash
# Oracle Instant Client 다운로드 및 압축 해제
# 환경변수 설정
set PATH=%PATH%;C:\oracle\instantclient_21_3
```

### 4. 데이터베이스 설정
`config.py` 파일 수정:
```python
# BICS (읽기용) - 기존 TB_PLAN_DATA 매칭
ORACLE_CONFIG = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics",
    "password": "your_password",
    "charset": "UTF8"
}

# BICS_DEV (쓰기용) - 하위 테이블 적재
ORACLE_CONFIG_DEV = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics_dev",
    "password": "bics_dev",
    "charset": "UTF8"
}
```

## 🚀 사용 방법

### 1. 명령줄 실행
```bash
# 단일 파일 처리
python main.py input/2024년도_생명공학육성시행계획.pdf

# 전체 파이프라인 (input/ 폴더 내 모든 PDF)
python main.py

# DB 적재 건너뛰기
python main.py --skip-db
```

### 2. Streamlit 웹 UI 실행
```bash
streamlit run streamlit_app.py
```

브라우저에서 `http://localhost:8501` 접속

#### 사용 단계:
1. **PDF 업로드** 탭에서 파일 선택
2. **전체 파이프라인 실행** 버튼 클릭
3. **CSV 데이터** 탭에서 결과 확인
4. **DB 통계** 탭에서 적재 결과 확인

## 📁 프로젝트 구조

```
PythonProject/
├── main.py                              # 메인 실행 파일
├── streamlit_app.py                     # Streamlit 웹 UI
├── config.py                            # 설정 파일
├── extract_pdf_to_json.py               # PDF → JSON 변환
├── normalize_government_standard.py     # JSON → CSV 정규화
├── load_oracle_direct.py                # CSV → Oracle DB 적재
├── oracle_db_manager.py                 # Oracle 연결 관리
├── oracle_table_ddl.py                  # 테이블 DDL 정의
├── requirements.txt                     # Python 패키지 목록
├── README.md                            # 이 파일
│
├── input/                               # PDF 입력 폴더
├── output/                              # JSON 출력 폴더
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

## 🔍 매칭 로직

### PLAN_ID 자동 매칭
1. **키 생성**: `(YEAR, BIZ_NM, DETAIL_BIZ_NM)`
2. **특수문자 정규화**: 가운뎃점(·, ∙, ･) 제거
3. **기존 DB 조회**: BICS.TB_PLAN_DATA에서 검색
4. **매칭 결과**:
   - ✅ 성공: 기존 PLAN_ID 재사용
   - ❌ 실패: 신규 레코드로 표시

### 매칭 리포트
- `matching_report.csv`: 전체 매칭 결과
- `unmatched_records.csv`: 매칭 실패 목록
- `diff_report.csv`: 차이점 발견 목록

## ⚙️ 고급 설정

### 병렬 처리
`main.py`에서 워커 수 조정:
```bash
python main.py --workers 8
```

### 배치 처리
`batch_processor.py` 사용:
```python
from batch_processor import BatchPDFProcessor

processor = BatchPDFProcessor(
    input_dir="input/",
    output_dir="output/",
    max_workers=4
)
processor.process_all()
```

## 🐛 트러블슈팅

### 1. Oracle 연결 실패
```
ORA-12541: TNS:no listener
```
**해결**: Oracle 서버가 실행 중인지 확인

### 2. FK 제약조건 위반
```
ORA-02291: integrity constraint violated - parent key not found
```
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


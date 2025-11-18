# 생명공학육성시행계획 PDF → Oracle DB 자동화 시스템

## 📋 프로젝트 개요

본 프로젝트는 **생명공학육성시행계획 PDF 문서**를 자동으로 파싱하여 구조화된 데이터로 변환하고, Oracle 데이터베이스에 적재하는 전체 자동화 파이프라인입니다.

### 주요 기능
- ✅ PDF 문서를 JSON으로 자동 변환 (OpenAI GPT-4 Vision API 활용)
- ✅ JSON 데이터를 정부 표준 형식으로 정규화
- ✅ Oracle DB 테이블 구조에 맞춰 CSV 생성
- ✅ Oracle 데이터베이스에 자동 적재
- ✅ 배치 처리 지원 (여러 PDF 동시 처리)

---

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 가상환경 활성화
.venv\Scripts\activate

# 패키지 설치 (이미 되어 있음)
pip install -r requirements.txt
```

### 2. OpenAI API 키 설정

`.env` 파일을 생성하고 API 키를 입력:
```
OPENAI_API_KEY=your-api-key-here
```

### 3. 실행

```bash
# 전체 파이프라인 실행 (input 폴더의 모든 PDF 처리)
python main.py --batch

# 특정 PDF 파일 처리
python main.py input/2020년도_생명공학육성시행계획.pdf

# DB 적재 건너뛰기 (CSV만 생성)
python main.py --batch --skip-db
```

---

## 📂 프로젝트 구조

```
PythonProject/
├── input/                              # 📥 PDF 입력 폴더
│   ├── 2020년도 생명공학육성시행계획.pdf
│   ├── 2021년도 생명공학육성시행계획.pdf
│   └── ...
│
├── output/                             # 📤 JSON 중간 결과
│   ├── 2020년도 생명공학육성시행계획.json
│   └── ...
│
├── normalized_output_government/       # 📊 최종 CSV 출력
│   ├── TB_PLAN_MASTER.csv
│   ├── TB_PLAN_DETAIL.csv
│   ├── TB_PLAN_BUDGET.csv
│   ├── TB_PLAN_SCHEDULE.csv
│   ├── TB_PLAN_PERFORMANCE.csv
│   └── raw_data.csv (감사용)
│
├── main.py                             # 🎯 메인 실행 파일
├── extract_pdf_to_json.py              # 1단계: PDF → JSON 변환
├── normalize_government_standard.py    # 2단계: JSON → CSV 정규화
├── load_oracle_direct.py               # 3단계: CSV → Oracle DB 적재
├── oracle_db_manager.py                # Oracle 연결 관리
├── oracle_table_ddl.py                 # Oracle 테이블 DDL 정의
├── batch_processor.py                  # 배치 처리 로직
├── config.py                           # 설정 파일
└── requirements.txt                    # Python 패키지 목록
```

---

## 🔄 데이터 처리 파이프라인

### 전체 흐름도

```
┌─────────────────┐
│   PDF 문서      │  input/
│ (정부 시행계획)  │
└────────┬────────┘
         │
         ▼ (1단계: extract_pdf_to_json.py)
┌─────────────────┐
│   JSON 데이터   │  output/
│  (구조화된 원본) │
└────────┬────────┘
         │
         ▼ (2단계: normalize_government_standard.py)
┌─────────────────┐
│   CSV 파일      │  normalized_output_government/
│  (Oracle 스키마) │
└────────┬────────┘
         │
         ▼ (3단계: load_oracle_direct.py)
┌─────────────────┐
│   Oracle DB     │
│  (최종 저장소)   │
└─────────────────┘
```

### 1단계: PDF → JSON 변환 (`extract_pdf_to_json.py`)

**목적:** PDF 문서를 구조화된 JSON으로 변환

**기술:**
- OpenAI GPT-4 Vision API 사용
- 페이지별로 이미지로 변환 후 텍스트 추출
- 테이블 구조 인식 및 파싱

**출력 예시:**
```json
{
  "metadata": {
    "document_year": 2020,
    "title": "2020년도 생명공학육성시행계획"
  },
  "pages": [
    {
      "page_number": 1,
      "category": "overview",
      "sub_project": "신약개발",
      "full_text": "...",
      "tables": [...]
    }
  ]
}
```

### 2단계: JSON → CSV 정규화 (`normalize_government_standard.py`)

**목적:** JSON 데이터를 Oracle DB 테이블 구조에 맞춰 정규화

**처리 내용:**
- 내역사업(프로젝트) 식별 및 PLAN_ID 생성
- 사업개요, 예산, 일정, 성과 데이터 추출
- Oracle 스키마에 맞춰 CSV 생성

**주요 로직:**
```python
# PLAN_ID 생성: 년도 + 3자리 일련번호
PLAN_ID = f"{year}{seq:03d}"  # 예: 2020001, 2020002, ...

# 데이터 분류
- TB_PLAN_MASTER:      마스터 정보 (1개 사업당 1건)
- TB_PLAN_DETAIL:      상세 정보 (1:1)
- TB_PLAN_BUDGET:      예산 정보 (1:N, 연도별)
- TB_PLAN_SCHEDULE:    일정 정보 (1:N, 분기별)
- TB_PLAN_PERFORMANCE: 성과 정보 (1:N, 항목별)
```

### 3단계: CSV → Oracle DB 적재 (`load_oracle_direct.py`)

**목적:** CSV 데이터를 Oracle 데이터베이스에 적재

**처리 순서:**
1. 기존 데이터 TRUNCATE (중복 방지)
2. TB_PLAN_MASTER 적재 (부모 테이블)
3. TB_PLAN_DETAIL, BUDGET, SCHEDULE, PERFORMANCE 적재 (자식 테이블)
4. Foreign Key 자동 연결

---

## 🗄️ Oracle 데이터베이스 구조

### ERD (Entity Relationship Diagram)

```
┌──────────────────────┐
│   TB_PLAN_MASTER     │ ◄── 마스터 테이블 (핵심)
│ ──────────────────── │
│ PK: PLAN_ID          │
│     YEAR             │
│     NATION_ORGAN_NM  │
│     BIZ_NM           │
│     DETAIL_BIZ_NM    │
└──────────┬───────────┘
           │
           │ 1:1
           ├──────────────────►┌──────────────────────┐
           │                   │   TB_PLAN_DETAIL     │
           │                   │ ──────────────────── │
           │                   │ FK: PLAN_ID          │
           │                   │     BIZ_TYPE         │
           │                   │     REP_FLD          │
           │                   │     LAST_GOAL        │
           │                   │     BIZ_CONTENTS     │
           │                   └──────────────────────┘
           │
           │ 1:N
           ├──────────────────►┌──────────────────────┐
           │                   │   TB_PLAN_BUDGET     │
           │                   │ ──────────────────── │
           │                   │ FK: PLAN_ID          │
           │                   │     BUDGET_YEAR      │
           │                   │     TOTAL_AMOUNT     │
           │                   │     GOV_AMOUNT       │
           │                   └──────────────────────┘
           │
           │ 1:N
           ├──────────────────►┌──────────────────────┐
           │                   │  TB_PLAN_SCHEDULE    │
           │                   │ ──────────────────── │
           │                   │ FK: PLAN_ID          │
           │                   │     SCHEDULE_YEAR    │
           │                   │     QUARTER          │
           │                   │     TASK_NAME        │
           │                   └──────────────────────┘
           │
           │ 1:N
           └──────────────────►┌──────────────────────┐
                               │ TB_PLAN_PERFORMANCE  │
                               │ ──────────────────── │
                               │ FK: PLAN_ID          │
                               │     PERFORMANCE_YEAR │
                               │     PERFORMANCE_TYPE │
                               │     VALUE            │
                               └──────────────────────┘
```

---

## 📊 CSV 파일 상세 설명

### 1️⃣ TB_PLAN_MASTER.csv (마스터 테이블)

**역할:** 각 사업(내역사업)의 기본 정보를 저장하는 핵심 테이블

| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| **PLAN_ID** | CHAR(30) | 🔑 시행계획 고유 ID (Primary Key) | 2020001 |
| **YEAR** | NUMBER(4) | 📅 계획 연도 | 2020 |
| **NUM** | NUMBER | 🔢 순번 | 1 |
| **NATION_ORGAN_NM** | VARCHAR2(768) | 🏛️ 부처명 | 과학기술정보통신부 |
| **BIZ_NM** | VARCHAR2(768) | 📋 세부사업명 | 바이오·의료기술개발사업 |
| **DETAIL_BIZ_NM** | VARCHAR2(768) | 📝 내역사업명 | 신약개발 |

**샘플 데이터:**
```csv
PLAN_ID,YEAR,NUM,NATION_ORGAN_NM,BIZ_NM,DETAIL_BIZ_NM
2020001,2020,1,과학기술정보통신부,바이오·의료기술개발사업,신약개발
2020002,2020,2,과학기술정보통신부,바이오·의료기술개발사업,차세대바이오
```

**활용:**
- 모든 테이블의 기준이 되는 마스터 데이터
- PLAN_ID로 다른 테이블과 연결
- 부처별, 사업별 검색의 기준

---

### 2️⃣ TB_PLAN_DETAIL.csv (상세 정보)

**역할:** 사업의 상세 정보 (목표, 내용, 관리기관 등)

| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| **DETAIL_ID** | CHAR(30) | 🔑 상세정보 ID | 2020001D01 |
| **PLAN_ID** | CHAR(30) | 🔗 시행계획 ID (Foreign Key) | 2020001 |
| **BIZ_TYPE** | VARCHAR2(768) | 📌 사업 유형 | 연구개발 |
| **REP_FLD** | VARCHAR2(768) | 🎯 대표 분야 | 신약 |
| **AREA** | VARCHAR2(768) | 🌐 3대 영역 | 레드바이오 |
| **LEAD_ORGAN_NM** | VARCHAR2(768) | 🏢 주관 기관명 | 한국보건산업진흥원 |
| **MNG_ORGAN_NM** | VARCHAR2(768) | 🏛️ 관리 기관명 | 보건복지부 |
| **BIZ_SDT** | DATE | 📆 사업 시작일 | 2020-01-01 |
| **BIZ_EDT** | DATE | 📆 사업 종료일 | 2024-12-31 |
| **RESPERIOD** | VARCHAR2(768) | ⏱️ 연구기간 | 5년 |
| **CUR_RESPERIOD** | VARCHAR2(768) | ⏱️ 현재 연구기간 | 3차년도 |
| **LAST_GOAL** | VARCHAR2(4000) | 🎯 최종 목표 | 혁신신약 후보물질 10건 발굴 |
| **BIZ_CONTENTS** | VARCHAR2(4000) | 📄 사업 내용 | 표적기반 신약 개발 연구... |
| **BIZ_CONTENTS_KEYWORD** | VARCHAR2(4000) | 🏷️ 키워드 | 신약, 임상, 후보물질 |

**관계:** TB_PLAN_MASTER와 **1:1 관계**

---

### 3️⃣ TB_PLAN_BUDGET.csv (예산 정보)

**역할:** 연도별 예산 집행 내역 (실적/계획 구분)

| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| **PLAN_ID** | CHAR(30) | 🔗 시행계획 ID | 2020001 |
| **BUDGET_YEAR** | NUMBER(4) | 📅 예산 연도 | 2020 |
| **CATEGORY** | VARCHAR2(50) | 📊 구분 | 실적 / 계획 |
| **TOTAL_AMOUNT** | NUMBER | 💰 총액 (백만원) | 55515.0 |
| **GOV_AMOUNT** | NUMBER | 🏛️ 정부 예산 | 55515.0 |
| **PRIVATE_AMOUNT** | NUMBER | 🏢 민간 예산 | 0 |
| **LOCAL_AMOUNT** | NUMBER | 🏘️ 지방비 | 0 |
| **ETC_AMOUNT** | NUMBER | 📦 기타 | 0 |
| **PERFORM_PRC** | NUMBER | ✅ 실적 비용 | 55515.0 |
| **PLAN_PRC** | NUMBER | 📋 계획 비용 | NULL |

**샘플 데이터:**
```csv
PLAN_ID,BUDGET_YEAR,CATEGORY,TOTAL_AMOUNT,GOV_AMOUNT,PRIVATE_AMOUNT
2020001,2017,실적,55515.0,55515.0,
2020001,2018,실적,44275.5,44275.5,
2020001,2019,실적,41963.0,41963.0,
2020001,2020,계획,50000.0,45000.0,5000.0
```

**관계:** TB_PLAN_MASTER와 **1:N 관계** (한 사업당 여러 연도 예산)

**특징:**
- 과거 연도는 "실적"
- 당해 연도는 "계획"
- 정부/민간/지방비/기타로 분리 집계

---

### 4️⃣ TB_PLAN_SCHEDULE.csv (일정 정보)

**역할:** 사업 추진 일정 (분기별 계획)

| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| **PLAN_ID** | CHAR(30) | 🔗 시행계획 ID | 2020001 |
| **SCHEDULE_YEAR** | NUMBER(4) | 📅 일정 연도 | 2020 |
| **QUARTER** | VARCHAR2(50) | 📊 분기 | 1/4분기 |
| **TASK_NAME** | VARCHAR2(768) | 📝 과제명 | 리더연구 |
| **TASK_CONTENT** | VARCHAR2(4000) | 📄 세부 내용 | • 리더연구\n- 신약 후보물질 발굴\n- 전임상 시험 |
| **START_DATE** | DATE | 📆 시작일 | 2020-01-01 |
| **END_DATE** | DATE | 📆 종료일 | 2020-03-31 |

**관계:** TB_PLAN_MASTER와 **1:N 관계** (한 사업당 여러 분기 일정)

**특징:**
- **실제 월 정보 우선 파싱** ✨
  - "1월~3월" → START_DATE: 2020-01-01, END_DATE: 2020-03-31
  - "4월~6월" → START_DATE: 2020-04-01, END_DATE: 2020-06-30
  - "2020.1 ~ 2020.5" → START_DATE: 2020-01-01, END_DATE: 2020-05-31
- **분기 정보로 대체**
  - "1/4분기" → START_DATE: 2020-01-01, END_DATE: 2020-03-31
  - "1/4분기~2/4분기" → 각 분기별로 분리 저장
- 연중 계획은 QUARTER='연중', START_DATE: 2020-01-01, END_DATE: 2020-12-31
- 과제별로 상세 내용 저장

---

### 5️⃣ TB_PLAN_PERFORMANCE.csv (성과 정보)

**역할:** 사업 성과 실적 (특허, 논문, 인력 등)

| 컬럼명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| **PLAN_ID** | CHAR(30) | 🔗 시행계획 ID | 2020001 |
| **PERFORMANCE_YEAR** | NUMBER(4) | 📅 실적 연도 | 2019 |
| **PERFORMANCE_TYPE** | VARCHAR2(100) | 📊 성과 유형 | 특허 / 논문 / 인력양성 |
| **CATEGORY** | VARCHAR2(200) | 🏷️ 세부 항목 | 국내출원 / SCIE / 박사 |
| **VALUE** | NUMBER | 🔢 실적 값 | 15 |
| **UNIT** | VARCHAR2(50) | 📏 단위 | 건 / 편 / 명 |
| **ORIGINAL_TEXT** | VARCHAR2(4000) | 📝 원본 텍스트 | [추적용 원본 데이터] |

**성과 유형 분류:**

**1) 특허 (PERFORMANCE_TYPE = '특허')**
- 국내출원
- 국내등록
- 국외출원
- 국외등록

**2) 논문 (PERFORMANCE_TYPE = '논문')**
- IF20이상
- IF10이상
- SCIE
- 비SCIE

**3) 인력양성 (PERFORMANCE_TYPE = '인력양성')**
- 박사
- 석사
- 학사
- 기타

**4) 정성적 실적 (PERFORMANCE_TYPE = '정성적실적')** ✨
- CATEGORY: '추진실적'
- VALUE: NULL (정량값 없음)
- ORIGINAL_TEXT: 실적 내용 전체 (예: "신약 후보물질 3건 발굴 및 전임상 진행 중")
- **용도:** 텍스트 기반 성과 추적, 정량화할 수 없는 실적 기록

**샘플 데이터:**
```csv
PLAN_ID,PERFORMANCE_YEAR,PERFORMANCE_TYPE,CATEGORY,VALUE,UNIT
2020001,2019,특허,국내출원,15,건
2020001,2019,특허,국내등록,8,건
2020001,2019,논문,SCIE,23,편
2020001,2019,인력양성,박사,5,명
```

**관계:** TB_PLAN_MASTER와 **1:N 관계** (한 사업당 여러 성과 항목)

**특징:**
- ORIGINAL_TEXT에 원본 데이터 저장 (검증 및 추적용)
- 연도별, 유형별, 세부항목별로 세분화
- 숫자형 VALUE로 통계 분석 가능

---

### 6️⃣ raw_data.csv (원본 데이터 - 감사용)

**역할:** JSON에서 추출한 원본 데이터를 그대로 보관 (감사 추적용)

| 컬럼명 | 설명 |
|--------|------|
| id | 레코드 ID |
| data_type | 데이터 유형 (overview, performance, schedule, budget) |
| data_year | 데이터 연도 |
| raw_content | 원본 JSON 텍스트 |
| page_number | PDF 페이지 번호 |
| table_index | 테이블 인덱스 |
| created_at | 생성 시각 |

**특징:**
- Oracle DB에는 적재하지 않음 (CSV만 보관)
- 데이터 검증 및 문제 추적용
- 원본 PDF와 매핑 가능

---

## 🔍 데이터 검색 및 활용 예시

### SQL 쿼리 예시

```sql
-- 1. 부처별 사업 목록 조회
SELECT YEAR, DETAIL_BIZ_NM, BIZ_NM
FROM TB_PLAN_MASTER
WHERE NATION_ORGAN_NM = '과학기술정보통신부'
ORDER BY YEAR, NUM;

-- 2. 특정 사업의 연도별 예산 추이
SELECT BUDGET_YEAR, CATEGORY, TOTAL_AMOUNT, GOV_AMOUNT
FROM TB_PLAN_BUDGET
WHERE PLAN_ID = '2020001'
ORDER BY BUDGET_YEAR;

-- 3. 2020년 사업의 성과 집계 (특허)
SELECT m.DETAIL_BIZ_NM, p.CATEGORY, SUM(p.VALUE) as 총건수
FROM TB_PLAN_PERFORMANCE p
JOIN TB_PLAN_MASTER m ON p.PLAN_ID = m.PLAN_ID
WHERE m.YEAR = 2020 AND p.PERFORMANCE_TYPE = '특허'
GROUP BY m.DETAIL_BIZ_NM, p.CATEGORY;

-- 4. 사업별 일정 조회
SELECT s.SCHEDULE_YEAR, s.QUARTER, s.TASK_NAME, s.TASK_CONTENT
FROM TB_PLAN_SCHEDULE s
JOIN TB_PLAN_MASTER m ON s.PLAN_ID = m.PLAN_ID
WHERE m.DETAIL_BIZ_NM = '신약개발'
ORDER BY s.SCHEDULE_YEAR, s.QUARTER;

-- 5. 사업 상세 정보와 함께 조회 (JOIN)
SELECT 
    m.PLAN_ID,
    m.YEAR,
    m.DETAIL_BIZ_NM,
    d.LAST_GOAL,
    d.BIZ_CONTENTS,
    d.LEAD_ORGAN_NM
FROM TB_PLAN_MASTER m
LEFT JOIN TB_PLAN_DETAIL d ON m.PLAN_ID = d.PLAN_ID
WHERE m.YEAR = 2020;
```

---

## ⚙️ 주요 Python 파일 설명

### 1. `main.py` - 전체 파이프라인 제어

```python
class PDFtoDBPipeline:
    def run(self):
        # 1. PDF → JSON
        self.extract_pdfs_to_json()
        
        # 2. JSON → CSV
        self.normalize_json_to_csv()
        
        # 3. CSV → Oracle DB
        self.load_to_database()
```

**주요 기능:**
- 전체 파이프라인 오케스트레이션
- 에러 핸들링 및 통계 수집
- 배치 처리 지원

---

### 2. `extract_pdf_to_json.py` - PDF 파싱

**핵심 로직:**
```python
def extract_pdf_to_json(pdf_path, output_path):
    # PDF��� 페이지별 이미지로 변환
    images = convert_pdf_to_images(pdf_path)
    
    # OpenAI Vision API로 텍스트 추출
    for page_num, image in enumerate(images):
        text = call_openai_vision_api(image)
        pages.append({
            'page_number': page_num,
            'text': text,
            'tables': extract_tables(text)
        })
    
    # JSON으로 저장
    save_as_json(output_path, pages)
```

---

### 3. `normalize_government_standard.py` - 데이터 정규화

**핵심 클래스:**
```python
class GovernmentStandardNormalizer:
    def normalize(self, json_data):
        # 내역사업 식별 및 PLAN_ID 생성
        for page in json_data['pages']:
            if '내역사업명' in page:
                plan_id = self.create_plan_id()
                
        # 데이터 분류 및 정규화
        self.extract_overview()      # → TB_PLAN_DETAIL
        self.extract_budget()        # → TB_PLAN_BUDGET
        self.extract_schedule()      # → TB_PLAN_SCHEDULE
        self.extract_performance()   # → TB_PLAN_PERFORMANCE
        
    def save_to_csv(self):
        # Oracle 스키마에 맞춰 CSV 저장
```

---

### 4. `load_oracle_direct.py` - DB 적재

**핵심 로직:**
```python
class OracleDirectLoader:
    def load_all_tables(self):
        # 기존 데이터 삭제 (중복 방지)
        self.truncate_tables()
        
        # 순서대로 적재 (FK 관계 고려)
        self.load_tb_plan_master()      # 1. 마스터
        self.load_tb_plan_detail()      # 2. 상세
        self.load_tb_plan_budget()      # 3. 예산
        self.load_tb_plan_schedule()    # 4. 일정
        self.load_tb_plan_performance() # 5. 성과
```

---

### 5. `config.py` - 설정 관리

```python
# Oracle 접속 정보
ORACLE_CONFIG = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics_dev",
    "password": "bics_dev"
}

# 디렉토리 설정
INPUT_DIR = "input/"
OUTPUT_DIR = "output/"
NORMALIZED_OUTPUT_GOVERNMENT_DIR = "normalized_output_government/"
```

---

## 🛠️ 문제 해결 (Troubleshooting)

### 1. `ORA-00001: unique constraint violated`

**원인:** DB에 이미 같은 PLAN_ID 데이터가 존재

**해결:**
```python
# load_oracle_direct.py에서 자동으로 TRUNCATE 수행
# 재실행하면 자동 해결됨
python main.py --batch
```

---

### 2. OpenAI API 에러

**증상:** `openai.error.AuthenticationError`

**해결:**
1. `.env` 파일에 올바른 API 키 입력
2. API 키 유효성 확인
3. 크레딧 잔액 확인

---

### 3. Oracle 연결 실패

**증상:** `ORA-12541: TNS:no listener`

**해결:**
1. Oracle 서버 실행 확인
2. `config.py`에서 host/port 확인
3. 네트워크 연결 확인
4. 방화벽 설정 확인

---

### 4. CSV 인코딩 문제

**증상:** 한글이 깨져 보임

**해결:**
- 모든 CSV는 **UTF-8 BOM** 인코딩으로 저장됨
- Excel에서 열 때: 데이터 → 텍스트 나누기 → UTF-8 선택

---

## 📈 성능 최적화

### 배치 처리

```bash
# 10개씩 묶어서 병렬 처리 (4개 워커)
python main.py --batch --batch-size 10 --max-workers 4
```

### DB 인덱스

테이블에 자동으로 인덱스가 생성되어 검색 성능 최적화:
- `IDX_MASTER_YEAR`: 연도별 검색
- `IDX_MASTER_DEPT`: 부처별 검색
- `IDX_BUDGET_YEAR`: 예산 연도별 검색
- `IDX_PERFORMANCE_TYPE`: 성과 유형별 검색

---

## 📝 데이터 품질 보장

### 검증 항목

1. **PLAN_ID 중복 없음**
   - 년도 + 일련번호로 고유성 보장

2. **Foreign Key 무결성**
   - 모든 자식 테이블의 PLAN_ID가 마스터에 존재

3. **필수 컬럼 NOT NULL**
   - NATION_ORGAN_NM, BIZ_NM은 필수

4. **데이터 타입 검증**
   - 금액은 NUMBER, 날짜는 DATE 형식

---

## 🎯 향후 개선 사항

- [ ] 웹 대시보드 추가 (Streamlit)
- [ ] 데이터 검증 자동화
- [ ] 증분 업데이트 지원 (전체 TRUNCATE 대신)
- [ ] 로깅 고도화 (파일 로그)
- [ ] 단위 테스트 추가

---

## 📞 문의

프로젝트 관련 문의사항이 있으시면 이슈를 등록해주세요.

---

## 📄 라이선스

이 프로젝트는 내부 사용 목적으로 제작되었습니다.

---

**마지막 업데이트:** 2025-11-18


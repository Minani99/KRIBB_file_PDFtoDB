"""
프로젝트 설정 파일
모든 경로 및 데이터베이스 설정을 중앙 관리
"""
from pathlib import Path

# ==================== 프로젝트 경로 설정 ====================
PROJECT_ROOT = Path(__file__).parent
INPUT_DIR = PROJECT_ROOT / "input"
OUTPUT_DIR = PROJECT_ROOT / "output"
NORMALIZED_OUTPUT_DIR = PROJECT_ROOT / "normalized_output_proper"

# 디렉토리 자동 생성
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
NORMALIZED_OUTPUT_DIR.mkdir(exist_ok=True)


# ==================== MySQL 데이터베이스 설정 ====================
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "0731",
    "database": "government_standard",
    "port": 3306,
    "charset": "utf8mb4"
}


# ==================== Oracle 데이터베이스 설정 ====================
ORACLE_CONFIG = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics_dev",
    "password": "bics_dev",
    "charset": "UTF8"
}


# ==================== 테이블 정의 ====================
# 테이블 생성 순서 (Foreign Key 의존성 고려)
TABLE_ORDER = [
    "document_metadata",    # 1. 문서 메타데이터 (독립)
    "detail_projects",      # 2. 세부사업 (독립)
    "sub_projects",         # 3. 내역사업 (detail_projects 참조)
    "sub_project_programs", # 4. 세부 프로그램 (sub_projects 참조)
    "budgets",              # 5. 예산 (sub_projects 참조)
    "performances",         # 6. 성과 (sub_projects 참조)
    "schedules",            # 7. 일정 (sub_projects 참조)
    "raw_tables"            # 8. 원본 테이블 (sub_projects 참조)
]


# ==================== CSV 파일 매핑 ====================
CSV_TABLE_MAPPING = {
    "document_metadata.csv": "document_metadata",
    "detail_projects.csv": "detail_projects",
    "sub_projects.csv": "sub_projects",
    "sub_project_programs.csv": "sub_project_programs",
    "budgets.csv": "budgets",
    "performances.csv": "performances",
    "schedules.csv": "schedules",
    "raw_tables.csv": "raw_tables"
}


# ==================== 로깅 설정 ====================
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"


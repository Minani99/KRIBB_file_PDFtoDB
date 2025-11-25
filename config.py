"""
프로젝트 설정 파일
모든 경로 및 데이터베이스 설정을 중앙 관리
"""
from pathlib import Path

# ==================== 프로젝트 경로 설정 ====================
PROJECT_ROOT = Path(__file__).parent
INPUT_DIR = PROJECT_ROOT / "input"
OUTPUT_DIR = PROJECT_ROOT / "output"
NORMALIZED_OUTPUT_GOVERNMENT_DIR = PROJECT_ROOT / "normalized_output_government"

# 필요한 디렉토리만 자동 생성
INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)
NORMALIZED_OUTPUT_GOVERNMENT_DIR.mkdir(exist_ok=True)


# ==================== Oracle 데이터베이스 설정 ====================
# 개발/테스트용 BICS_DEV 계정 (CSV 적재용)
ORACLE_CONFIG = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics_dev",
    "password": "bics_dev",
    "charset": "UTF8"
}

# BICS 개발 스키마 (기존 TB_PLAN_DATA 조회용)
ORACLE_CONFIG_BICS = {
    "host": "192.168.73.208",
    "port": 1521,
    "sid": "bics",
    "user": "bics",
    "password": "!@Bicsmain!@#",
    "charset": "UTF8"
}


"""
Oracle 스키마 설정 파일
회사 실제 스키마에 맞게 수정하여 사용
"""
from typing import Dict, List, Any

# =====================================
# 데이터베이스 연결 설정
# =====================================
ORACLE_CONFIG = {
    'host': 'your_oracle_host',  # 실제 호스트 주소
    'port': 1521,
    'sid': 'your_sid',  # 또는 service_name
    'user': 'your_username',
    'password': 'your_password'
}

# =====================================
# 테이블 처리 설정
# =====================================
# 기존 테이블 사용 여부
USE_EXISTING_TABLES = True  # True: 기존 테이블 사용, False: 재생성

# TRUNCATE 실행 여부 (USE_EXISTING_TABLES=True일 때만)
TRUNCATE_BEFORE_LOAD = False  # True: 데이터 삭제 후 적재, False: MERGE 사용

# 임시 테이블 사용 여부
USE_TEMP_TABLES = False  # True: _TEMP 테이블에 먼저 적재
MERGE_TO_PRODUCTION = False  # True: 임시→운영 MERGE

# =====================================
# 데이터 파일 경로
# =====================================
DATA_DIR = "data"  # CSV 및 JSON 파일 디렉토리

# CSV 파일명
CSV_FILES = {
    'sub_projects': 'sub_projects.csv',
    'overviews': 'normalized_overviews.csv',
    'budgets': 'normalized_budgets.csv',
    'schedules': 'normalized_schedules.csv',
    'performances': 'normalized_performances.csv',
    'achievements': 'key_achievements.csv',
    'details': 'plan_details.csv'
}

# JSON 파일명 (추가 데이터용)
JSON_FILES = {
    'project_details': 'project_details.json',  # 3대영역, 가중치, 키워드 등
    'mapping_data': 'plan_id_mapping.json'  # PLAN_ID 매핑 캐시
}

# =====================================
# 컬럼 타입 매핑 (회사 스키마에 맞게 수정)
# =====================================
COLUMN_TYPES = {
    'TB_PLAN_DATA': {
        # 금액 관련 컬럼 타입
        'TOTAL_RESPRC': 'VARCHAR2',  # 또는 'NUMBER'
        'TOTAL_RESPRC_GOV': 'NUMBER',
        'TOTAL_RESPRC_CIV': 'NUMBER',
        'CUR_RESPRC': 'VARCHAR2',  # 또는 'NUMBER'
        'CUR_RESPRC_GOV': 'NUMBER',
        'CUR_RESPRC_CIV': 'NUMBER',
        'PERFORM_PRC': 'NUMBER',
        'PLAN_PRC': 'NUMBER',
        
        # 가중치 관련 컬럼
        'BIOLOGY_WEI': 'NUMBER',
        'RED_WEI': 'NUMBER',
        'GREEN_WEI': 'NUMBER',
        'WHITE_WEI': 'NUMBER',
        'FUSION_WEI': 'NUMBER',
        'REGUL_WEI': 'NUMBER',
        'WEI': 'VARCHAR2',
        
        # 문자열 컬럼 길이
        'AREA': 768,
        'BIZ_CONTENTS_KEYWORD': 4000,
        'RESPERIOD': 768,
        'CUR_RESPERIOD': 768
    }
}

# =====================================
# 필수 컬럼 목록 (NULL 체크용)
# =====================================
REQUIRED_COLUMNS = {
    'TB_PLAN_DATA': [
        'PLAN_ID', 'YEAR', 'NUM', 'NATION_ORGAN_NM', 
        'DETAIL_BIZ_NM', 'BIZ_NM'
    ],
    'TB_PLAN_SCHEDULES': ['PLAN_ID'],
    'TB_PLAN_PERFORMANCES': ['PLAN_ID'],
    'TB_PLAN_BUDGETS': ['PLAN_ID'],
    'TB_PLAN_ACHIEVEMENTS': ['PLAN_ID'],
    'TB_PLAN_DETAILS': ['PLAN_ID']
}

# =====================================
# 3대영역 매핑
# =====================================
AREA_MAPPING = {
    '레드': '레드바이오',
    'RED': '레드바이오',
    '그린': '그린바이오',
    'GREEN': '그린바이오',
    '화이트': '화이트바이오',
    'WHITE': '화이트바이오',
    '융합': '융합바이오',
    'FUSION': '융합바이오'
}

# =====================================
# 예산 타입 매핑 (인코딩 문제 처리)
# =====================================
BUDGET_TYPE_MAPPING = {
    '정부': '정부',
    '민간': '민간',
    '민감': '민간',  # 인코딩 오류 처리
    '��간': '민간',  # 깨진 문자 처리
    '지방비': '지방비',
    '기타': '기타'
}

# =====================================
# 배치 처리 설정
# =====================================
BATCH_SIZE = 100  # 한 번에 처리할 레코드 수
COMMIT_INTERVAL = 1000  # 커밋 주기

# =====================================
# 로깅 설정
# =====================================
LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR
LOG_FILE = 'oracle_load.log'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# =====================================
# 에러 처리 설정
# =====================================
SKIP_ON_ERROR = True  # 에러 발생 시 해당 레코드만 스킵
MAX_ERROR_COUNT = 100  # 최대 허용 에러 수
ROLLBACK_ON_CRITICAL = True  # 치명적 에러 시 전체 롤백

# =====================================
# 검증 설정
# =====================================
VALIDATE_AFTER_LOAD = True  # 적재 후 자동 검증
VALIDATION_QUERIES = [
    """
    -- 테이블별 레코드 수 확인
    SELECT 'TB_PLAN_DATA' AS TABLE_NAME, COUNT(*) AS CNT FROM TB_PLAN_DATA
    UNION ALL
    SELECT 'TB_PLAN_SCHEDULES', COUNT(*) FROM TB_PLAN_SCHEDULES
    """,
    """
    -- NULL 값 체크
    SELECT 
        COUNT(*) AS TOTAL_RECORDS,
        SUM(CASE WHEN AREA IS NULL THEN 1 ELSE 0 END) AS AREA_NULL
    FROM TB_PLAN_DATA
    """
]

# =====================================
# 추가 데이터 소스 설정
# =====================================
# 3대영역, 가중치, 키워드 데이터를 가져올 소스
ADDITIONAL_DATA_SOURCES = {
    'area': 'project_details.json',  # 3대영역
    'weights': 'project_details.json',  # 가중치
    'keywords': 'project_details.json',  # 키워드
    'research_period': 'project_schedules.json'  # 연구기간
}

# =====================================
# 테스트 모드 설정
# =====================================
TEST_MODE = False  # True: 테스트 모드 (실제 DB 변경 없음)
TEST_LIMIT = 10  # 테스트 모드에서 처리할 최대 레코드 수

# =====================================
# 백업 설정
# =====================================
BACKUP_BEFORE_LOAD = False  # 적재 전 자동 백업
BACKUP_DIR = 'backups'  # 백업 파일 저장 디렉토리
BACKUP_FORMAT = 'DUMP'  # DUMP, CSV, JSON

# =====================================
# 문자 인코딩 설정
# =====================================
import os
os.environ['NLS_LANG'] = 'KOREAN_KOREA.AL32UTF8'  # Oracle 한글 처리
CSV_ENCODING = 'utf-8-sig'  # CSV 파일 인코딩
JSON_ENCODING = 'utf-8'  # JSON 파일 인코딩

# =====================================
# 성능 최적화 설정
# =====================================
PARALLEL_PROCESSING = False  # 병렬 처리 사용
PARALLEL_WORKERS = 4  # 병렬 처리 워커 수
DISABLE_INDEXES_DURING_LOAD = False  # 적재 중 인덱스 비활성화
USE_DIRECT_PATH_INSERT = False  # Direct Path Insert 사용

# =====================================
# 알림 설정
# =====================================
SEND_NOTIFICATIONS = False  # 알림 전송 여부
NOTIFICATION_EMAIL = 'admin@company.com'  # 알림 이메일
NOTIFICATION_EVENTS = ['ERROR', 'COMPLETE']  # 알림 이벤트
"""
Oracle 데이터베이스 연결 및 관리 모듈 - 개선 버전
주요 개선사항:
1. 상세 로깅 (테이블명 포함)
2. TRUNCATE TABLE 지원
3. PLAN_ID lookup 기능
4. 트랜잭션 관리 개선
"""
import oracledb
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class OracleDBManager:
    """Oracle 데이터베이스 관리 클래스 - 개선 버전"""

    def __init__(self, db_config: Dict[str, Any]):
        """
        Args:
            db_config: Oracle 연결 설정
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        
        # 트랜잭션 추적
        self.in_transaction = False
        self.transaction_count = 0

    def connect(self):
        """Oracle 데이터베이스 연결"""
        try:
            # DSN 생성
            dsn = oracledb.makedsn(
                self.db_config['host'],
                self.db_config['port'],
                sid=self.db_config.get('sid')
            )

            # 연결
            self.connection = oracledb.connect(
                user=self.db_config['user'],
                password=self.db_config['password'],
                dsn=dsn
            )

            self.cursor = self.connection.cursor()
            
            # 연결 정보 로깅
            logger.info(f"✅ Oracle 데이터베이스 연결 성공")
            logger.info(f"   - Host: {self.db_config['host']}:{self.db_config['port']}")
            logger.info(f"   - SID: {self.db_config.get('sid')}")
            logger.info(f"   - User: {self.db_config['user']}")

            return True

        except oracledb.Error as error:
            logger.error(f"❌ Oracle 연결 실패: {error}")
            logger.error(f"   - DSN: {dsn if 'dsn' in locals() else 'N/A'}")
            raise

    def execute_ddl(self, ddl: str, table_name: str = None):
        """DDL 실행 (테이블 생성 등) - 개선"""
        try:
            self.cursor.execute(ddl)
            
            # DDL 타입 추출
            ddl_type = "DDL"
            ddl_upper = ddl.upper().strip()
            if ddl_upper.startswith("CREATE TABLE"):
                ddl_type = "CREATE TABLE"
            elif ddl_upper.startswith("DROP TABLE"):
                ddl_type = "DROP TABLE"
            elif ddl_upper.startswith("ALTER TABLE"):
                ddl_type = "ALTER TABLE"
            elif ddl_upper.startswith("CREATE INDEX"):
                ddl_type = "CREATE INDEX"
            elif ddl_upper.startswith("COMMENT ON"):
                ddl_type = "COMMENT"
            
            if table_name:
                logger.info(f"✅ {ddl_type} 실행 완료: {table_name}")
            else:
                # DDL에서 테이블명 추출 시도
                if "TABLE" in ddl_upper:
                    import re
                    match = re.search(r'TABLE\s+([^\s(]+)', ddl_upper)
                    if match:
                        table_name = match.group(1)
                        logger.info(f"✅ {ddl_type} 실행 완료: {table_name}")
                    else:
                        logger.info(f"✅ {ddl_type} 실행 완료")
                else:
                    logger.info(f"✅ {ddl_type} 실행 완료")
                    
        except oracledb.Error as error:
            logger.error(f"❌ DDL 실행 실패: {error}")
            if table_name:
                logger.error(f"   - 테이블: {table_name}")
            logger.error(f"   - DDL: {ddl[:200]}...")  # DDL 일부만 로깅
            raise

    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[tuple]:
        """쿼리 실행 및 결과 반환"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            return self.cursor.fetchall()

        except oracledb.Error as error:
            logger.error(f"❌ 쿼리 실행 실패: {error}")
            logger.error(f"   - Query: {query[:200]}...")
            if params:
                logger.error(f"   - Params: {params}")
            raise

    def execute_many(self, query: str, data: List[tuple], table_name: str = None):
        """배치 INSERT - 개선"""
        try:
            self.cursor.executemany(query, data)
            self.connection.commit()
            
            if table_name:
                logger.info(f"✅ {table_name}: {len(data)}건 배치 삽입 완료")
            else:
                logger.info(f"✅ {len(data)}건 배치 삽입 완료")
                
        except oracledb.Error as error:
            logger.error(f"❌ 배치 삽입 실패: {error}")
            if table_name:
                logger.error(f"   - 테이블: {table_name}")
            logger.error(f"   - 데이터 건수: {len(data)}")
            self.connection.rollback()
            raise

    def truncate_table(self, table_name: str):
        """테이블 TRUNCATE (데이터만 삭제)"""
        try:
            # TRUNCATE는 DDL이므로 자동 커밋됨
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            logger.info(f"✅ {table_name} 테이블 TRUNCATE 완료")
        except oracledb.Error as error:
            if "ORA-00942" in str(error):
                logger.warning(f"⚠️ {table_name} 테이블이 존재하지 않습니다")
            else:
                logger.error(f"❌ {table_name} 테이블 TRUNCATE 실패: {error}")
                raise

    def begin_transaction(self):
        """명시적 트랜잭션 시작"""
        self.in_transaction = True
        self.transaction_count = 0
        logger.debug("🔄 트랜잭션 시작")

    def commit(self):
        """트랜잭션 커밋 - 개선"""
        if self.connection:
            self.connection.commit()
            if self.in_transaction:
                logger.debug(f"✅ 트랜잭션 커밋 (변경: {self.transaction_count}건)")
                self.in_transaction = False
                self.transaction_count = 0

    def rollback(self):
        """트랜잭션 롤백 - 개선"""
        if self.connection:
            self.connection.rollback()
            if self.in_transaction:
                logger.warning(f"⚠️ 트랜잭션 롤백 (취소: {self.transaction_count}건)")
                self.in_transaction = False
                self.transaction_count = 0

    def close(self):
        """연결 종료"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔌 Oracle 연결 종료")

    def table_exists(self, table_name: str) -> bool:
        """테이블 존재 여부 확인"""
        query = """
            SELECT COUNT(*) 
            FROM USER_TABLES 
            WHERE TABLE_NAME = UPPER(:table_name)
        """
        result = self.execute_query(query, (table_name,))
        exists = result[0][0] > 0
        
        if exists:
            logger.debug(f"✅ {table_name} 테이블 존재함")
        else:
            logger.debug(f"❌ {table_name} 테이블 없음")
            
        return exists

    def drop_table(self, table_name: str, cascade: bool = True):
        """테이블 삭제"""
        try:
            if cascade:
                self.cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
            else:
                self.cursor.execute(f"DROP TABLE {table_name}")
            logger.info(f"✅ {table_name} 테이블 삭제 완료")
        except oracledb.Error as error:
            # 테이블이 없으면 무시
            if "ORA-00942" not in str(error):
                logger.error(f"❌ {table_name} 테이블 삭제 실패: {error}")

    def get_table_count(self, table_name: str) -> int:
        """테이블 레코드 수 조회"""
        if not self.table_exists(table_name):
            return 0
        
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute_query(query)
        count = result[0][0]
        
        logger.debug(f"📊 {table_name} 테이블: {count:,}건")
        return count

    def get_next_plan_id(self, year: int) -> str:
        """다음 PLAN_ID 생성"""
        query = """
            SELECT MAX(TO_NUMBER(SUBSTR(PLAN_ID, 5))) 
            FROM TB_PLAN_DATA 
            WHERE SUBSTR(PLAN_ID, 1, 4) = :year
        """
        result = self.execute_query(query, (str(year),))
        max_num = result[0][0]

        if max_num is None:
            next_num = 1
        else:
            next_num = max_num + 1

        # 2023001 형식으로 반환
        plan_id = f"{year}{next_num:03d}"
        logger.debug(f"📝 생성된 PLAN_ID: {plan_id}")
        return plan_id

    def plan_id_exists(self, plan_id: str) -> bool:
        """PLAN_ID 존재 여부 확인"""
        query = "SELECT COUNT(*) FROM TB_PLAN_DATA WHERE PLAN_ID = :plan_id"
        result = self.execute_query(query, (plan_id,))
        return result[0][0] > 0

    def lookup_plan_id(self, sub_project_id: int) -> Optional[str]:
        """
        sub_project_id로 PLAN_ID 조회 (DB에서 직접)
        이 기능은 테이블에 sub_project_id 컬럼이 있다고 가정
        """
        try:
            # TB_PLAN_DATA에 sub_project_id 컬럼이 있는 경우
            query = """
                SELECT PLAN_ID 
                FROM TB_PLAN_DATA 
                WHERE SUB_PROJECT_ID = :sub_project_id
            """
            result = self.execute_query(query, (sub_project_id,))
            if result:
                plan_id = result[0][0]
                logger.debug(f"✅ PLAN_ID 조회 성공: {sub_project_id} → {plan_id}")
                return plan_id
        except oracledb.Error:
            # 컬럼이 없거나 조회 실패
            pass
        
        # 대체 방법: DETAIL_BIZ_NM이나 다른 컬럼으로 매칭
        try:
            # sub_projects.csv의 sub_project_name과 매칭
            query = """
                SELECT PLAN_ID 
                FROM TB_PLAN_DATA 
                WHERE DETAIL_BIZ_NM = (
                    SELECT sub_project_name 
                    FROM sub_projects_temp 
                    WHERE id = :sub_project_id
                )
            """
            result = self.execute_query(query, (sub_project_id,))
            if result:
                return result[0][0]
        except:
            pass
        
        logger.debug(f"❌ PLAN_ID 조회 실패: sub_project_id={sub_project_id}")
        return None

    def get_column_info(self, table_name: str) -> List[Dict]:
        """테이블 컬럼 정보 조회"""
        query = """
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                DATA_LENGTH,
                NULLABLE,
                COLUMN_ID
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = UPPER(:table_name)
            ORDER BY COLUMN_ID
        """
        
        result = self.execute_query(query, (table_name,))
        
        columns = []
        for row in result:
            columns.append({
                'name': row[0],
                'type': row[1],
                'length': row[2],
                'nullable': row[3],
                'position': row[4]
            })
        
        logger.info(f"📋 {table_name} 테이블: {len(columns)}개 컬럼")
        return columns

    def validate_table_schema(self, table_name: str, required_columns: List[str]) -> bool:
        """테이블 스키마 검증"""
        columns = self.get_column_info(table_name)
        column_names = [col['name'] for col in columns]
        
        missing_columns = []
        for required_col in required_columns:
            if required_col.upper() not in column_names:
                missing_columns.append(required_col)
        
        if missing_columns:
            logger.warning(f"⚠️ {table_name} 테이블에 필수 컬럼 누락: {missing_columns}")
            return False
        
        logger.info(f"✅ {table_name} 테이블 스키마 검증 완료")
        return True

    def execute_merge(self, merge_query: str, table_name: str = None):
        """MERGE 문 실행 (UPSERT)"""
        try:
            self.cursor.execute(merge_query)
            rows_affected = self.cursor.rowcount
            self.connection.commit()
            
            if table_name:
                logger.info(f"✅ {table_name}: MERGE 완료 ({rows_affected}행 영향)")
            else:
                logger.info(f"✅ MERGE 완료 ({rows_affected}행 영향)")
                
        except oracledb.Error as error:
            logger.error(f"❌ MERGE 실행 실패: {error}")
            if table_name:
                logger.error(f"   - 테이블: {table_name}")
            self.connection.rollback()
            raise

    def get_db_info(self) -> Dict:
        """데이터베이스 정보 조회"""
        info = {}
        
        # DB 버전
        query = "SELECT * FROM v$version WHERE banner LIKE 'Oracle%'"
        result = self.execute_query(query)
        if result:
            info['version'] = result[0][0]
        
        # 현재 사용자
        query = "SELECT USER FROM DUAL"
        result = self.execute_query(query)
        info['current_user'] = result[0][0]
        
        # 현재 스키마
        query = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"
        result = self.execute_query(query)
        info['current_schema'] = result[0][0]
        
        # 테이블 수
        query = "SELECT COUNT(*) FROM USER_TABLES"
        result = self.execute_query(query)
        info['table_count'] = result[0][0]
        
        logger.info("📊 데이터베이스 정보:")
        for key, value in info.items():
            logger.info(f"   - {key}: {value}")
        
        return info
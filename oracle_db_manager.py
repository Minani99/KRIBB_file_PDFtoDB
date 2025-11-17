"""
Oracle 데이터베이스 연결 및 관리 모듈
"""
import oracledb
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class OracleDBManager:
    """Oracle 데이터베이스 관리 클래스"""

    def __init__(self, db_config: Dict[str, Any]):
        """
        Args:
            db_config: Oracle 연결 설정
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None

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
            logger.info("✅ Oracle 데이터베이스 연결 성공")

            return True

        except oracledb.Error as error:
            logger.error(f"❌ Oracle 연결 실패: {error}")
            raise

    def execute_ddl(self, ddl: str):
        """DDL 실행 (테이블 생성 등)"""
        try:
            self.cursor.execute(ddl)
            logger.info(f"✅ DDL 실행 완료")
        except oracledb.Error as error:
            logger.error(f"❌ DDL 실행 실패: {error}")
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
            raise

    def execute_many(self, query: str, data: List[tuple]):
        """배치 INSERT"""
        try:
            self.cursor.executemany(query, data)
            self.connection.commit()
            logger.info(f"✅ {len(data)}건 배치 삽입 완료")
        except oracledb.Error as error:
            logger.error(f"❌ 배치 삽입 실패: {error}")
            self.connection.rollback()
            raise

    def commit(self):
        """트랜잭션 커밋"""
        if self.connection:
            self.connection.commit()

    def rollback(self):
        """트랜잭션 롤백"""
        if self.connection:
            self.connection.rollback()

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
        return result[0][0] > 0

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
        return f"{year}{next_num:03d}"

    def plan_id_exists(self, plan_id: str) -> bool:
        """PLAN_ID 존재 여부 확인"""
        query = "SELECT COUNT(*) FROM TB_PLAN_DATA WHERE PLAN_ID = :plan_id"
        result = self.execute_query(query, (plan_id,))
        return result[0][0] > 0


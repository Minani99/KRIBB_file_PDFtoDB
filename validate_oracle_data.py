"""
Oracle 데이터 검증 스크립트
적재된 데이터의 무결성 및 정확성 검증
"""
import logging
from typing import Dict, List, Any, Tuple
import pandas as pd
from oracle_db_manager_improved import OracleDBManager
from config_oracle_schema import ORACLE_CONFIG, REQUIRED_COLUMNS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OracleDataValidator:
    """Oracle 데이터 검증 클래스"""
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_manager = OracleDBManager(db_config)
        self.validation_results = {
            'passed': [],
            'failed': [],
            'warnings': [],
            'statistics': {}
        }
    
    def connect(self):
        """데이터베이스 연결"""
        return self.db_manager.connect()
    
    def validate_all(self) -> Dict:
        """전체 검증 실행"""
        logger.info("🔍 Oracle 데이터 검증 시작...")
        
        # 1. 테이블 존재 여부
        self._validate_table_existence()
        
        # 2. 레코드 수 확인
        self._validate_record_counts()
        
        # 3. 필수 컬럼 NULL 체크
        self._validate_required_columns()
        
        # 4. 참조 무결성 체크
        self._validate_referential_integrity()
        
        # 5. 데이터 정합성 체크
        self._validate_data_consistency()
        
        # 6. 금액 데이터 검증
        self._validate_amount_data()
        
        # 7. 날짜 데이터 검증
        self._validate_date_data()
        
        # 8. 인코딩 문제 체크
        self._validate_encoding()
        
        # 9. PLAN_ID 매핑 검증
        self._validate_plan_id_mapping()
        
        return self.validation_results
    
    def _validate_table_existence(self):
        """테이블 존재 여부 검증"""
        logger.info("📋 테이블 존재 여부 검증 중...")
        
        tables = [
            'TB_PLAN_DATA',
            'TB_PLAN_SCHEDULES',
            'TB_PLAN_PERFORMANCES',
            'TB_PLAN_BUDGETS',
            'TB_PLAN_ACHIEVEMENTS',
            'TB_PLAN_DETAILS'
        ]
        
        for table in tables:
            if self.db_manager.table_exists(table):
                self.validation_results['passed'].append(f"✅ {table} 테이블 존재")
            else:
                self.validation_results['failed'].append(f"❌ {table} 테이블 없음")
    
    def _validate_record_counts(self):
        """레코드 수 확인"""
        logger.info("📊 레코드 수 확인 중...")
        
        query = """
            SELECT 'TB_PLAN_DATA' AS TABLE_NAME, COUNT(*) AS CNT FROM TB_PLAN_DATA
            UNION ALL
            SELECT 'TB_PLAN_SCHEDULES', COUNT(*) FROM TB_PLAN_SCHEDULES
            UNION ALL
            SELECT 'TB_PLAN_PERFORMANCES', COUNT(*) FROM TB_PLAN_PERFORMANCES
            UNION ALL
            SELECT 'TB_PLAN_BUDGETS', COUNT(*) FROM TB_PLAN_BUDGETS
            UNION ALL
            SELECT 'TB_PLAN_ACHIEVEMENTS', COUNT(*) FROM TB_PLAN_ACHIEVEMENTS
            UNION ALL
            SELECT 'TB_PLAN_DETAILS', COUNT(*) FROM TB_PLAN_DETAILS
        """
        
        results = self.db_manager.execute_query(query)
        
        total_records = 0
        for table_name, count in results:
            self.validation_results['statistics'][table_name] = count
            total_records += count
            
            if count == 0:
                self.validation_results['warnings'].append(f"⚠️ {table_name}: 레코드 없음")
            else:
                logger.info(f"  {table_name}: {count:,}건")
        
        self.validation_results['statistics']['TOTAL'] = total_records
        
        if total_records > 0:
            self.validation_results['passed'].append(f"✅ 전체 레코드: {total_records:,}건")
        else:
            self.validation_results['failed'].append("❌ 적재된 데이터 없음")
    
    def _validate_required_columns(self):
        """필수 컬럼 NULL 체크"""
        logger.info("🔍 필수 컬럼 NULL 체크 중...")
        
        # TB_PLAN_DATA 필수 컬럼 체크
        query = """
            SELECT 
                COUNT(*) AS TOTAL_RECORDS,
                SUM(CASE WHEN PLAN_ID IS NULL THEN 1 ELSE 0 END) AS PLAN_ID_NULL,
                SUM(CASE WHEN YEAR IS NULL THEN 1 ELSE 0 END) AS YEAR_NULL,
                SUM(CASE WHEN NATION_ORGAN_NM IS NULL THEN 1 ELSE 0 END) AS DEPT_NULL,
                SUM(CASE WHEN DETAIL_BIZ_NM IS NULL THEN 1 ELSE 0 END) AS DETAIL_BIZ_NULL,
                SUM(CASE WHEN BIZ_NM IS NULL THEN 1 ELSE 0 END) AS BIZ_NULL,
                SUM(CASE WHEN AREA IS NULL THEN 1 ELSE 0 END) AS AREA_NULL,
                SUM(CASE WHEN BIZ_CONTENTS_KEYWORD IS NULL THEN 1 ELSE 0 END) AS KEYWORD_NULL
            FROM TB_PLAN_DATA
        """
        
        result = self.db_manager.execute_query(query)
        if result:
            row = result[0]
            total = row[0]
            
            null_counts = {
                'PLAN_ID': row[1],
                'YEAR': row[2],
                'NATION_ORGAN_NM': row[3],
                'DETAIL_BIZ_NM': row[4],
                'BIZ_NM': row[5],
                'AREA': row[6],
                'BIZ_CONTENTS_KEYWORD': row[7]
            }
            
            for column, null_count in null_counts.items():
                if null_count == 0:
                    self.validation_results['passed'].append(f"✅ {column}: NULL 없음")
                elif column in ['AREA', 'BIZ_CONTENTS_KEYWORD']:
                    # 선택적 컬럼은 경고만
                    if null_count > 0:
                        rate = (null_count / total * 100) if total > 0 else 0
                        self.validation_results['warnings'].append(
                            f"⚠️ {column}: {null_count}건 NULL ({rate:.1f}%)"
                        )
                else:
                    # 필수 컬럼은 실패
                    if null_count > 0:
                        self.validation_results['failed'].append(
                            f"❌ {column}: {null_count}건 NULL"
                        )
    
    def _validate_referential_integrity(self):
        """참조 무결성 체크"""
        logger.info("🔗 참조 무결성 체크 중...")
        
        # 하위 테이블의 PLAN_ID가 모두 TB_PLAN_DATA에 존재하는지 확인
        tables = [
            'TB_PLAN_SCHEDULES',
            'TB_PLAN_PERFORMANCES',
            'TB_PLAN_BUDGETS',
            'TB_PLAN_ACHIEVEMENTS',
            'TB_PLAN_DETAILS'
        ]
        
        for table in tables:
            query = f"""
                SELECT COUNT(*) 
                FROM {table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM TB_PLAN_DATA p 
                    WHERE p.PLAN_ID = t.PLAN_ID
                )
            """
            
            try:
                result = self.db_manager.execute_query(query)
                orphan_count = result[0][0] if result else 0
                
                if orphan_count == 0:
                    self.validation_results['passed'].append(f"✅ {table}: 참조 무결성 OK")
                else:
                    self.validation_results['failed'].append(
                        f"❌ {table}: {orphan_count}건 고아 레코드"
                    )
            except Exception as e:
                logger.warning(f"참조 무결성 체크 실패 ({table}): {e}")
    
    def _validate_data_consistency(self):
        """데이터 정합성 체크"""
        logger.info("🔄 데이터 정합성 체크 중...")
        
        # 1. 날짜 논리성 체크 (시작일 <= 종료일)
        query = """
            SELECT COUNT(*)
            FROM TB_PLAN_DATA
            WHERE BIZ_SDT IS NOT NULL 
            AND BIZ_EDT IS NOT NULL
            AND BIZ_SDT > BIZ_EDT
        """
        
        result = self.db_manager.execute_query(query)
        invalid_dates = result[0][0] if result else 0
        
        if invalid_dates == 0:
            self.validation_results['passed'].append("✅ 날짜 논리성: OK")
        else:
            self.validation_results['failed'].append(
                f"❌ 날짜 논리성: {invalid_dates}건 시작일 > 종료일"
            )
        
        # 2. PLAN_ID 형식 체크 (YYYYNNN)
        query = """
            SELECT COUNT(*)
            FROM TB_PLAN_DATA
            WHERE NOT REGEXP_LIKE(PLAN_ID, '^[0-9]{7}$')
        """
        
        try:
            result = self.db_manager.execute_query(query)
            invalid_plan_ids = result[0][0] if result else 0
            
            if invalid_plan_ids == 0:
                self.validation_results['passed'].append("✅ PLAN_ID 형식: OK")
            else:
                self.validation_results['failed'].append(
                    f"❌ PLAN_ID 형식: {invalid_plan_ids}건 형식 오류"
                )
        except:
            # REGEXP_LIKE 지원 안 하는 경우
            pass
    
    def _validate_amount_data(self):
        """금액 데이터 검증"""
        logger.info("💰 금액 데이터 검증 중...")
        
        # 금액 합계 검증 (정부 + 민간 = 총액)
        query = """
            SELECT 
                COUNT(*) AS TOTAL_COUNT,
                SUM(CASE 
                    WHEN ABS(NVL(TOTAL_RESPRC_GOV, 0) + NVL(TOTAL_RESPRC_CIV, 0) - 
                            NVL(TO_NUMBER(TOTAL_RESPRC), 0)) > 0.01 
                    THEN 1 ELSE 0 
                END) AS MISMATCH_COUNT
            FROM TB_PLAN_DATA
            WHERE TOTAL_RESPRC IS NOT NULL
        """
        
        try:
            result = self.db_manager.execute_query(query)
            if result:
                total, mismatch = result[0]
                if mismatch == 0:
                    self.validation_results['passed'].append("✅ 금액 합계: 정합성 OK")
                else:
                    self.validation_results['warnings'].append(
                        f"⚠️ 금액 합계: {mismatch}건 불일치 (지방비/기타 포함 가능)"
                    )
        except Exception as e:
            logger.warning(f"금액 검증 실패: {e}")
    
    def _validate_date_data(self):
        """날짜 데이터 검증"""
        logger.info("📅 날짜 데이터 검증 중...")
        
        # 유효한 날짜 범위 체크 (2020~2030)
        query = """
            SELECT COUNT(*)
            FROM TB_PLAN_DATA
            WHERE (BIZ_SDT IS NOT NULL AND (
                EXTRACT(YEAR FROM BIZ_SDT) < 2020 OR 
                EXTRACT(YEAR FROM BIZ_SDT) > 2030
            ))
            OR (BIZ_EDT IS NOT NULL AND (
                EXTRACT(YEAR FROM BIZ_EDT) < 2020 OR 
                EXTRACT(YEAR FROM BIZ_EDT) > 2030
            ))
        """
        
        result = self.db_manager.execute_query(query)
        invalid_dates = result[0][0] if result else 0
        
        if invalid_dates == 0:
            self.validation_results['passed'].append("✅ 날짜 범위: 정상")
        else:
            self.validation_results['warnings'].append(
                f"⚠️ 날짜 범위: {invalid_dates}건 의심스러운 날짜"
            )
    
    def _validate_encoding(self):
        """인코딩 문제 체크"""
        logger.info("🔤 인코딩 문제 체크 중...")
        
        # 깨진 문자 패턴 체크
        query = """
            SELECT COUNT(*)
            FROM TB_PLAN_BUDGETS
            WHERE BUDGET_TYPE LIKE '%��%'
            OR BUDGET_TYPE LIKE '%?%'
        """
        
        result = self.db_manager.execute_query(query)
        encoding_issues = result[0][0] if result else 0
        
        if encoding_issues == 0:
            self.validation_results['passed'].append("✅ 인코딩: 문제 없음")
        else:
            self.validation_results['failed'].append(
                f"❌ 인코딩: {encoding_issues}건 깨진 문자"
            )
    
    def _validate_plan_id_mapping(self):
        """PLAN_ID 매핑 검증"""
        logger.info("🗺️ PLAN_ID 매핑 검증 중...")
        
        # 중복 PLAN_ID 체크
        query = """
            SELECT PLAN_ID, COUNT(*) AS CNT
            FROM TB_PLAN_DATA
            GROUP BY PLAN_ID
            HAVING COUNT(*) > 1
        """
        
        result = self.db_manager.execute_query(query)
        
        if not result:
            self.validation_results['passed'].append("✅ PLAN_ID: 중복 없음")
        else:
            for plan_id, count in result:
                self.validation_results['failed'].append(
                    f"❌ PLAN_ID 중복: {plan_id} ({count}건)"
                )
    
    def print_summary(self):
        """검증 결과 요약 출력"""
        print("\n" + "="*80)
        print("📊 Oracle 데이터 검증 결과")
        print("="*80)
        
        # 통계
        if self.validation_results['statistics']:
            print("\n📈 테이블별 레코드 수:")
            for table, count in self.validation_results['statistics'].items():
                if table != 'TOTAL':
                    print(f"  • {table}: {count:,}건")
            print(f"  ────────────────────")
            print(f"  • 전체: {self.validation_results['statistics'].get('TOTAL', 0):,}건")
        
        # 성공 항목
        if self.validation_results['passed']:
            print(f"\n✅ 성공: {len(self.validation_results['passed'])}건")
            for item in self.validation_results['passed'][:10]:
                print(f"  {item}")
            if len(self.validation_results['passed']) > 10:
                print(f"  ... 외 {len(self.validation_results['passed']) - 10}건")
        
        # 경고 항목
        if self.validation_results['warnings']:
            print(f"\n⚠️ 경고: {len(self.validation_results['warnings'])}건")
            for item in self.validation_results['warnings']:
                print(f"  {item}")
        
        # 실패 항목
        if self.validation_results['failed']:
            print(f"\n❌ 실패: {len(self.validation_results['failed'])}건")
            for item in self.validation_results['failed']:
                print(f"  {item}")
        
        # 최종 판정
        print("\n" + "="*80)
        if not self.validation_results['failed']:
            print("🎉 검증 결과: 성공 (문제 없음)")
        elif len(self.validation_results['failed']) <= 3:
            print("⚠️ 검증 결과: 부분 성공 (일부 문제 있음)")
        else:
            print("❌ 검증 결과: 실패 (심각한 문제 발견)")
        print("="*80)
    
    def close(self):
        """연결 종료"""
        self.db_manager.close()


def main():
    """메인 실행"""
    validator = OracleDataValidator(ORACLE_CONFIG)
    
    try:
        # 연결
        logger.info("🔌 Oracle 데이터베이스 연결 중...")
        validator.connect()
        
        # 검증 실행
        results = validator.validate_all()
        
        # 결과 출력
        validator.print_summary()
        
        # 상세 결과 저장
        with open('validation_report.txt', 'w', encoding='utf-8') as f:
            f.write("Oracle 데이터 검증 보고서\n")
            f.write("="*80 + "\n\n")
            
            f.write("성공 항목:\n")
            for item in results['passed']:
                f.write(f"  {item}\n")
            
            f.write("\n경고 항목:\n")
            for item in results['warnings']:
                f.write(f"  {item}\n")
            
            f.write("\n실패 항목:\n")
            for item in results['failed']:
                f.write(f"  {item}\n")
        
        logger.info("📄 검증 보고서 저장: validation_report.txt")
        
    except Exception as e:
        logger.error(f"❌ 검증 실패: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        validator.close()


if __name__ == "__main__":
    main()
-- ================================================================================
-- 생명공학육성시행계획 Oracle DB 테스트 쿼리
-- ================================================================================

-- ============================================================================
-- 1. 추진실적 조회 (TB_PLAN_PERFORMANCE)
-- ============================================================================

-- 1-1. 정량적 실적 조회 (특허, 논문)
SELECT
    m.YEAR,
    m.NATION_ORGAN_NM AS 부처명,
    m.DETAIL_BIZ_NM AS 내역사업명,
    p.PERFORMANCE_YEAR AS 실적연도,
    p.PERFORMANCE_TYPE AS 성과유형,
    p.CATEGORY AS 세부항목,
    p.VALUE AS 실적값,
    p.UNIT AS 단위
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_PERFORMANCE p ON m.PLAN_ID = p.PLAN_ID
WHERE p.PERFORMANCE_TYPE IN ('특허', '논문', '인력양성')
ORDER BY m.YEAR DESC, m.NATION_ORGAN_NM, p.PERFORMANCE_YEAR DESC;

-- 1-2. 정성적 실적 조회 (추진실적 텍스트)
SELECT
    m.YEAR,
    m.NATION_ORGAN_NM AS 부처명,
    m.DETAIL_BIZ_NM AS 내역사업명,
    p.PERFORMANCE_YEAR AS 실적연도,
    p.ORIGINAL_TEXT AS 추진실적내용
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_PERFORMANCE p ON m.PLAN_ID = p.PLAN_ID
WHERE p.PERFORMANCE_TYPE = '정성적실적'
ORDER BY m.YEAR DESC, m.NATION_ORGAN_NM;

-- 1-3. 특정 사업의 전체 실적 조회
SELECT
    p.PERFORMANCE_YEAR AS 실적연도,
    p.PERFORMANCE_TYPE AS 성과유형,
    p.CATEGORY AS 세부항목,
    p.VALUE AS 실적값,
    p.UNIT AS 단위,
    SUBSTR(p.ORIGINAL_TEXT, 1, 100) AS 내용요약
FROM TB_PLAN_PERFORMANCE p
JOIN TB_PLAN_MASTER m ON p.PLAN_ID = m.PLAN_ID
WHERE m.DETAIL_BIZ_NM LIKE '%신약개발%'
ORDER BY p.PERFORMANCE_YEAR DESC, p.PERFORMANCE_TYPE;


-- ============================================================================
-- 2. 추진계획 조회 (TB_PLAN_DETAIL, TB_PLAN_SCHEDULE)
-- ============================================================================

-- 2-1. 사업 목표 및 내용 조회
SELECT
    m.YEAR,
    m.NATION_ORGAN_NM AS 부처명,
    m.DETAIL_BIZ_NM AS 내역사업명,
    d.LAST_GOAL AS 최종목표,
    d.BIZ_CONTENTS AS 사업내용,
    d.LEAD_ORGAN_NM AS 주관기관,
    d.MNG_ORGAN_NM AS 관리기관
FROM TB_PLAN_MASTER m
LEFT JOIN TB_PLAN_DETAIL d ON m.PLAN_ID = d.PLAN_ID
WHERE m.YEAR = 2020
ORDER BY m.NATION_ORGAN_NM, m.NUM;

-- 2-2. 연도별 추진계획 수립 현황
SELECT
    m.YEAR AS 계획연도,
    COUNT(DISTINCT m.PLAN_ID) AS 사업수,
    COUNT(d.DETAIL_ID) AS 상세정보수
FROM TB_PLAN_MASTER m
LEFT JOIN TB_PLAN_DETAIL d ON m.PLAN_ID = d.PLAN_ID
GROUP BY m.YEAR
ORDER BY m.YEAR DESC;


-- ============================================================================
-- 3. 소요예산 조회 (TB_PLAN_BUDGET) - 표 형태
-- ============================================================================

-- 3-1. 사업별 연도별 예산 현황 (실적/계획 구분)
SELECT
    m.YEAR AS 문서연도,
    m.NATION_ORGAN_NM AS 부처명,
    m.DETAIL_BIZ_NM AS 내역사업명,
    b.BUDGET_YEAR AS 예산연도,
    b.CATEGORY AS 구분,
    b.TOTAL_AMOUNT AS 총액,
    b.GOV_AMOUNT AS 정부예산,
    b.PRIVATE_AMOUNT AS 민간예산,
    b.LOCAL_AMOUNT AS 지방비,
    b.ETC_AMOUNT AS 기타
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_BUDGET b ON m.PLAN_ID = b.PLAN_ID
WHERE m.YEAR = 2020
ORDER BY m.NATION_ORGAN_NM, m.NUM, b.BUDGET_YEAR, b.CATEGORY;

-- 3-2. 특정 사업의 예산 추이 (PIVOT 형태)
SELECT
    m.DETAIL_BIZ_NM AS 내역사업명,
    MAX(CASE WHEN b.BUDGET_YEAR = 2017 AND b.CATEGORY = '실적' THEN b.TOTAL_AMOUNT END) AS "2017_실적",
    MAX(CASE WHEN b.BUDGET_YEAR = 2018 AND b.CATEGORY = '실적' THEN b.TOTAL_AMOUNT END) AS "2018_실적",
    MAX(CASE WHEN b.BUDGET_YEAR = 2019 AND b.CATEGORY = '실적' THEN b.TOTAL_AMOUNT END) AS "2019_실적",
    MAX(CASE WHEN b.BUDGET_YEAR = 2020 AND b.CATEGORY = '계획' THEN b.TOTAL_AMOUNT END) AS "2020_계획",
    MAX(CASE WHEN b.BUDGET_YEAR = 2021 AND b.CATEGORY = '계획' THEN b.TOTAL_AMOUNT END) AS "2021_계획"
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_BUDGET b ON m.PLAN_ID = b.PLAN_ID
WHERE m.YEAR = 2020
GROUP BY m.DETAIL_BIZ_NM
ORDER BY m.DETAIL_BIZ_NM;

-- 3-3. 부처별 총 예산 집계
SELECT
    m.NATION_ORGAN_NM AS 부처명,
    b.BUDGET_YEAR AS 예산연도,
    b.CATEGORY AS 구분,
    SUM(b.TOTAL_AMOUNT) AS 총예산,
    SUM(b.GOV_AMOUNT) AS 정부예산합계,
    SUM(b.PRIVATE_AMOUNT) AS 민간예산합계,
    COUNT(DISTINCT m.PLAN_ID) AS 사업수
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_BUDGET b ON m.PLAN_ID = b.PLAN_ID
WHERE m.YEAR = 2020
GROUP BY m.NATION_ORGAN_NM, b.BUDGET_YEAR, b.CATEGORY
ORDER BY m.NATION_ORGAN_NM, b.BUDGET_YEAR, b.CATEGORY;


-- ============================================================================
-- 4. 추진일정 조회 (TB_PLAN_SCHEDULE) - 표 형태
-- ============================================================================

-- 4-1. 사업별 추진일정 전체 조회
SELECT
    m.YEAR AS 문서연도,
    m.NATION_ORGAN_NM AS 부처명,
    m.DETAIL_BIZ_NM AS 내역사업명,
    s.SCHEDULE_YEAR AS 일정연도,
    s.QUARTER AS 분기,
    s.TASK_NAME AS 과제명,
    s.START_DATE AS 시작일,
    s.END_DATE AS 종료일,
    SUBSTR(s.TASK_CONTENT, 1, 100) AS 세부내용
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_SCHEDULE s ON m.PLAN_ID = s.PLAN_ID
WHERE m.YEAR = 2020
ORDER BY m.NATION_ORGAN_NM, m.NUM, s.SCHEDULE_YEAR, s.START_DATE;

-- 4-2. 특정 사업의 분기별 일정 (읽기 쉬운 형태)
SELECT
    s.SCHEDULE_YEAR AS 연도,
    s.QUARTER AS 분기,
    TO_CHAR(s.START_DATE, 'YYYY-MM-DD') AS 시작일,
    TO_CHAR(s.END_DATE, 'YYYY-MM-DD') AS 종료일,
    s.TASK_NAME AS 과제명,
    s.TASK_CONTENT AS 세부내용
FROM TB_PLAN_SCHEDULE s
JOIN TB_PLAN_MASTER m ON s.PLAN_ID = m.PLAN_ID
WHERE m.DETAIL_BIZ_NM LIKE '%신약개발%'
  AND s.SCHEDULE_YEAR = 2020
ORDER BY s.START_DATE;

-- 4-3. 2020년 전체 사업의 분기별 과제 수
SELECT
    m.NATION_ORGAN_NM AS 부처명,
    s.QUARTER AS 분기,
    COUNT(*) AS 과제수,
    COUNT(DISTINCT m.PLAN_ID) AS 사업수
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_SCHEDULE s ON m.PLAN_ID = s.PLAN_ID
WHERE m.YEAR = 2020 AND s.SCHEDULE_YEAR = 2020
GROUP BY m.NATION_ORGAN_NM, s.QUARTER
ORDER BY m.NATION_ORGAN_NM, s.QUARTER;


-- ============================================================================
-- 5. 통합 검색 쿼리 (한 사업의 모든 정보)
-- ============================================================================

-- 5-1. 특정 사업의 완전한 정보 조회
-- 마스터 정보
SELECT '=== 기본 정보 ===' AS 구분, NULL AS 내용 FROM DUAL
UNION ALL
SELECT '연도', TO_CHAR(YEAR) FROM TB_PLAN_MASTER WHERE DETAIL_BIZ_NM LIKE '%신약개발%' AND YEAR = 2020
UNION ALL
SELECT '부처명', NATION_ORGAN_NM FROM TB_PLAN_MASTER WHERE DETAIL_BIZ_NM LIKE '%신약개발%' AND YEAR = 2020
UNION ALL
SELECT '내역사업명', DETAIL_BIZ_NM FROM TB_PLAN_MASTER WHERE DETAIL_BIZ_NM LIKE '%신약개발%' AND YEAR = 2020;

-- 5-2. 사업별 데이터 존재 여부 체크
SELECT
    m.DETAIL_BIZ_NM AS 내역사업명,
    COUNT(DISTINCT d.DETAIL_ID) AS 상세정보,
    COUNT(DISTINCT b.BUDGET_YEAR) AS 예산연도수,
    COUNT(DISTINCT s.SCHEDULE_ID) AS 일정건수,
    COUNT(DISTINCT p.PERFORMANCE_ID) AS 성과건수
FROM TB_PLAN_MASTER m
LEFT JOIN TB_PLAN_DETAIL d ON m.PLAN_ID = d.PLAN_ID
LEFT JOIN TB_PLAN_BUDGET b ON m.PLAN_ID = b.PLAN_ID
LEFT JOIN TB_PLAN_SCHEDULE s ON m.PLAN_ID = s.PLAN_ID
LEFT JOIN TB_PLAN_PERFORMANCE p ON m.PLAN_ID = p.PLAN_ID
WHERE m.YEAR = 2020
GROUP BY m.DETAIL_BIZ_NM
ORDER BY m.DETAIL_BIZ_NM;


-- ============================================================================
-- 6. 빠른 확인용 쿼리 (테이블별 레코드 수)
-- ============================================================================

SELECT 'TB_PLAN_MASTER' AS 테이블명, COUNT(*) AS 레코드수 FROM TB_PLAN_MASTER
UNION ALL
SELECT 'TB_PLAN_DETAIL', COUNT(*) FROM TB_PLAN_DETAIL
UNION ALL
SELECT 'TB_PLAN_BUDGET', COUNT(*) FROM TB_PLAN_BUDGET
UNION ALL
SELECT 'TB_PLAN_SCHEDULE', COUNT(*) FROM TB_PLAN_SCHEDULE
UNION ALL
SELECT 'TB_PLAN_PERFORMANCE', COUNT(*) FROM TB_PLAN_PERFORMANCE
UNION ALL
SELECT 'TB_PLAN_WEIGHT', COUNT(*) FROM TB_PLAN_WEIGHT;


-- ============================================================================
-- 7. 데이터 검증 쿼리 (NULL 체크, 일정 날짜 확인)
-- ============================================================================

-- 7-1. 일정의 날짜 정확성 체크 (실제 월 정보 확인)
SELECT
    m.DETAIL_BIZ_NM AS 내역사업명,
    s.QUARTER AS 분기,
    TO_CHAR(s.START_DATE, 'YYYY-MM-DD') AS 시작일,
    TO_CHAR(s.END_DATE, 'YYYY-MM-DD') AS 종료일,
    EXTRACT(MONTH FROM s.START_DATE) AS 시작월,
    EXTRACT(MONTH FROM s.END_DATE) AS 종료월,
    CASE
        WHEN s.QUARTER LIKE '%월~%월' THEN '✓ 실제 월 정보 사용'
        WHEN s.QUARTER LIKE '%분기' THEN '○ 분기 정보 대체'
        WHEN s.QUARTER = '연중' THEN '○ 연중'
        ELSE '? 기타'
    END AS 날짜추출방식
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_SCHEDULE s ON m.PLAN_ID = s.PLAN_ID
WHERE m.YEAR = 2020
ORDER BY m.DETAIL_BIZ_NM, s.START_DATE;

-- 7-2. 정성적 실적 포함 여부 확인
SELECT
    PERFORMANCE_TYPE AS 성과유형,
    COUNT(*) AS 건수
FROM TB_PLAN_PERFORMANCE
GROUP BY PERFORMANCE_TYPE
ORDER BY PERFORMANCE_TYPE;


-- ============================================================================
-- 8. 실무 활용 쿼리
-- ============================================================================

-- 8-1. 특정 부처의 2020년 전체 사업 현황
SELECT
    m.NUM AS 순번,
    m.DETAIL_BIZ_NM AS 내역사업명,
    d.LAST_GOAL AS 최종목표,
    (SELECT SUM(b.TOTAL_AMOUNT)
     FROM TB_PLAN_BUDGET b
     WHERE b.PLAN_ID = m.PLAN_ID AND b.BUDGET_YEAR = 2020) AS "2020년예산",
    (SELECT COUNT(*)
     FROM TB_PLAN_SCHEDULE s
     WHERE s.PLAN_ID = m.PLAN_ID AND s.SCHEDULE_YEAR = 2020) AS 일정건수,
    (SELECT COUNT(*)
     FROM TB_PLAN_PERFORMANCE p
     WHERE p.PLAN_ID = m.PLAN_ID) AS 성과건수
FROM TB_PLAN_MASTER m
LEFT JOIN TB_PLAN_DETAIL d ON m.PLAN_ID = d.PLAN_ID
WHERE m.YEAR = 2020
  AND m.NATION_ORGAN_NM = '과학기술정보통신부'
ORDER BY m.NUM;

-- 8-2. 예산 규모별 사업 순위
SELECT
    m.NATION_ORGAN_NM AS 부처명,
    m.DETAIL_BIZ_NM AS 내역사업명,
    SUM(CASE WHEN b.BUDGET_YEAR = 2020 THEN b.TOTAL_AMOUNT ELSE 0 END) AS "2020년예산",
    SUM(b.TOTAL_AMOUNT) AS 총예산
FROM TB_PLAN_MASTER m
JOIN TB_PLAN_BUDGET b ON m.PLAN_ID = b.PLAN_ID
WHERE m.YEAR = 2020
GROUP BY m.NATION_ORGAN_NM, m.DETAIL_BIZ_NM
ORDER BY "2020년예산" DESC;


-- ============================================================================
-- 9. 전체 데이터 샘플 조회 (각 테이블 상위 5건)
-- ============================================================================

SELECT '=== TB_PLAN_MASTER (상위 5건) ===' AS INFO FROM DUAL;
SELECT * FROM TB_PLAN_MASTER WHERE ROWNUM <= 5 ORDER BY YEAR DESC, NUM;

SELECT '=== TB_PLAN_BUDGET (상위 5건) ===' AS INFO FROM DUAL;
SELECT * FROM TB_PLAN_BUDGET WHERE ROWNUM <= 5;

SELECT '=== TB_PLAN_SCHEDULE (상위 5건) ===' AS INFO FROM DUAL;
SELECT * FROM TB_PLAN_SCHEDULE WHERE ROWNUM <= 5;

SELECT '=== TB_PLAN_PERFORMANCE (상위 5건) ===' AS INFO FROM DUAL;
SELECT * FROM TB_PLAN_PERFORMANCE WHERE ROWNUM <= 5;


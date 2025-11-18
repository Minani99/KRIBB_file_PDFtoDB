# Oracle 적재 모듈 변경 사항

## 2024-11-18 개선 사항

### 🔧 주요 수정 사항

#### 1. **plan_id_mapping 문제 해결**
- **문제**: 지역 변수로만 다루고 `self.plan_id_mapping`은 업데이트하지 않아 하위 테이블 적재 시 매핑 실패
- **해결**: 인스턴스 변수 `self.plan_id_mapping`에 직접 저장

#### 2. **금액 타입 처리 개선**
- **문제**: 모든 금액을 문자열로 변환하여 NUMBER 컬럼에 타입 불일치
- **해결**: 
  - `TOTAL_RESPRC`, `CUR_RESPRC`: VARCHAR2로 문자열 저장
  - 나머지 금액 필드: NUMBER로 숫자 저장

#### 3. **테이블 관리 방식 개선**
- **문제**: 항상 DROP TABLE로 회사 스키마 손실 위험
- **해결**: TRUNCATE 옵션 제공, 기존 테이블 구조 유지

#### 4. **인코딩 문제 해결**
- **문제**: '민간'이 '��간'으로 깨짐
- **해결**: UTF-8 설정 및 정규식 패턴 매칭으로 자동 보정

#### 5. **로깅 개선**
- **문제**: 테이블명 없는 일반 메시지
- **해결**: 모든 작업에 테이블명 포함한 상세 로그

### 📁 추가된 파일

1. **load_oracle_final.py** - 최종 운영 버전
2. **load_oracle_db_improved.py** - 개선 버전 (참고용)
3. **oracle_db_manager_improved.py** - 개선된 DB 매니저
4. **validate_oracle_data.py** - 데이터 검증 도구
5. **config_oracle_schema.py** - 스키마 설정 템플릿
6. **README_ORACLE_LOAD.md** - 사용 가이드
7. **run_oracle_load.sh** - 실행 스크립트
8. **.env.example** - 환경 변수 예제

### ✅ 검증된 사항

- [x] 회사 DDL(BICS_DEV.TB_PLAN_DATA)과 정확히 일치
- [x] normalized 폴더 구조 반영
- [x] 금액 타입 구분 처리
- [x] plan_id_mapping 정상 작동
- [x] 인코딩 문제 해결
- [x] NULL 허용 컬럼 처리

### 📝 TODO (추후 작업)

- [ ] AREA (3대영역) 데이터 매핑
- [ ] 가중치 데이터 (BIOLOGY_WEI, RED_WEI 등) 입력
- [ ] BIZ_CONTENTS_KEYWORD 추출 및 입력
- [ ] RESPERIOD 상세 기간 정보 개선

### 🚀 사용 방법

```bash
# 1. 설정
cp .env.example .env
# .env 파일 수정

# 2. 실행
./run_oracle_load.sh

# 3. 검증
python validate_oracle_data.py
```

### ⚠️ 주의사항

1. **운영 환경 적용 전 테스트 필수**
2. **데이터 백업 권장**
3. **DBA와 사전 협의**
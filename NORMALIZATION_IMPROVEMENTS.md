# 정규화 전략 개선안

## 📊 현재 상황 분석

### 매칭 로직 (normalize_government_standard.py)
**현재 매칭 방식:**
```python
def _find_best_match(self, year, biz_name, detail_biz_name, threshold=85):
    """
    (YEAR, BIZ_NM, DETAIL_BIZ_NM) 3개 조건으로 DB 매칭
    1. 완전 일치 확인
    2. 유사도 기반 매칭 (fuzz ratio)
    """
```

**✅ 장점:**
- 원본 텍스트 그대로 비교 (정보 손실 최소화)
- 완전 일치 → 유사도 매칭 순차 시도
- threshold=85로 적절한 허용치

**❌ 문제:**
- 첫 실행 시 DB가 비어있어 매칭 불가 (정상 동작)
- 연도별로 사업명이 약간씩 변경되면 매칭 실패 가능

---

## 🔧 개선 전략

### 1. **정규화 로직 개선** ✅ (이미 적용됨)
현재 코드가 이미 최적화되어 있습니다:
- `_normalize_for_matching()`: 특수문자 통일, 공백 제거
- `_find_best_match()`: 원본 텍스트로 fuzz 매칭
- 정보 손실 최소화

### 2. **매칭 threshold 조정 가능**
현재 85% → 필요시 80%로 낮출 수 있음
```python
# normalize_government_standard.py Line 89
def _find_best_match(self, year, biz_name, detail_biz_name, threshold=80):  # 85 → 80
```

### 3. **CID 폰트 문제** ⚠️ (진행 중)
- PDF 추출 단계에서 CID 코드 제거
- `extract_pdf_to_json.py`에 `_clean_cid_text()` 추가
- 2024년 PDF 재추출 필요

---

## 📝 실행 순서 (권장)

### 단계 1: 2024년 PDF 재추출 (CID 수정)
```bash
python extract_2024_fixed.py
```

### 단계 2: 전체 JSON 정규화 (DB 없이)
```bash
python run_normalize_and_match.py
```
→ **결과**: CSV 생성 (모두 TEMP_ ID)

### 단계 3: CSV를 DB에 적재
```bash
python load_csv_to_db.py
```

### 단계 4: 다시 정규화 (DB 매칭)
```bash
python run_normalize_and_match.py
```
→ **결과**: CSV 갱신 (실제 PLAN_ID 매칭)

---

## 🎯 매칭률 향상 팁

### 1. **사업명 정확도 확인**
DB의 BIZ_NM과 JSON의 사업명이 완전히 일치하는지 확인
```sql
SELECT DISTINCT BIZ_NM FROM TB_PLAN_DATA WHERE YEAR = 2024;
```

### 2. **내역사업명 확인**
JSON 추출이 정확한지 확인 (특히 테이블 헤더)
```python
# 로그에서 확인
[INFO] 내역사업 발견: XXX
```

### 3. **매칭 실패 시 로그 확인**
```python
# normalize_government_standard.py에서 로깅
[FAIL] 매칭 실패 - BIZ_NM: 'XXX', DETAIL_BIZ_NM: 'YYY'
```

---

## ⚠️ 알려진 제약사항

### 1. **첫 실행은 항상 TEMP_ ID**
- DB가 비어있으므로 정상 동작
- 해결: DB 적재 후 재실행

### 2. **CID 폰트 완전 제거 불가**
- PDF 자체에 CID 폰트 임베딩
- 해결: OCR 도구 사용 권장 (Adobe Acrobat, Tesseract)

### 3. **연도별 사업명 변경**
- 정부 사업명이 매년 조금씩 변경됨
- 해결: fuzz matching으로 유사도 기반 매칭

---

## 📌 결론

**현재 정규화 로직은 이미 최적화되어 있습니다.**

주요 개선 사항:
✅ 원본 텍스트 기반 매칭 (정보 손실 최소화)
✅ Fuzzy matching (유사도 85%)
✅ 완전 일치 우선, 유사도 fallback
⚠️ CID 폰트 문제 해결 진행 중

**다음 작업:**
1. 2024년 PDF 재추출 (CID 수정)
2. 전체 정규화 실행
3. DB 적재
4. 재정규화 (매칭 확인)


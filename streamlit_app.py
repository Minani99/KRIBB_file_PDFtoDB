#!/usr/bin/env python3
"""
생명공학육성시행계획 PDF → Oracle DB 처리 시스템
Streamlit 웹 UI

사용법:
    streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import json
import time
import sys
import os
import re

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 모듈 임포트
from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from config import INPUT_DIR, OUTPUT_DIR, NORMALIZED_OUTPUT_GOVERNMENT_DIR

# DB 모듈 (선택적)
try:
    from load_oracle_direct import OracleDirectLoader
    from oracle_db_manager import OracleDBManager
    from config import ORACLE_CONFIG, ORACLE_CONFIG_DEV
    DB_AVAILABLE = True
except ImportError as e:
    DB_AVAILABLE = False
    print(f"⚠️ DB 모듈 로드 실패: {e}")

# 페이지 설정
st.set_page_config(
    page_title="생명공학육성시행계획 PDF 처리",
    page_icon="🧬",
    layout="wide"
)

# 디렉토리 설정
SERVER_INPUT_DIR = Path(INPUT_DIR).resolve()
SERVER_OUTPUT_DIR = Path(OUTPUT_DIR).resolve()
SERVER_NORMALIZED_DIR = Path(NORMALIZED_OUTPUT_GOVERNMENT_DIR).resolve()

# 디렉토리 생성
SERVER_INPUT_DIR.mkdir(exist_ok=True)
SERVER_OUTPUT_DIR.mkdir(exist_ok=True)
SERVER_NORMALIZED_DIR.mkdir(exist_ok=True)

# 세션 상태 초기화
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = []
if 'normalized_stats' not in st.session_state:
    st.session_state.normalized_stats = None
if 'db_stats' not in st.session_state:
    st.session_state.db_stats = None


def save_uploaded_files(uploaded_files):
    """업로드된 파일 저장"""
    saved_files = []
    for file in uploaded_files:
        file_path = SERVER_INPUT_DIR / file.name
        with open(file_path, 'wb') as f:
            f.write(file.getbuffer())
        saved_files.append(file_path)
    return saved_files


def process_single_pdf(pdf_path, progress_callback=None):
    """단일 PDF 처리 (PDF → JSON)"""
    try:
        if progress_callback:
            progress_callback(f"📄 {pdf_path.name} - PDF 파싱 중...")

        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF 파일이 없습니다: {pdf_path}")

        file_size = pdf_path.stat().st_size
        if file_size == 0:
            raise ValueError(f"PDF 파일이 비어있습니다: {pdf_path}")

        # PDF → JSON 변환
        extract_pdf_to_json(str(pdf_path), str(SERVER_OUTPUT_DIR))

        # JSON 파일 확인
        json_path = SERVER_OUTPUT_DIR / f"{pdf_path.stem}.json"
        if not json_path.exists():
            raise FileNotFoundError(f"JSON 파일이 생성되지 않았습니다: {json_path}")

        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)

        pages_count = len(json_data.get('pages', []))
        
        return {
            'file': pdf_path.name, 
            'status': 'success', 
            'json_path': str(json_path), 
            'pages': pages_count
        }

    except Exception as e:
        import traceback
        return {
            'file': pdf_path.name, 
            'status': 'failed', 
            'error': str(e),
            'traceback': traceback.format_exc()
        }


def normalize_all_jsons(progress_callback=None):
    """모든 JSON 정규화 (JSON → CSV)"""
    json_files = list(SERVER_OUTPUT_DIR.glob("*.json"))
    json_files = [f for f in json_files if not f.name.startswith('batch_')]

    if not json_files:
        st.error(f"❌ {SERVER_OUTPUT_DIR}에 JSON 파일이 없습니다.")
        return None

    if progress_callback:
        progress_callback(f"📋 {len(json_files)}개 JSON 파일 발견")

    try:
        # 모든 JSON 로드
        all_json_data = []
        for i, json_file in enumerate(json_files, 1):
            if progress_callback:
                progress_callback(f"📂 JSON 로드 중: {json_file.name} ({i}/{len(json_files)})")

            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                all_json_data.append((json_file, json_data))

        if not all_json_data:
            st.error("❌ 로드된 JSON이 없습니다.")
            return None

        # DB 연결 (PLAN_ID 매칭용 - 선택적)
        db_manager = None
        if DB_AVAILABLE:
            try:
                db_manager = OracleDBManager(ORACLE_CONFIG)
                db_manager.connect()
                st.success("🔗 DB 연결 성공 (PLAN_ID 매칭용)")
            except Exception as e:
                st.warning(f"⚠️ DB 연결 실패 (신규 PLAN_ID로 생성): {e}")
                db_manager = None

        # 첫 번째 파일로 normalizer 초기화
        if progress_callback:
            progress_callback("📋 데이터 정규화 시작...")

        normalizer = GovernmentStandardNormalizer(
            str(json_files[0]),
            str(SERVER_NORMALIZED_DIR),
            db_manager=db_manager
        )

        # 각 JSON 파일별로 처리
        for json_file, json_data in all_json_data:
            if progress_callback:
                progress_callback(f"📋 정규화 중: {json_file.name}")

            # 파일명에서 연도 추출
            filename = json_file.stem
            year_match = re.search(r'(20\d{2})', filename)

            if year_match:
                doc_year = int(year_match.group(1))
                normalizer.current_context['document_year'] = doc_year
                normalizer.current_context['performance_year'] = doc_year - 1
                normalizer.current_context['plan_year'] = doc_year

            normalizer.normalize(json_data)

        # CSV 저장
        if progress_callback:
            progress_callback("💾 CSV 저장 중...")

        normalizer.save_to_csv()

        # DB 연결 종료
        if db_manager:
            db_manager.close()

        # 통계
        stats = {
            'plan_data': len(normalizer.data['plan_data']),
            'budgets': len(normalizer.data['budgets']),
            'schedules': len(normalizer.data['schedules']),
            'performances': len(normalizer.data['performances']),
            'achievements': len(normalizer.data['achievements'])
        }

        return stats

    except Exception as e:
        st.error(f"❌ 정규화 실패: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


def load_to_oracle(progress_callback=None):
    """Oracle DB 적재 (CSV → DB)"""
    if not DB_AVAILABLE:
        st.error("❌ DB 모듈이 로드되지 않았습니다.")
        return None

    try:
        csv_files = list(SERVER_NORMALIZED_DIR.glob("TB_PLAN_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"CSV 파일이 없습니다: {SERVER_NORMALIZED_DIR}")

        if progress_callback:
            progress_callback(f"🔌 Oracle DB 연결 중... ({len(csv_files)}개 CSV 발견)")

        loader = OracleDirectLoader(
            db_config_read=ORACLE_CONFIG,
            db_config_write=ORACLE_CONFIG_DEV,
            csv_dir=str(SERVER_NORMALIZED_DIR)
        )

        loader.connect()

        if progress_callback:
            progress_callback("🔍 기존 TB_PLAN_DATA와 매칭 중...")

        loader.load_with_matching()
        loader.close()

        return loader.load_stats

    except Exception as e:
        error_msg = str(e)
        
        # ORA 에러 해석
        if "ORA-00001" in error_msg:
            raise Exception("중복 키 에러: 이미 같은 데이터가 존재합니다. DB 초기화 후 재시도하세요.")
        elif "ORA-02291" in error_msg:
            raise Exception("FK 제약 위반: 부모 키(PLAN_ID)를 찾을 수 없습니다.")
        elif "ORA-12541" in error_msg:
            raise Exception("Oracle 서버 연결 실패: 서버가 실행 중인지 확인하세요.")
        else:
            raise Exception(f"Oracle DB 적재 실패: {error_msg}")


def display_csv_data(csv_dir):
    """CSV 데이터 표시"""
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        st.warning("❌ CSV 파일이 없습니다.")
        return

    csv_files = [f for f in csv_path.glob("*.csv") if f.name.startswith("TB_PLAN_")]
    if not csv_files:
        st.warning("❌ CSV 파일이 없습니다.")
        return

    # 테이블별 탭
    tab_names = [f.stem for f in csv_files]
    tabs = st.tabs(tab_names)

    for tab, csv_file in zip(tabs, csv_files):
        with tab:
            try:
                df = pd.read_csv(csv_file, encoding='utf-8-sig')
                st.write(f"**{csv_file.stem}** - {len(df):,}건")
                
                # 처음 100개만 표시
                st.dataframe(df.head(100), use_container_width=True)
                
                if len(df) > 100:
                    st.info(f"ℹ️ 전체 {len(df):,}건 중 100건만 표시됨")

                # 다운로드 버튼
                csv_data = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button(
                    label=f"📥 {csv_file.stem} 다운로드",
                    data=csv_data,
                    file_name=csv_file.name,
                    mime='text/csv'
                )

            except Exception as e:
                st.error(f"❌ 파일 로드 실패: {e}")


def main():
    """메인 UI"""
    
    # 헤더
    st.title("🧬 생명공학육성시행계획 PDF 처리 시스템")
    st.markdown("**PDF 자동 파싱 → JSON 변환 → CSV 정규화 → Oracle DB 적재**")

    # 사이드바
    with st.sidebar:
        st.header("⚙️ 설정")

        enable_db_load = st.checkbox(
            "🗄️ Oracle DB 적재",
            value=DB_AVAILABLE,
            disabled=not DB_AVAILABLE,
            help="체크 해제 시 CSV만 생성"
        )

        if not DB_AVAILABLE:
            st.warning("⚠️ DB 모듈 미로드")

        st.markdown("---")

        if DB_AVAILABLE:
            st.subheader("📊 Oracle DB 정보")
            st.text(f"🔍 읽기: {ORACLE_CONFIG['user']}@{ORACLE_CONFIG['host']}")
            st.text(f"✍️ 쓰기: {ORACLE_CONFIG_DEV['user']}@{ORACLE_CONFIG_DEV['host']}")

            st.markdown("---")

            # DB 초기화 버튼
            if st.button("🗑️ BICS_DEV 하위테이블 초기화", type="secondary"):
                try:
                    with st.spinner("초기화 중..."):
                        loader = OracleDirectLoader(
                            db_config_read=ORACLE_CONFIG,
                            db_config_write=ORACLE_CONFIG_DEV,
                            csv_dir=str(SERVER_NORMALIZED_DIR)
                        )
                        loader.connect()
                        
                        cursor = loader.db_manager_write.connection.cursor()
                        tables = ['TB_PLAN_ACHIEVEMENTS', 'TB_PLAN_PERFORMANCE', 
                                 'TB_PLAN_SCHEDULE', 'TB_PLAN_BUDGET']
                        
                        for table in tables:
                            try:
                                cursor.execute(f"DELETE FROM {table}")
                                loader.db_manager_write.connection.commit()
                                st.success(f"✅ {table} 삭제 완료")
                            except Exception as e:
                                st.warning(f"⚠️ {table}: {e}")
                        
                        cursor.close()
                        loader.close()
                        
                except Exception as e:
                    st.error(f"❌ 초기화 실패: {e}")

        st.markdown("---")
        st.info("""
        **처리 단계:**
        1. PDF 업로드
        2. PDF → JSON 변환
        3. JSON → CSV 정규화
        4. CSV → Oracle DB 적재
        """)

    # 메인 탭
    tab1, tab2, tab3, tab4 = st.tabs(["📤 업로드", "📊 처리 결과", "📁 CSV 데이터", "🗄️ DB 통계"])

    with tab1:
        st.header("PDF 파일 업로드")

        uploaded_files = st.file_uploader(
            "PDF 파일 선택 (여러 개 가능)",
            type=['pdf'],
            accept_multiple_files=True,
            help="생명공학육성시행계획 PDF 파일을 업로드하세요"
        )

        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)}개 파일 선택됨")

            with st.expander("📋 선택된 파일", expanded=True):
                for file in uploaded_files:
                    st.write(f"- {file.name} ({file.size:,} bytes)")

            col1, col2 = st.columns([3, 1])

            with col1:
                process_button = st.button(
                    "🚀 전체 파이프라인 실행",
                    type="primary",
                    use_container_width=True
                )

            with col2:
                if st.button("🗑️ 초기화"):
                    st.session_state.processing_results = []
                    st.session_state.normalized_stats = None
                    st.session_state.db_stats = None
                    st.rerun()

            if process_button:
                progress_bar = st.progress(0)
                status_text = st.empty()
                start_time = time.time()

                try:
                    # 1단계: 파일 저장
                    status_text.text("💾 파일 저장 중...")
                    saved_files = save_uploaded_files(uploaded_files)
                    progress_bar.progress(0.1)

                    # 2단계: PDF → JSON
                    status_text.text("📄 PDF 파싱 중...")
                    results = []
                    for i, pdf_file in enumerate(saved_files):
                        result = process_single_pdf(pdf_file, lambda msg: status_text.text(msg))
                        results.append(result)
                        
                        if result['status'] == 'success':
                            st.success(f"✅ {result['file']}: {result['pages']}페이지")
                        else:
                            st.error(f"❌ {result['file']}: {result.get('error', '알 수 없는 오류')}")
                        
                        progress_bar.progress(0.1 + 0.4 * (i + 1) / len(saved_files))

                    st.session_state.processing_results = results
                    success_count = sum(1 for r in results if r['status'] == 'success')

                    if success_count == 0:
                        st.error("❌ 모든 파일 처리 실패")
                        return

                    # 3단계: JSON → CSV 정규화
                    status_text.text("📋 데이터 정규화 중...")
                    norm_stats = normalize_all_jsons(lambda msg: status_text.text(msg))
                    st.session_state.normalized_stats = norm_stats
                    progress_bar.progress(0.7)

                    if norm_stats:
                        st.success(f"""
                        ✅ 정규화 완료!
                        - 내역사업: {norm_stats['plan_data']}개
                        - 예산: {norm_stats['budgets']}건
                        - 일정: {norm_stats['schedules']}건
                        - 성과: {norm_stats['performances']}건
                        - 대표성과: {norm_stats['achievements']}건
                        """)

                    # 4단계: Oracle DB 적재
                    if enable_db_load and DB_AVAILABLE:
                        status_text.text("🗄️ Oracle DB 적재 중...")
                        try:
                            db_stats = load_to_oracle(lambda msg: status_text.text(msg))
                            st.session_state.db_stats = db_stats
                            progress_bar.progress(1.0)
                            
                            if db_stats:
                                st.success(f"✅ DB 적재 완료: {db_stats['total_records']}건")
                        except Exception as db_error:
                            st.error(f"❌ DB 적재 실패: {db_error}")
                            st.warning("⚠️ CSV 파일은 정상 생성되었습니다.")
                    else:
                        progress_bar.progress(1.0)

                    elapsed = time.time() - start_time
                    status_text.empty()
                    st.success(f"✅ 완료! (소요 시간: {elapsed:.1f}초)")
                    st.balloons()

                except Exception as e:
                    st.error(f"❌ 처리 실패: {e}")
                    import traceback
                    st.code(traceback.format_exc())

    with tab2:
        st.header("처리 결과")

        if st.session_state.processing_results:
            results = st.session_state.processing_results
            success = [r for r in results if r['status'] == 'success']
            failed = [r for r in results if r['status'] == 'failed']

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("총 파일", len(results))
            with col2:
                st.metric("성공", len(success))
            with col3:
                st.metric("실패", len(failed))

            if success:
                st.subheader("✅ 성공")
                for r in success:
                    st.write(f"- {r['file']} ({r.get('pages', 0)}페이지)")

            if failed:
                st.subheader("❌ 실패")
                for r in failed:
                    st.error(f"- {r['file']}: {r.get('error', '알 수 없는 오류')}")

            if st.session_state.normalized_stats:
                st.subheader("📊 정규화 통계")
                stats = st.session_state.normalized_stats
                
                cols = st.columns(5)
                with cols[0]:
                    st.metric("메인 데이터", stats['plan_data'])
                with cols[1]:
                    st.metric("예산", stats['budgets'])
                with cols[2]:
                    st.metric("일정", stats['schedules'])
                with cols[3]:
                    st.metric("성과", stats['performances'])
                with cols[4]:
                    st.metric("대표성과", stats['achievements'])
        else:
            st.info("ℹ️ 아직 처리된 결과가 없습니다.")

    with tab3:
        st.header("CSV 데이터")
        st.markdown("""
        **📋 테이블 구조:**
        - **TB_PLAN_DATA**: 내역사업 메인 정보
        - **TB_PLAN_BUDGET**: 연도별 예산 상세
        - **TB_PLAN_SCHEDULE**: 일정 상세
        - **TB_PLAN_PERFORMANCE**: 성과 상세
        - **TB_PLAN_ACHIEVEMENTS**: 대표성과
        """)

        if SERVER_NORMALIZED_DIR.exists():
            display_csv_data(SERVER_NORMALIZED_DIR)
        else:
            st.info("ℹ️ CSV 데이터가 없습니다.")

    with tab4:
        st.header("Oracle DB 통계")

        if st.session_state.db_stats:
            stats = st.session_state.db_stats

            cols = st.columns(4)
            with cols[0]:
                st.metric("총 적재", f"{stats['total_records']:,}건")
            with cols[1]:
                st.metric("✅ 매칭 성공", f"{stats.get('matched', 0)}건")
            with cols[2]:
                st.metric("❌ 매칭 실패", f"{stats.get('unmatched', 0)}건")
            with cols[3]:
                st.metric("⚠️ 차이점", f"{stats.get('diff_found', 0)}건")

            if stats.get('unmatched', 0) > 0:
                st.warning(f"⚠️ 매칭 실패 {stats['unmatched']}건 - 신규 사업으로 추정됩니다.")
                
                # 매칭 실패 리포트 표시
                unmatched_csv = SERVER_NORMALIZED_DIR / "matching_reports" / "unmatched_records.csv"
                if unmatched_csv.exists():
                    with st.expander("📄 매칭 실패 레코드"):
                        df = pd.read_csv(unmatched_csv, encoding='utf-8-sig')
                        st.dataframe(df, use_container_width=True)

            st.subheader("📊 테이블별 통계")
            if 'records_by_table' in stats:
                table_data = [{'테이블': k, '레코드 수': f"{v:,}건"} 
                             for k, v in stats['records_by_table'].items()]
                st.dataframe(pd.DataFrame(table_data), use_container_width=True)

        else:
            st.info("ℹ️ DB 적재 통계가 없습니다.")
            if not enable_db_load:
                st.warning("⚠️ 'Oracle DB 적재' 옵션이 비활성화되어 있습니다.")

    # 푸터
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
    생명공학육성시행계획 PDF → Oracle DB 처리 시스템 v2.0 | 2025
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()

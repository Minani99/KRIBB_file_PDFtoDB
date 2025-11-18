#!/usr/bin/env python3
"""
생명공학육성시행계획 PDF → Oracle DB 처리 시스템
Streamlit 웹 UI
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import json
import time
from datetime import datetime
import sys
import os

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from load_oracle_direct import OracleDirectLoader
from config import ORACLE_CONFIG, INPUT_DIR, OUTPUT_DIR, NORMALIZED_OUTPUT_GOVERNMENT_DIR

# 페이지 설정
st.set_page_config(
    page_title="PDF to Oracle DB 처리 시스템",
    page_icon="📄",
    layout="wide"
)

# 세션 상태 초기화
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = []
if 'normalized_stats' not in st.session_state:
    st.session_state.normalized_stats = None
if 'db_stats' not in st.session_state:
    st.session_state.db_stats = None


def save_uploaded_files(uploaded_files):
    """업로드된 파일 저장"""
    INPUT_DIR.mkdir(exist_ok=True)

    saved_files = []
    for file in uploaded_files:
        file_path = INPUT_DIR / file.name
        with open(file_path, 'wb') as f:
            f.write(file.getbuffer())
        saved_files.append(file_path)

    return saved_files


def process_single_pdf(pdf_path, progress_callback=None):
    """단일 PDF 처리"""
    try:
        # OUTPUT_DIR 생성 확인
        OUTPUT_DIR.mkdir(exist_ok=True)

        # 1. PDF → JSON
        if progress_callback:
            progress_callback(f"📄 {pdf_path.name} - PDF 파싱 중...")

        # extract_pdf_to_json은 output_dir를 받아서 자동으로 파일명 생성
        # output_dir에 pdf_path.stem + ".json" 형태로 저장됨
        extract_pdf_to_json(str(pdf_path), str(OUTPUT_DIR))

        # 생성된 JSON 파일 경로
        json_path = OUTPUT_DIR / f"{pdf_path.stem}.json"

        # JSON 파일이 정상 생성되었는지 확인
        if not json_path.exists():
            raise FileNotFoundError(f"JSON 파일이 생성되지 않았습니다: {json_path}")

        return {'file': pdf_path.name, 'status': 'success', 'json_path': str(json_path)}

    except Exception as e:
        return {'file': pdf_path.name, 'status': 'failed', 'error': str(e)}


def normalize_all_jsons(progress_callback=None):
    """모든 JSON 정규화"""
    # OUTPUT_DIR에서 JSON 파일 찾기
    json_files = list(OUTPUT_DIR.glob("*.json"))

    if not json_files:
        st.error(f"❌ {OUTPUT_DIR}에 JSON 파일이 없습니다.")
        return None

    if progress_callback:
        progress_callback(f"📋 {len(json_files)}개 JSON 파일 발견")

    # NORMALIZED_OUTPUT_GOVERNMENT_DIR 생성 확인
    NORMALIZED_OUTPUT_GOVERNMENT_DIR.mkdir(exist_ok=True)

    # 모든 JSON을 하나의 normalizer로 처리
    normalizer = GovernmentStandardNormalizer(
        json_path="batch",
        output_dir=str(NORMALIZED_OUTPUT_GOVERNMENT_DIR)
    )

    success_count = 0

    for i, json_file in enumerate(json_files):
        if progress_callback:
            progress_callback(f"📋 정규화 중: {json_file.name} ({i+1}/{len(json_files)})")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            # JSON 데이터가 비어있지 않은지 확인
            if not json_data or 'pages' not in json_data:
                st.warning(f"⚠️ {json_file.name}: JSON 구조 오류 (pages 키 없음)")
                continue

            normalizer.normalize(json_data)
            success_count += 1

        except json.JSONDecodeError as e:
            st.error(f"❌ {json_file.name}: JSON 파싱 실패 - {e}")
        except Exception as e:
            st.error(f"❌ {json_file.name}: 정규화 실패 - {e}")

    if success_count == 0:
        st.error("❌ 정규화에 성공한 파일이 없습니다.")
        return None

    # CSV 저장
    try:
        normalizer.save_to_csv()

        # 통계
        stats = {
            'master': len(normalizer.data['master']),
            'details': len(normalizer.data['details']),
            'budgets': len(normalizer.data['budgets']),
            'schedules': len(normalizer.data['schedules']),
            'performances': len(normalizer.data['performances'])
        }

        if progress_callback:
            progress_callback(f"✅ 정규화 완료: {success_count}/{len(json_files)}개 파일")

        return stats

    except Exception as e:
        st.error(f"❌ CSV 저장 실패: {e}")
        return None


def load_to_oracle(progress_callback=None):
    """Oracle DB 적재"""
    try:
        # CSV 파일 존재 확인
        csv_files = list(NORMALIZED_OUTPUT_GOVERNMENT_DIR.glob("TB_PLAN_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"CSV 파일이 없습니다: {NORMALIZED_OUTPUT_GOVERNMENT_DIR}")

        if progress_callback:
            progress_callback(f"🔌 Oracle DB 연결 중... ({len(csv_files)}개 CSV 발견)")

        loader = OracleDirectLoader(ORACLE_CONFIG, str(NORMALIZED_OUTPUT_GOVERNMENT_DIR))

        try:
            loader.connect()
        except Exception as e:
            raise Exception(f"Oracle 연결 실패: {e}")

        if progress_callback:
            progress_callback("🏗️ 테이블 생성 중...")

        try:
            loader.create_tables()
        except Exception as e:
            loader.db_manager.close()
            raise Exception(f"테이블 생성 실패: {e}")

        if progress_callback:
            progress_callback("📊 데이터 적재 중...")

        try:
            loader.load_all_tables()
        except Exception as e:
            loader.db_manager.close()
            raise Exception(f"데이터 적재 실패: {e}")

        loader.db_manager.close()

        # 적재된 레코드 수 확인
        total_records = loader.load_stats.get('total_records', 0)

        if total_records == 0:
            raise Exception("적재된 레코드가 0건입니다. 에러 로그를 확인하세요.")

        return loader.load_stats

    except Exception as e:
        error_msg = str(e)

        # ORA 에러 코드 해석
        if "ORA-00001" in error_msg:
            raise Exception(f"중복 키 에러 (ORA-00001): 이미 같은 데이터가 DB에 존재합니다.\n해���: Streamlit을 재시작하거나 DB 데이터를 삭제하세요.")
        elif "ORA-12541" in error_msg:
            raise Exception(f"Oracle 서버 연결 실패 (ORA-12541): 서버가 실행 중인지 확인하세요.")
        elif "ORA-01017" in error_msg:
            raise Exception(f"인증 실패 (ORA-01017): 사용자명/비밀번호를 확인하세요.")
        else:
            raise Exception(f"Oracle DB 적재 실패: {error_msg}")


def display_csv_data(csv_dir):
    """CSV 데이터 표시"""
    csv_path = Path(csv_dir)

    if not csv_path.exists():
        st.warning("❌ CSV 파일이 없습니다.")
        return

    csv_files = [f for f in csv_path.glob("*.csv") if f.name != "raw_data.csv"]

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
                display_df = df.head(100)
                st.dataframe(display_df, width=None)

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
    st.title("📄 생명공학육성시행계획 PDF → Oracle DB 시스템")
    st.markdown("**정부 시행계획 PDF 자동 파싱 및 데이터베이스 적재**")

    # 사이드바
    with st.sidebar:
        st.header("⚙️ 설정")

        enable_db_load = st.checkbox(
            "🗄️ Oracle DB 적재",
            value=True,
            help="체크 해제 시 CSV만 생성"
        )

        st.markdown("---")

        st.subheader("📊 Oracle DB 정보")
        st.text(f"Host: {ORACLE_CONFIG['host']}")
        st.text(f"SID: {ORACLE_CONFIG['sid']}")
        st.text(f"User: {ORACLE_CONFIG['user']}")

        # DB 초기화 버튼
        st.markdown("---")

        if st.button("🗑️ DB 데이터 초기화", type="secondary", use_container_width=True):
            try:
                with st.spinner("DB 초기화 중..."):
                    loader = OracleDirectLoader(ORACLE_CONFIG, str(NORMALIZED_OUTPUT_GOVERNMENT_DIR))

                    # 연결
                    loader.connect()

                    # 테이블 삭제
                    truncated_count = loader.truncate_tables()

                    # 명시적 커밋
                    loader.db_manager.connection.commit()

                    # 연결 종료
                    loader.db_manager.close()

                if truncated_count > 0:
                    st.success(f"✅ DB 데이터 삭제 완료! ({truncated_count}개 테이블)")
                    st.info("ℹ️ 이제 PDF를 다시 처리하면 중복 에러 없이 적재됩니다.")
                else:
                    st.warning("⚠️ 삭제된 테이블이 없습니다. 테이블이 존재하지 않거나 이미 비어있습니다.")

            except Exception as e:
                st.error(f"❌ DB 초기화 실패: {e}")
                import traceback
                with st.expander("🔍 상세 에러"):
                    st.code(traceback.format_exc())

                # 해결 방법 안내
                st.markdown("""
                **💡 수동 해결 방법 (Oracle SQL Developer):**
                ```sql
                -- 역순으로 실행하세요 (FK 제약조건 때문)
                TRUNCATE TABLE TB_PLAN_WEIGHT CASCADE;
                TRUNCATE TABLE TB_PLAN_PERFORMANCE CASCADE;
                TRUNCATE TABLE TB_PLAN_SCHEDULE CASCADE;
                TRUNCATE TABLE TB_PLAN_BUDGET CASCADE;
                TRUNCATE TABLE TB_PLAN_DETAIL CASCADE;
                TRUNCATE TABLE TB_PLAN_MASTER CASCADE;
                COMMIT;
                ```
                
                또는 DELETE 사용:
                ```sql
                DELETE FROM TB_PLAN_WEIGHT;
                DELETE FROM TB_PLAN_PERFORMANCE;
                DELETE FROM TB_PLAN_SCHEDULE;
                DELETE FROM TB_PLAN_BUDGET;
                DELETE FROM TB_PLAN_DETAIL;
                DELETE FROM TB_PLAN_MASTER;
                COMMIT;
                ```
                """)

        st.markdown("---")

        st.info("""
        **처리 단계:**
        1. PDF 업로드
        2. PDF → JSON 변환
        3. JSON → CSV 정규화 (정부 표준)
        4. CSV → Oracle DB 적재
        
        **최신 개선사항:** ✨
        - 정성적 성과 자동 추출
        - 세부일정의 실제 날짜 파싱
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

            # 파일 목록
            with st.expander("📋 선택된 파일", expanded=True):
                for file in uploaded_files:
                    st.write(f"- {file.name} ({file.size:,} bytes)")

            # 처리 시작
            col1, col2 = st.columns([3, 1])

            with col1:
                process_button = st.button(
                    "🚀 전체 파이프라인 실행",
                    type="primary",
                    use_container_width=True
                )

            with col2:
                clear_button = st.button(
                    "🗑️ 초기화",
                    use_container_width=True
                )

            if clear_button:
                st.session_state.processing_results = []
                st.session_state.normalized_stats = None
                st.session_state.db_stats = None
                st.rerun()

            if process_button:
                progress_container = st.container()

                with progress_container:
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
                            def progress_cb(msg):
                                status_text.text(msg)

                            result = process_single_pdf(pdf_file, progress_cb)
                            results.append(result)
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

                        # 4단계: Oracle DB 적재
                        db_stats = None  # ✅ 초기화
                        if enable_db_load:
                            status_text.text("🗄️ Oracle DB 적재 중...")
                            try:
                                db_stats = load_to_oracle(lambda msg: status_text.text(msg))
                                st.session_state.db_stats = db_stats
                                progress_bar.progress(1.0)
                            except Exception as db_error:
                                progress_bar.progress(1.0)
                                st.error(f"❌ Oracle DB 적재 실패: {db_error}")

                                # 에러 상세 정보
                                with st.expander("🔍 DB 에러 상세"):
                                    st.code(str(db_error))

                                    # 해결 방법 안내
                                    st.markdown("""
                                    **해결 방법:**
                                    1. **중복 데이터 에러 (ORA-00001):**
                                       - 아래 SQL을 실행하여 기존 데이터 삭제:
                                       ```sql
                                       TRUNCATE TABLE TB_PLAN_PERFORMANCE;
                                       TRUNCATE TABLE TB_PLAN_SCHEDULE;
                                       TRUNCATE TABLE TB_PLAN_BUDGET;
                                       TRUNCATE TABLE TB_PLAN_DETAIL;
                                       TRUNCATE TABLE TB_PLAN_MASTER;
                                       TRUNCATE TABLE TB_PLAN_WEIGHT;
                                       ```
                                       - 또는 Streamlit 앱을 재시작하세요.
                                    
                                    2. **연결 실패:**
                                       - Oracle 서버가 실행 중인지 확인
                                       - config.py의 접속 정보 확인
                                    """)

                                # DB 적재는 실패했지만 CSV는 생성됨
                                st.warning("⚠️ CSV 파일은 정상 생성되었습니다. 'CSV 데이터' 탭에서 확인 가능합니다.")
                        else:
                            progress_bar.progress(1.0)

                        elapsed = time.time() - start_time

                        status_text.empty()
                        progress_bar.empty()

                        # 완료 메시지
                        st.success(f"✅ 전체 파이프라인 완료! (소요 시간: {elapsed:.1f}초)")
                        st.balloons()

                        # 결과 요약
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("📄 처리 성공", f"{success_count}/{len(results)}")
                        with col2:
                            if norm_stats:
                                st.metric("📊 내역사업", f"{norm_stats['master']}개")
                        with col3:
                            if enable_db_load and db_stats:
                                st.metric("🗄️ DB 적재", f"{db_stats['total_records']:,}건")

                    except Exception as e:
                        st.error(f"❌ 처리 실패: {e}")
                        import traceback
                        with st.expander("🔍 상세 에러"):
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
                st.metric("성공", len(success), delta_color="normal")
            with col3:
                st.metric("실패", len(failed), delta_color="inverse")

            # 성공 목록
            if success:
                st.subheader("✅ 성공")
                for r in success:
                    st.write(f"- {r['file']}")

            # 실패 목록
            if failed:
                st.subheader("❌ 실패")
                for r in failed:
                    st.error(f"- {r['file']}: {r.get('error', '알 수 없는 오류')}")

            # 정규화 통계
            if st.session_state.normalized_stats:
                st.subheader("📊 정규화 통계")
                stats = st.session_state.normalized_stats

                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("마스터", stats['master'])
                with col2:
                    st.metric("상세", stats['details'])
                with col3:
                    st.metric("예산", stats['budgets'])
                with col4:
                    st.metric("일정", stats['schedules'])
                with col5:
                    st.metric("성과", stats['performances'])

                st.info("ℹ️ 성과에는 정량적 성과(특허, 논문)와 정성적 성과(추진실적)가 모두 포함됩니다.")
        else:
            st.info("ℹ️ 아직 처리된 결과가 없습니다.")

    with tab3:
        st.header("CSV 데이터")

        st.markdown("""
        **📋 테이블 구조:**
        - **TB_PLAN_MASTER**: 내역사업 기본 정보
        - **TB_PLAN_DETAIL**: 사업 상세 정보
        - **TB_PLAN_BUDGET**: 연도별 예산 (실적/계획 구분)
        - **TB_PLAN_SCHEDULE**: 일정 정보 (실제 월 정보 우선 파싱 ✨)
        - **TB_PLAN_PERFORMANCE**: 성과 정보 (정량적 + 정성적 ✨)
        """)

        if NORMALIZED_OUTPUT_GOVERNMENT_DIR.exists():
            display_csv_data(NORMALIZED_OUTPUT_GOVERNMENT_DIR)
        else:
            st.info("ℹ️ CSV 데이터가 없습니다. PDF를 업로드하고 처리하세요.")

    with tab4:
        st.header("Oracle DB 통계")

        if st.session_state.db_stats:
            stats = st.session_state.db_stats

            st.metric("총 적재 레코드", f"{stats['total_records']:,}건")

            st.subheader("📊 테이블별 통계")

            table_data = []
            for table, count in stats['records_by_table'].items():
                table_data.append({'테이블': table, '레코드 수': f"{count:,}건"})

            df = pd.DataFrame(table_data)
            st.dataframe(df, width=None)

        else:
            st.info("ℹ️ Oracle DB 적재 통계가 없습니다.")

            if not enable_db_load:
                st.warning("⚠️ 'Oracle DB 적재' 옵션이 비활성화되어 있습니다.")

    # 푸터
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
    생명공학육성시행계획 PDF → Oracle DB 처리 시스템 v1.0 | 2025
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()


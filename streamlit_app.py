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
import sys
import os

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from extract_pdf_to_json import extract_pdf_to_json
from normalize_government_standard import GovernmentStandardNormalizer
from load_oracle_direct import OracleDirectLoader
from config import ORACLE_CONFIG, INPUT_DIR, OUTPUT_DIR, NORMALIZED_OUTPUT_GOVERNMENT_DIR, ORACLE_CONFIG_DEV

# 페이지 설정
st.set_page_config(
    page_title="PDF to Oracle DB 처리 시스템",
    page_icon="📄",
    layout="wide"
)

SERVER_INPUT_DIR = Path(INPUT_DIR).resolve()
SERVER_OUTPUT_DIR = Path(OUTPUT_DIR).resolve()
SERVER_NORMALIZED_DIR = Path(NORMALIZED_OUTPUT_GOVERNMENT_DIR).resolve()

# 서버에서 디렉토리 생성 (앱 시작 시 한 번만)
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
    """업로드된 파일 저장 (서버 컴퓨터에 저장)"""
    SERVER_INPUT_DIR.mkdir(exist_ok=True)

    saved_files = []
    for file in uploaded_files:
        file_path = SERVER_INPUT_DIR / file.name
        with open(file_path, 'wb') as f:
            f.write(file.getbuffer())
        saved_files.append(file_path)

    return saved_files


def process_single_pdf(pdf_path, progress_callback=None):
    """단일 PDF 처리 (서버에서 실행)"""
    try:
        # OUTPUT_DIR 생성 확인
        SERVER_OUTPUT_DIR.mkdir(exist_ok=True)

        # 1. PDF → JSON
        if progress_callback:
            progress_callback(f"📄 {pdf_path.name} - PDF 파싱 중...")

        # PDF 파일 존재 확인
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF 파일이 없습니다: {pdf_path}")

        # PDF 파일 크기 확인
        file_size = pdf_path.stat().st_size
        if file_size == 0:
            raise ValueError(f"PDF 파일이 비어있습니다: {pdf_path}")

        st.info(f"📄 처리 중: {pdf_path.name} ({file_size:,} bytes)")

        # extract_pdf_to_json은 output_dir를 받아서 자동으로 파일명 생성
        # output_dir에 pdf_path.stem + ".json" 형태로 저장됨
        try:
            extract_pdf_to_json(str(pdf_path), str(SERVER_OUTPUT_DIR))
        except Exception as extract_error:
            raise Exception(f"PDF 추출 실패: {extract_error}")

        # 생성된 JSON 파일 경로
        json_path = SERVER_OUTPUT_DIR / f"{pdf_path.stem}.json"

        # JSON 파일이 정상 생성되었는지 확인
        if not json_path.exists():
            raise FileNotFoundError(f"JSON 파일이 생성되지 않았습니다: {json_path}")

        # JSON 파일 크기 확인
        json_size = json_path.stat().st_size
        if json_size == 0:
            raise ValueError(f"JSON 파일이 비어있습니다: {json_path}")

        # JSON 내용 검증
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)

            pages_count = len(json_data.get('pages', []))
            st.success(f"✅ {pdf_path.name}: JSON 생성 완료 ({pages_count}페이지, {json_size:,} bytes)")

        except json.JSONDecodeError as e:
            raise ValueError(f"JSON 파싱 실패: {e}")

        return {'file': pdf_path.name, 'status': 'success', 'json_path': str(json_path), 'pages': pages_count}

    except Exception as e:
        st.error(f"❌ {pdf_path.name}: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return {'file': pdf_path.name, 'status': 'failed', 'error': str(e)}


def normalize_all_jsons(progress_callback=None):
    """모든 JSON 정규화 (서버에서 실행) - main.py 방식으로 수정"""
    # OUTPUT_DIR에서 JSON 파일 찾기
    json_files = list(SERVER_OUTPUT_DIR.glob("*.json"))
    json_files = [f for f in json_files if not f.name.startswith('batch_')]

    if not json_files:
        st.error(f"❌ {SERVER_OUTPUT_DIR}에 JSON 파일이 없습니다.")
        return None

    if progress_callback:
        progress_callback(f"📋 {len(json_files)}개 JSON 파일 발견")

    st.info(f"처리할 파일: {', '.join([f.name for f in json_files])}")
    SERVER_NORMALIZED_DIR.mkdir(exist_ok=True)

    try:
        # 1. 모든 JSON 로드
        all_json_data = []
        for i, json_file in enumerate(json_files, 1):
            if progress_callback:
                progress_callback(f"📂 JSON 로드 중: {json_file.name} ({i}/{len(json_files)})")

            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    all_json_data.append(json_data)
            except Exception as e:
                st.error(f"❌ JSON 로드 실패 {json_file.name}: {e}")

        st.info(f"✅ {len(all_json_data)}개 JSON 로드 완료")

        if not all_json_data:
            st.error("❌ 로드된 JSON이 없습니다.")
            return None

        # 2. Oracle DB 연결 (PLAN_ID 매칭용)
        from oracle_db_manager import OracleDBManager
        db_manager = None

        try:
            db_manager = OracleDBManager(ORACLE_CONFIG)
            db_manager.connect()
            st.success("🔗 DB 연결 성공 (PLAN_ID 매칭용)")
        except Exception as e:
            st.warning(f"⚠️ DB 연결 실패 (신규 PLAN_ID로 생성): {e}")
            db_manager = None

        # 3. 첫 번째 파일로 normalizer 초기화 (DB 연결 전달)
        if progress_callback:
            progress_callback("📋 데이터 정규화 시작...")

        normalizer = GovernmentStandardNormalizer(
            str(json_files[0]),
            str(SERVER_NORMALIZED_DIR),
            db_manager=db_manager  # ✅ DB 연결 전달
        )

        # 4. 각 JSON 파일별로 처리 (파일명에서 연도 추출 후 누적)
        for json_file, json_data in zip(json_files, all_json_data):
            if progress_callback:
                progress_callback(f"📋 정규화 중: {json_file.name}")

            # 파일명에서 연도 추출
            import re
            filename = json_file.stem
            year_match = re.search(r'(20\d{2})', filename)

            if year_match:
                doc_year = int(year_match.group(1))
                st.info(f"📅 {filename} -> {doc_year}년도 데이터 처리 중...")

                # ✅ 연도별로 컨텍스트 업데이트 (main.py와 동일)
                normalizer.current_context['document_year'] = doc_year
                normalizer.current_context['performance_year'] = doc_year - 1
                normalizer.current_context['plan_year'] = doc_year

            # 정규화 실행 (데이터 누적)
            normalizer.normalize(json_data)

        # 5. 한 번에 CSV 저장 (main.py와 동일)
        if progress_callback:
            progress_callback("💾 CSV 저장 중...")

        normalizer.save_to_csv()

        # DB 연결 종료
        if db_manager:
            db_manager.close()
            st.info("🔌 DB 연결 종료")

        # 6. 통계 출력
        stats = {
            'plan_data': len(normalizer.data['plan_data']),
            'budgets': len(normalizer.data['budgets']),
            'schedules': len(normalizer.data['schedules']),
            'performances': len(normalizer.data['performances']),
            'achievements': len(normalizer.data['achievements'])
        }

        st.success(f"""
        ✅ 정규화 완료!
        - 내역사업: {stats['plan_data']}개
        - 예산: {stats['budgets']}건
        - 일정: {stats['schedules']}건
        - 성과: {stats['performances']}건
        - 대표성과: {stats['achievements']}건
        """)

        if progress_callback:
            progress_callback(f"✅ 정규화 완료: {stats['plan_data']}개 내역사업")

        return stats

    except Exception as e:
        st.error(f"❌ 정규화 실패: {e}")
        import traceback
        st.code(traceback.format_exc())
        return None


def load_to_oracle(progress_callback=None):
    """Oracle DB 적재 (서버에서 실행)"""
    try:
        # CSV 파일 존재 확인
        csv_files = list(SERVER_NORMALIZED_DIR.glob("TB_PLAN_*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"CSV 파일이 없습니다: {SERVER_NORMALIZED_DIR}")

        if progress_callback:
            progress_callback(f"🔌 Oracle DB 연결 중... ({len(csv_files)}개 CSV 발견)")

        # 2개 DB 연결: 읽기(BICS) + 쓰기(BICS_DEV)
        loader = OracleDirectLoader(
            db_config_read=ORACLE_CONFIG,
            db_config_write=ORACLE_CONFIG_DEV,
            csv_dir=str(SERVER_NORMALIZED_DIR)
        )

        try:
            loader.connect()
        except Exception as e:
            raise Exception(f"Oracle 연결 실패: {e}")

        if progress_callback:
            progress_callback("🔍 기존 TB_PLAN_DATA와 매칭 중...")

        try:
            # ✅ 매칭 기반 적재 실행
            # - BICS의 TB_PLAN_DATA를 BICS_DEV로 복사 (FK용)
            # - 기존 BICS.TB_PLAN_DATA 조회 (매칭용)
            # - CSV와 매칭 (YEAR, BIZ_NM, DETAIL_BIZ_NM 기준)
            # - 매칭 리포트 생성
            # - 하위 4개 테이블 BICS_DEV에 적재
            loader.load_with_matching()
        except Exception as e:
            loader.close()
            raise Exception(f"데이터 적재 실패: {e}")

        loader.close()

        # 적재 통계
        total_records = loader.load_stats.get('total_records', 0)
        matched = loader.load_stats.get('matched', 0)
        unmatched = loader.load_stats.get('unmatched', 0)
        diff_found = loader.load_stats.get('diff_found', 0)

        if total_records == 0 and matched == 0:
            raise Exception(f"적재된 레코드가 0건입니다.\n매칭 성공: {matched}건, 실패: {unmatched}건\n차이점 발견: {diff_found}건")

        return loader.load_stats

    except Exception as e:
        error_msg = str(e)

        # ORA 에러 코드 해석
        if "ORA-00001" in error_msg:
            raise Exception(f"중복 키 에러 (ORA-00001): 이미 같은 데이터가 하위 테이블에 존재합니다.\n해결: 사이드바에서 'DB 데이터 초기화' 버튼을 누르세요.")
        elif "ORA-02291" in error_msg:
            raise Exception(f"FK 제약 조건 위반 (ORA-02291): 부모 키(PLAN_ID)를 찾을 수 없습니다.\n기존 TB_PLAN_DATA에 해당 내역사업이 없을 수 있습니다.\n매칭 리포트를 확인하세요: {SERVER_NORMALIZED_DIR}/matching_reports/")
        elif "ORA-12541" in error_msg:
            raise Exception(f"Oracle 서버 연결 실패 (ORA-12541): 서버가 실행 중인지 확인하세요.")
        elif "ORA-01017" in error_msg:
            raise Exception(f"인증 실패 (ORA-01017): 사용자명/비밀번호를 확인하세요. (현재: {ORACLE_CONFIG['user']})")
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
        st.text("🔍 읽기용 (BICS):")
        st.text(f"  Host: {ORACLE_CONFIG['host']}")
        st.text(f"  User: {ORACLE_CONFIG['user']}")
        st.text("")
        st.text("✍️ 쓰기용 (BICS_DEV):")
        st.text(f"  Host: {ORACLE_CONFIG_DEV['host']}")
        st.text(f"  User: {ORACLE_CONFIG_DEV['user']}")

        # DB 초기화 버튼
        st.markdown("---")

        if st.button("🗑️ BICS_DEV 하위테이블 초기화", type="secondary", use_container_width=True):
            try:
                with st.spinner("BICS_DEV 하위 테이블 초기화 중..."):
                    loader = OracleDirectLoader(
                        db_config_read=ORACLE_CONFIG,
                        db_config_write=ORACLE_CONFIG_DEV,
                        csv_dir=str(SERVER_NORMALIZED_DIR)
                    )

                    # 연결
                    loader.connect()

                    # BICS_DEV의 하위 테이블만 삭제
                    cursor = loader.db_manager_write.connection.cursor()
                    deleted_tables = []

                    for table in ['TB_PLAN_ACHIEVEMENTS', 'TB_PLAN_PERFORMANCE', 'TB_PLAN_SCHEDULE', 'TB_PLAN_BUDGET']:
                        try:
                            cursor.execute(f"DELETE FROM {table}")
                            deleted_count = cursor.rowcount
                            loader.db_manager_write.connection.commit()
                            deleted_tables.append(f"{table}: {deleted_count}건")
                            st.info(f"✅ {table} 삭제: {deleted_count}건")
                        except Exception as e:
                            st.warning(f"⚠️ {table} 삭제 실패: {e}")

                    cursor.close()

                    # 연결 종료
                    loader.close()

                if deleted_tables:
                    st.success(f"✅ BICS_DEV 하위 테이블 초기화 완료!")
                    with st.expander("📋 삭제 내역"):
                        for item in deleted_tables:
                            st.text(f"• {item}")
                    st.info("ℹ️ TB_PLAN_DATA는 유지되었습니다. 이제 PDF를 다시 처리하면 중복 없이 적재됩니다.")
                else:
                    st.warning("⚠️ 삭제된 테이블이 없습니다.")

            except Exception as e:
                st.error(f"❌ DB 초기화 실패: {e}")
                import traceback
                with st.expander("🔍 상세 에러"):
                    st.code(traceback.format_exc())

                # 해결 방법 안내
                st.markdown("""
                **💡 수동 해결 방법 (Oracle SQL Developer):**
                
                ⚠️ TB_PLAN_DATA는 삭제하지 마세요! (BICS에서 복사된 원본 유지)
                
                ```sql
                -- BICS_DEV 스키마의 하위 테이블만 삭제
                DELETE FROM BICS_DEV.TB_PLAN_ACHIEVEMENTS;
                DELETE FROM BICS_DEV.TB_PLAN_PERFORMANCE;
                DELETE FROM BICS_DEV.TB_PLAN_SCHEDULE;
                DELETE FROM BICS_DEV.TB_PLAN_BUDGET;
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
        - 2개 DB 연결 (BICS 읽기 + BICS_DEV 쓰기)
        - PLAN_ID 자동 매칭 (100%)
        - TB_PLAN_DATA 자동 복사 (BICS → BICS_DEV)
        - 하위 4개 테이블 완전 적재
        - 특수문자 처리
        - FK 제약조건 자동 처리
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
                                       TRUNCATE TABLE TB_PLAN_ACHIEVEMENTS;
                                       TRUNCATE TABLE TB_PLAN_PERFORMANCE;
                                       TRUNCATE TABLE TB_PLAN_SCHEDULE;
                                       TRUNCATE TABLE TB_PLAN_BUDGET;
                                       TRUNCATE TABLE TB_PLAN_DATA;
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
                        if enable_db_load and db_stats:
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("📄 처리 성공", f"{success_count}/{len(results)}")
                            with col2:
                                if norm_stats:
                                    st.metric("📊 내역사업", f"{norm_stats['plan_data']}개")
                            with col3:
                                st.metric("✅ 매칭 성공", f"{db_stats.get('matched', 0)}건")
                            with col4:
                                st.metric("🗄️ DB 적재", f"{db_stats['total_records']:,}건")

                            # 매칭 실패 경고
                            if db_stats.get('unmatched', 0) > 0:
                                st.warning(f"⚠️ 매칭 실패: {db_stats['unmatched']}건 - 리포트를 확인하세요!")
                                st.info(f"📄 리포트 위치: `{SERVER_NORMALIZED_DIR}/matching_reports/`")

                            # 차이점 발견 안내
                            if db_stats.get('diff_found', 0) > 0:
                                st.info(f"ℹ️ {db_stats['diff_found']}건의 레코드에서 기존 데이터와 차이점이 발견되었습니다. (diff_report.csv 확인)")
                        else:
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("📄 처리 성공", f"{success_count}/{len(results)}")
                            with col2:
                                if norm_stats:
                                    st.metric("📊 내역사업", f"{norm_stats['plan_data']}개")
                            with col3:
                                st.metric("📋 CSV 생성", "완료")

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
                    st.metric("메인 데이터", stats['plan_data'])
                with col2:
                    st.metric("예산", stats['budgets'])
                with col3:
                    st.metric("일정", stats['schedules'])
                with col4:
                    st.metric("성과", stats['performances'])
                with col5:
                    st.metric("대표성과", stats['achievements'])

                st.info("ℹ️ TB_PLAN_DATA(메인) + 4개 하위 테이블로 구성됩니다.")
        else:
            st.info("ℹ️ 아직 처리된 결과가 없습니다.")

    with tab3:
        st.header("CSV 데이터")

        st.markdown("""
        **📋 테이블 구조:**
        - **TB_PLAN_DATA**: 내역사업 메인 정보 (회사 기존 43개 컬럼)
        - **TB_PLAN_BUDGET**: 연도별 예산 상세 (실적/계획 구분)
        - **TB_PLAN_SCHEDULE**: 일정 상세 (실제 월 정보 우선 파싱 ✨)
        - **TB_PLAN_PERFORMANCE**: 성과 상세 (정량적 + 정성적 ✨)
        - **TB_PLAN_ACHIEVEMENTS**: 대표성과
        """)

        if SERVER_NORMALIZED_DIR.exists():
            display_csv_data(SERVER_NORMALIZED_DIR)
        else:
            st.info("ℹ️ CSV 데이터가 없습니다. PDF를 업로드하고 처리하세요.")

    with tab4:
        st.header("Oracle DB 통계")

        if st.session_state.db_stats:
            stats = st.session_state.db_stats

            # 매칭 통계
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("총 적재 레코드", f"{stats['total_records']:,}건")
            with col2:
                st.metric("✅ 매칭 성공", f"{stats.get('matched', 0)}건", delta_color="normal")
            with col3:
                st.metric("❌ 매칭 실패", f"{stats.get('unmatched', 0)}건", delta_color="inverse")
            with col4:
                st.metric("⚠️ 차이점 발견", f"{stats.get('diff_found', 0)}건", delta_color="off")

            # 매칭 실패 레코드 표시
            if stats.get('unmatched', 0) > 0:
                st.warning(f"⚠️ {stats['unmatched']}건의 레코드가 기존 TB_PLAN_DATA와 매칭되지 않았습니다.")

                # unmatched_records.csv 읽기
                unmatched_csv = SERVER_NORMALIZED_DIR / "matching_reports" / "unmatched_records.csv"
                if unmatched_csv.exists():
                    with st.expander("📄 매칭 실패 레코드 상세 보기", expanded=True):
                        try:
                            unmatched_df = pd.read_csv(unmatched_csv, encoding='utf-8-sig')

                            st.write(f"**총 {len(unmatched_df)}건의 매칭 실패 레코드**")

                            # 필터링 옵션
                            col_filter1, col_filter2 = st.columns(2)
                            with col_filter1:
                                year_filter = st.multiselect(
                                    "연도 필터",
                                    options=sorted(unmatched_df['year'].unique()),
                                    default=sorted(unmatched_df['year'].unique())
                                )
                            with col_filter2:
                                search_text = st.text_input("검색 (BIZ_NM 또는 DETAIL_BIZ_NM)", "")

                            # 필터 적용
                            filtered_df = unmatched_df[unmatched_df['year'].isin(year_filter)]
                            if search_text:
                                filtered_df = filtered_df[
                                    filtered_df['biz_nm'].str.contains(search_text, case=False, na=False) |
                                    filtered_df['detail_biz_nm'].str.contains(search_text, case=False, na=False)
                                ]

                            # 표시할 컬럼 선택
                            display_cols = ['csv_index', 'year', 'biz_nm', 'detail_biz_nm', 'reason']
                            if all(col in filtered_df.columns for col in display_cols):
                                display_df = filtered_df[display_cols]
                            else:
                                display_df = filtered_df

                            st.dataframe(
                                display_df,
                                use_container_width=True,
                                height=400
                            )

                            # 다운로드 버튼
                            csv_data = unmatched_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                            st.download_button(
                                label="📥 매칭 실패 레코드 다운로드 (CSV)",
                                data=csv_data,
                                file_name="unmatched_records.csv",
                                mime='text/csv'
                            )

                            # 패턴 분석
                            st.subheader("🔍 매칭 실패 패턴 분석")

                            # BIZ_NM = DETAIL_BIZ_NM인 경우
                            same_name = unmatched_df[unmatched_df['biz_nm'] == unmatched_df['detail_biz_nm']]
                            if len(same_name) > 0:
                                st.info(f"📌 BIZ_NM과 DETAIL_BIZ_NM이 동일한 경우: {len(same_name)}건 (신규 사업일 가능성)")

                            # 연도별 매칭 실패 건수
                            year_counts = unmatched_df['year'].value_counts().sort_index()
                            st.write("**연도별 매칭 실패 건수:**")
                            for year, count in year_counts.items():
                                st.write(f"- {year}년: {count}건")

                        except Exception as e:
                            st.error(f"❌ 매칭 실패 레코드 로드 실패: {e}")
                else:
                    st.info("📄 매칭 리포트: `normalized_output_government/matching_reports/unmatched_records.csv`")

            # 차이점 발견 레코드 표시
            if stats.get('diff_found', 0) > 0:
                st.info(f"ℹ️ {stats['diff_found']}건의 레코드에서 기존 데이터와 차이점이 발견되었습니다.")

                # diff_report.csv 읽기
                diff_csv = SERVER_NORMALIZED_DIR / "matching_reports" / "diff_report.csv"
                if diff_csv.exists():
                    with st.expander("📄 차이점 발견 레코드 상세 보기"):
                        try:
                            diff_df = pd.read_csv(diff_csv, encoding='utf-8-sig')

                            st.write(f"**총 {len(diff_df)}건의 차이점 발견**")

                            st.dataframe(
                                diff_df,
                                use_container_width=True,
                                height=300
                            )

                            # 다운로드 버튼
                            csv_data = diff_df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
                            st.download_button(
                                label="📥 차이점 리포트 다운로드 (CSV)",
                                data=csv_data,
                                file_name="diff_report.csv",
                                mime='text/csv'
                            )
                        except Exception as e:
                            st.error(f"❌ 차이점 리포트 로드 실패: {e}")
                else:
                    st.info("📄 차이점 리포트: `normalized_output_government/matching_reports/diff_report.csv`")

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


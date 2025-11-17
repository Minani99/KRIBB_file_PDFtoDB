#!/usr/bin/env python3
"""
PDF 처리 시스템 Streamlit UI
대량 PDF 업로드, 실시간 진행률, 결과 시각화
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
from batch_processor import BatchPDFProcessor, create_pdf_processor_func

# 페이지 설정
st.set_page_config(
    page_title="PDF to Database 처리 시스템",
    page_icon="📄",
    layout="wide"
)

# 세션 상태 초기화
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = None
if 'normalized_data' not in st.session_state:
    st.session_state.normalized_data = None


def save_uploaded_files(uploaded_files, input_dir="input_temp"):
    """업로드된 파일 저장"""
    input_path = Path(input_dir)
    input_path.mkdir(exist_ok=True)

    saved_files = []
    for file in uploaded_files:
        file_path = input_path / file.name
        with open(file_path, 'wb') as f:
            f.write(file.getbuffer())
        saved_files.append(file_path)

    return saved_files


def process_pdfs(pdf_files, batch_size, max_workers):
    """PDF 파일 처리"""
    output_dir = Path("output_temp")
    output_dir.mkdir(exist_ok=True)

    # 배치 프로세서 생성
    processor = BatchPDFProcessor(
        input_dir="input_temp",
        output_dir=str(output_dir),
        batch_size=batch_size,
        max_workers=max_workers,
        use_multiprocessing=False  # Streamlit과 호환성
    )

    # 처리 함수
    pdf_processor = create_pdf_processor_func(str(output_dir))

    # 처리 실행
    summary = processor.process_all(
        pdf_processor,
        recursive=False,
        save_results=True
    )

    return summary, processor


def display_results(summary):
    """처리 결과 표시"""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("총 파일", f"{summary['total_files']:,}개")
    with col2:
        st.metric("처리 성공", f"{summary['processed']:,}개", delta_color="normal")
    with col3:
        st.metric("처리 실패", f"{summary['failed']:,}개", delta_color="inverse")
    with col4:
        st.metric("성공률", summary['success_rate'])

    if summary['duration_seconds']:
        st.info(f"⏱️ 소요 시간: {summary['duration_seconds']:.1f}초")

    # 오류 표시
    if summary['total_errors'] > 0:
        with st.expander(f"⚠️ 오류 상세 ({summary['total_errors']}건)", expanded=False):
            error_df = pd.DataFrame(summary['errors'])
            st.dataframe(error_df, use_container_width=True)


def display_normalized_data(normalized_dir):
    """정규화된 데이터 표시"""
    normalized_path = Path(normalized_dir)

    if not normalized_path.exists():
        st.warning("정규화된 데이터가 없습니다.")
        return

    csv_files = list(normalized_path.glob("*.csv"))

    if not csv_files:
        st.warning("CSV 파일이 없습니다.")
        return

    # 탭으로 각 테이블 표시
    tab_names = [f.stem for f in csv_files]
    tabs = st.tabs(tab_names)

    for tab, csv_file in zip(tabs, csv_files):
        with tab:
            try:
                df = pd.read_csv(csv_file)
                st.write(f"**{csv_file.stem}** ({len(df):,} 레코드)")
                st.dataframe(df, use_container_width=True)

                # 다운로드 버튼
                csv_data = df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label=f"📥 {csv_file.stem} 다운로드",
                    data=csv_data,
                    file_name=csv_file.name,
                    mime='text/csv'
                )
            except Exception as e:
                st.error(f"파일 로드 실패: {e}")


def main():
    """메인 UI"""

    # 헤더
    st.title("📄 PDF to Database 처리 시스템")
    st.markdown("정부/공공기관 표준 데이터 처리 - 대량 PDF 자동화")

    # 사이드바 - 설정
    with st.sidebar:
        st.header("⚙️ 처리 설정")

        batch_size = st.slider(
            "배치 크기",
            min_value=5,
            max_value=50,
            value=10,
            help="한 번에 처리할 파일 수"
        )

        max_workers = st.slider(
            "병렬 작업자",
            min_value=1,
            max_value=8,
            value=4,
            help="동시에 실행할 작업자 수"
        )

        normalize_data = st.checkbox(
            "데이터 정규화",
            value=True,
            help="처리 후 데이터 정규화 수행"
        )

        st.markdown("---")
        st.info("""
        **처리 단계:**
        1. PDF 업로드
        2. PDF → JSON 변환
        3. 데이터 정규화
        4. 결과 확인 및 다운로드
        """)

    # 메인 영역
    tab1, tab2, tab3 = st.tabs(["📤 파일 업로드", "📊 처리 결과", "📁 데이터 확인"])

    with tab1:
        st.header("PDF 파일 업로드")

        uploaded_files = st.file_uploader(
            "PDF 파일 선택 (여러 개 가능)",
            type=['pdf'],
            accept_multiple_files=True
        )

        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)}개 파일 선택됨")

            # 파일 목록 표시
            with st.expander("선택된 파일 목록", expanded=False):
                for file in uploaded_files:
                    st.write(f"- {file.name} ({file.size:,} bytes)")

            # 처리 시작 버튼
            if st.button("🚀 처리 시작", type="primary", use_container_width=True):

                progress_container = st.container()

                with progress_container:
                    st.info("📝 파일 저장 중...")
                    saved_files = save_uploaded_files(uploaded_files)

                    st.info("🔄 PDF 처리 중...")
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    start_time = time.time()

                    try:
                        # PDF 처리
                        summary, processor = process_pdfs(
                            saved_files,
                            batch_size,
                            max_workers
                        )

                        st.session_state.processing_results = summary

                        # 정규화
                        if normalize_data and summary['processed'] > 0:
                            st.info("📋 데이터 정규화 중...")

                            output_dir = Path("output_temp")
                            normalized_dir = Path("normalized_output_temp")
                            normalized_dir.mkdir(exist_ok=True)

                            # 각 JSON 파일 정규화
                            json_files = list(output_dir.glob("*.json"))

                            for i, json_file in enumerate(json_files):
                                status_text.text(f"정규화 중: {json_file.name}")
                                progress_bar.progress((i + 1) / len(json_files))

                                try:
                                    with open(json_file, 'r', encoding='utf-8') as f:
                                        json_data = json.load(f)

                                    normalizer = GovernmentStandardNormalizer(
                                        str(json_file),
                                        str(normalized_dir)
                                    )
                                    normalizer.normalize(json_data)
                                    normalizer.save_to_csv()

                                except Exception as e:
                                    st.warning(f"정규화 실패 {json_file.name}: {e}")

                            st.session_state.normalized_data = str(normalized_dir)

                        elapsed_time = time.time() - start_time

                        st.success(f"✅ 처리 완료! (소요 시간: {elapsed_time:.1f}초)")
                        st.balloons()

                        # 자동으로 결과 탭으로 전환
                        st.info("👉 '처리 결과' 탭에서 결과를 확인하세요.")

                    except Exception as e:
                        st.error(f"❌ 처리 실패: {e}")

    with tab2:
        st.header("처리 결과")

        if st.session_state.processing_results:
            display_results(st.session_state.processing_results)

            # 상세 결과 다운로드
            if st.button("📥 상세 결과 다운로드 (JSON)"):
                results_json = json.dumps(
                    st.session_state.processing_results,
                    ensure_ascii=False,
                    indent=2,
                    default=str
                )
                st.download_button(
                    label="다운로드",
                    data=results_json,
                    file_name=f"processing_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        else:
            st.info("아직 처리된 결과가 없습니다. '파일 업로드' 탭에서 PDF를 업로드하세요.")

    with tab3:
        st.header("정규화된 데이터")

        if st.session_state.normalized_data:
            display_normalized_data(st.session_state.normalized_data)
        else:
            st.info("정규화된 데이터가 없습니다.")
            st.markdown("""
            데이터를 확인하려면:
            1. '파일 업로드' 탭에서 PDF 업로드
            2. '데이터 정규화' 옵션 활성화
            3. 처리 완료 후 여기서 확인
            """)

    # 푸터
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: gray;'>
    PDF to Database Processing System v2.0 | Made with Streamlit
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()


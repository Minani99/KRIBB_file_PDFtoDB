#!/bin/bash

# Oracle 데이터 적재 실행 스크립트
# 사용법: ./run_oracle_load.sh [옵션]

echo "======================================"
echo "Oracle 데이터베이스 적재 시작"
echo "======================================"
echo ""

# Python 환경 확인
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3가 설치되어 있지 않습니다."
    exit 1
fi

# 필요한 패키지 확인
echo "📦 필요한 패키지 확인 중..."
pip install -q oracledb pandas

# 옵션 처리
TRUNCATE_OPTION=""
if [ "$1" = "--truncate" ]; then
    TRUNCATE_OPTION="--truncate"
    echo "⚠️  기존 데이터를 삭제하고 재적재합니다."
fi

TEST_OPTION=""
if [ "$1" = "--test" ]; then
    TEST_OPTION="--test"
    echo "🧪 테스트 모드로 실행합니다 (10건만 처리)."
fi

# 데이터 파일 확인
echo ""
echo "📁 데이터 파일 확인 중..."
if [ ! -d "normalized" ]; then
    echo "❌ normalized 폴더가 없습니다."
    exit 1
fi

FILES=(
    "normalized/sub_projects.csv"
    "normalized/normalized_overviews.csv"
    "normalized/normalized_budgets.csv"
    "normalized/normalized_schedules.csv"
)

for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✅ $file"
    else
        echo "  ⚠️  $file (없음)"
    fi
done

# 실행
echo ""
echo "🚀 적재 스크립트 실행 중..."
echo "======================================"

python3 load_oracle_final.py $TRUNCATE_OPTION $TEST_OPTION

# 결과 확인
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ 적재가 성공적으로 완료되었습니다!"
    echo ""
    echo "📊 다음 명령으로 검증할 수 있습니다:"
    echo "  python3 validate_oracle_data.py"
else
    echo ""
    echo "❌ 적재 중 오류가 발생했습니다."
    echo "  로그 파일을 확인하세요: oracle_load.log"
    exit 1
fi
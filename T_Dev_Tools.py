# 이 코드의 이름은 T_Dev_Tools.py
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import argparse
import ast
from datetime import datetime, timedelta
import C_Global_Config as cfg  # [핵심] 모든 설정의 기준

# --- [Tool 1] Repo Map Generator ---
def generate_repo_map():
    """
    [Tool 1] Project Repository Map 생성
    - 프로젝트 내 .py, .parquet, .yml 파일의 구조를 분석하여 repo_map.md 생성
    """
    target_file = "repo_map.md"
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    lines = [
        "# 🗺️ Project Repository Map",
        f"- **Generated Date**: {now_str}",
        "- **Status**: Managed by GitHub Actions (Retention: 5 Days)",
        "\n---"
    ]

    # 탐색 디렉토리 설정
    search_dirs = [".", cfg.BASE_DIR, ".github/workflows"]
    
    for d in search_dirs:
        if not os.path.exists(d):
            continue
            
        lines.append(f"\n## 📂 Directory: {d}")
        items = sorted(os.listdir(d))
        
        for item in items:
            path = os.path.join(d, item)
            
            # 1. 파이썬 파일 분석
            if item.endswith(".py"):
                functions = []
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        for line in f:
                            if line.strip().startswith("def "):
                                func_name = line.split("def ")[1].split("(")[0].strip()
                                functions.append(f"`{func_name}()`")
                except:
                    pass
                func_str = ", ".join(functions) if functions else "*None*"
                lines.append(f"### 📄 `{item}`")
                lines.append(f"- **Functions**: {func_str}")

            # 2. 데이터 파일 분석
            elif item.endswith(".parquet"):
                try:
                    df = pd.read_parquet(path)
                    cols = ", ".join([f"`{c}`" for c in df.columns])
                    lines.append(f"### 📊 `{item}`")
                    lines.append(f"- **All Columns**: {cols}")
                except:
                    lines.append(f"### 📊 `{item}` (Error reading file)")

            # 3. 워크플로우 분석
            elif item.endswith(".yml"):
                lines.append(f"### ⚙️ `{item}` (Workflow)")

    with open(target_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    print(f"✅ {target_file} 가 성공적으로 생성/갱신되었습니다.")

# --- [Tool 2] Strategy Sandbox (시뮬레이션 엔진) ---
import pandas as pd
import numpy as np
import os
import C_Global_Config as cfg # 관례에 따른 설정 로드

def strategy_sandbox(target_date, stock_code=None):
    """
    [Tool 2] 전략 시뮬레이션 샌드박스 (고도화 버전)
    - NameError 수정 및 Waterfall 정밀 분석 기능 통합
    """
    # 1. 데이터 로드 및 사전 준비
    if not os.path.exists(cfg.PATH_ADB_SUM):
        print(f"❌ 데이터 파일을 찾을 수 없습니다: {cfg.PATH_ADB_SUM}")[cite: 2]
        return

    full_df = pd.read_parquet(cfg.PATH_ADB_SUM).sort_values(['종목코드', '날짜'])[cite: 3]
    target_dt = pd.to_datetime(target_date)
    
    # 분석일(D-1) 산출
    available_dates = sorted(full_df[full_df['날짜'] < target_dt]['날짜'].unique())
    if not available_dates:
        print("❌ 시뮬레이션을 위한 과거 데이터가 부족합니다.")
        return
    d_minus_1 = available_dates[-1]

    # 분석 대상 데이터프레임(df_d1) 먼저 정의 (오류 해결 핵심)[cite: 1]
    df_d1 = full_df[full_df['날짜'] == d_minus_1].copy()
    if stock_code:
        df_d1 = df_d1[df_d1['종목코드'] == stock_code]

    # 지표 계산 (필요한 칼럼: 거래대금, 기금순매수, 종가, 최고가 등)
    # (효율을 위해 df_d1 대상으로만 계산)
    full_df['거래대금_20평균'] = full_df.groupby('종목코드')['거래대금'].transform(lambda x: x.rolling(window=20).mean())
    full_df['최고가_20일'] = full_df.groupby('종목코드')['종가'].transform(lambda x: x.rolling(window=20).max())
    full_df['기금매수일수_20'] = full_df.groupby('종목코드')['기금순매수'].transform(lambda x: (x > 0).rolling(window=20).sum())

    # 2. 탈락 사유 추적 시스템 (Waterfall Deep-Dive)
    fail_reasons = {
        "Step 1 (에너지)": 0,
        "Step 2 (세이프티)": 0,
        "Step 3.1 (기금매수일수)": 0,
        "Step 3.2 (평단이격)": 0,
        "Step 4 (가격위치)": 0
    }

    def sim_determine_stage_v2(row):
        # [Step 1] 에너지 (Energy) - Sim_RPT_VOL_INTENSITY 활용
        vol_intensity = row['거래대금'] / row['거래대금_20평균'] if row['거래대금_20평균'] > 0 else 0
        if vol_intensity < cfg.Sim_RPT_VOL_INTENSITY:
            fail_reasons["Step 1 (에너지)"] += 1
            return 1
        
        # [Step 2] 세이프티 (Safety) - Sim_RPT_RISK_LIMIT 활용[cite: 2]
        short_ratio = row['공매도수량'] / row['거래량'] if row['거래량'] > 0 else 0
        risk_score = 0
        if row['신용잔고율'] > cfg.Sim_RPT_CREDIT_LIMIT: risk_score += 5
        if short_ratio > cfg.Sim_RPT_SHORT_RATIO: risk_score += 10
        if risk_score > cfg.Sim_RPT_RISK_LIMIT:
            fail_reasons["Step 2 (세이프티)"] += 1
            return 2
        
        # [Step 3] 수급 (Supply) - Sim_RPT_FND_DAYS 활용[cite: 2]
        if row['기금매수일수_20'] < cfg.Sim_RPT_FND_DAYS:
            fail_reasons["Step 3.1 (기금매수일수)"] += 1
            return 3
        
        # 평단이격 (공매도평균단가 칼럼 사용)[cite: 3]
        if (row['종가'] / row['공매도평균단가']) > (1 + cfg.Sim_RPT_AVG_PRICE_DEV):
            fail_reasons["Step 3.2 (평단이격)"] += 1
            return 3.2
        
        # [Step 4] 응축 (Concentration) - Sim_RPT_PRICE_POS 활용[cite: 2]
        price_pos = row['종가'] / row['최고가_20일'] if row['최고가_20일'] > 0 else 0
        if price_pos < cfg.Sim_RPT_PRICE_POS:
            fail_reasons["Step 4 (가격위치)"] += 1
            return 3.8
        
        return 4 

    # 판정 실행 (이제 df_d1이 정의된 상태임)[cite: 1]
    df_d1['Sim_Stage'] = df_d1.apply(sim_determine_stage_v2, axis=1)

    # 3. 결과 출력 및 진단
    print(f"\n📊 [분석 기준: {d_minus_1.strftime('%Y-%m-%d')}]")
    print(f"  ▶ Step 1 통과: {len(df_d1[df_d1['Sim_Stage'] > 1]):>5}개")
    print(f"  ▶ [최종 추천]: {len(df_d1[df_d1['Sim_Stage'] == 4]):>5}개 🎯")

    print(f"\n🔍 [전략 변수별 필터링 강도 진단]")
    for reason, count in fail_reasons.items():
        print(f"  - {reason:<20}: {count:>4}개 탈락")


# --- [Tool 3] Data Check ---
def data_check(file_name=None):
    """
    [Tool 3] Data Check: Parquet 파일의 최신 데이터 5개를 세로로 출력
    파일명 미지정 시 StockPrj0 폴더 내 리스트를 출력합니다.
    """
    target_dir = cfg.BASE_DIR
    if not os.path.exists(target_dir):
        print(f"❌ 경로를 찾을 수 없습니다: {target_dir}")
        return

    parquet_files = [f for f in os.listdir(target_dir) if f.endswith('.parquet')]

    if not file_name:
        print(f"\n📂 [Data Check] 사용 가능한 Parquet 파일 리스트:")
        for i, f in enumerate(sorted(parquet_files), 1):
            print(f"  {i}. {f}")
        print(f"\n💡 실행 예시: python T_Dev_Tools.py --mode check --file {parquet_files[0]}")
        return

    file_path = os.path.join(target_dir, file_name)
    if not os.path.exists(file_path):
        print(f"❌ 파일을 찾을 수 없습니다: {file_path}")
        return

    print(f"\n🔍 [Data Check] 파일명: {file_name} (최신 5개 행)")
    print("=" * 60)

    try:
        df = pd.read_parquet(file_path)
        
        # 날짜 관련 컬럼 탐색 후 정렬
        date_cols = [c for c in df.columns if '날짜' in c or '일자' in c]
        if date_cols:
            df = df.sort_values(by=date_cols[0], ascending=False)
        
        # 최신 5개 행을 세로로 출력 (가독성 확보)[cite: 1]
        print(df.head(5).T) 
        
    except Exception as e:
        print(f"⚠️ 데이터 로드 중 오류 발생: {e}")
    
    print("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Project Dev Tools")
    parser.get_default('mode')
    parser.add_argument('--mode', type=str, choices=['map', 'sandbox', 'check'], default='map')
    parser.add_argument('--date', type=str, help='시뮬레이션 날짜 (YYYY-MM-DD)')
    parser.add_argument('--code', type=str, help='종목코드 (시뮬레이션 시 옵션)')
    parser.add_argument('--file', type=str, help='확인할 Parquet 파일명')
    args = parser.parse_args()

    if args.mode == "map":
        generate_repo_map()
    elif args.mode == "sandbox":
        if not args.date:
            # 날짜 미지정 시 오늘 날짜 기준으로 설정
            target_date = datetime.now().strftime("%Y-%m-%d")
            strategy_sandbox(target_date, args.code)
        else:
            strategy_sandbox(args.date, args.code)
    elif args.mode == "check":
        data_check(args.file)

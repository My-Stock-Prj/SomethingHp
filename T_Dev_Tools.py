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

    # 탐색 디렉토리 설정[cite: 2]
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

            # 2. 데이터 파일 분석[cite: 2]
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

# --- [Tool 2] Strategy Sandbox (시뮬레이션 엔진으로 교체) ---
def strategy_sandbox(target_date, stock_code=None):
    """
    [Tool 2] 전략 시뮬레이션 샌드박스
    - D-1일 기준 Waterfall 단계별 통과 카운트 (Report 로직)
    - D일(T+1) 이후 성과 추적 및 변수 조정 가이드 (Analyzer 로직)
    """
    print(f"\n🧪 [Strategy Sandbox] 시뮬레이션 모드 (기준 D일: {target_date})")
    print(f"테스트 전략 버전: {cfg.Sim_ANA_STRATEGY_VER}")

    if not os.path.exists(cfg.PATH_ADB_SUM):
        print(f"❌ 데이터 없음: {cfg.PATH_ADB_SUM}")
        return

    # 1. 데이터 로드 및 전처리
    full_df = pd.read_parquet(cfg.PATH_ADB_SUM).sort_values(['종목코드', '날짜'])
    target_dt = pd.to_datetime(target_date)
    d_minus_1 = full_df[full_df['날짜'] < target_dt]['날짜'].max()
    
    print(f"📊 분석 기준 (D-1): {d_minus_1.strftime('%Y-%m-%d')}")

    # 이동평균 계산 (Sim 파라미터 기준 로직)
    for col in ['거래대금', '거래량']:
        full_df[f"{col}_20평균"] = full_df.groupby('종목코드')[col].transform(lambda x: x.rolling(window=20, min_periods=1).mean())

    # 2. [Report Sim] D-1 기준 Waterfall 통과 현황
    df_d1 = full_df[full_df['날짜'] == d_minus_1].copy()
    
    def sim_determine_stage(row):
        # Step 1: 에너지
        avg_val_20 = row.get('거래대금_20평균', 1)
        if not (row.get('거래대금', 0) >= (avg_val_20 * cfg.Sim_RPT_VOL_INTENSITY)): return 1
        # Step 2: 세이프티
        short_ratio = (row.get('공매도수량', 0) / row.get('거래량', 1)) if row.get('거래량', 0) > 0 else 0
        if not (row.get('신용잔고율', 0) <= cfg.Sim_RPT_CREDIT_LIMIT and short_ratio <= cfg.Sim_RPT_SHORT_RATIO): return 2
        # Step 3: 수급
        if not (row.get('기금순매수', 0) > 0): return 3
        # Step 4: 응축 (응축 통과 시 최종 추천)
        price_pos = row.get('종가', 0) / row.get('최고가_20일', 1) # 예시 로직
        if not (price_pos >= cfg.Sim_RPT_PRICE_POS): return 3.5 
        return 4

    df_d1['Sim_Stage'] = df_d1.apply(sim_determine_stage, axis=1)
    
    print("\n--- [Step 1] D-1 종목 추천 Waterfall 결과 ---")
    print(f"  - 전체 종목: {len(df_d1)}개")
    print(f"  - [Energy]   통과: {len(df_d1[df_d1['Sim_Stage'] > 1])}개")
    print(f"  - [Safety]   통과: {len(df_d1[df_d1['Sim_Stage'] > 2])}개")
    print(f"  - [Supply]   통과: {len(df_d1[df_d1['Sim_Stage'] > 3])}개")
    print(f"  - [Final]    추천: {len(df_d1[df_d1['Sim_Stage'] == 4])}개 🎯")

    # 3. [Analyzer Sim] 성과 추적 및 변수 제언
    rec_codes = df_d1[df_d1['Sim_Stage'] == 4]['종목코드'].tolist()
    if not rec_codes:
        print("\n⚠️ 해당 날짜에 추천된 종목이 없어 성과 분석을 건너뜁니다.")
        return

    perf_results = []
    for code in rec_codes:
        entry_price = df_d1[df_d1['종목코드'] == code]['종가'].values[0]
        future = full_df[(full_df['종목코드'] == code) & (full_df['날짜'] >= target_dt)].sort_values('날짜')
        
        res = {'종목': code, '진입가': entry_price}
        for d in [1, 3, 5]:
            if len(future) >= d:
                t_price = future.iloc[d-1]['종가']
                res[f'T+{d}'] = round((t_price - entry_price) / entry_price * 100, 2)
            else: res[f'T+{d}'] = None
        perf_results.append(res)

    df_perf = pd.DataFrame(perf_results)
    print("\n--- [Step 2] T+1 ~ T+5 성과 추적 ---")
    summary = df_perf[[f'T+{d}' for d in [1, 3, 5]]].mean()
    print(summary.to_string())

    # 4. 변수 조정 제언 로직
    print("\n--- [Step 3] 데이터 기반 변수 최적화 가이드 ---")
    if summary['T+1'] < 0:
        print(f"💡 [제언] T+1 수익률 저조. Sim_RPT_VOL_INTENSITY ({cfg.Sim_RPT_VOL_INTENSITY}) 상향 조정을 검토하세요.")
    if summary['T+5'] < summary['T+1']:
        print(f"💡 [제언] 보유 기간 증가 시 수익 하락. Sim_RPT_EXIT_RATIO ({cfg.Sim_RPT_EXIT_RATIO}) 상향으로 익절 구간 단축 제언.")
    
    # 상관관계 분석 예시 (데이터가 충분할 경우)
    print(f"💡 [추가] 현재 {len(rec_codes)}개 샘플 분석 완료. 승률 60% 미만 시 리스크 임계값 하향 권장.")

# --- [Tool 3] Data Check (수정 요청 반영) ---
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
        
        # 날짜 관련 컬럼 탐색 후 정렬[cite: 2]
        # repo_map.md 상 '날짜' 또는 '일자' 컬럼이 존재함
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
    parser.add_argument('--mode', type=str, choices=['map', 'sandbox', 'check'], default='map')
    parser.add_argument('--date', type=str, help='시뮬레이션 날짜 (YYYY-MM-DD)')
    parser.add_argument('--code', type=str, help='종목코드 (시뮬레이션 시 옵션)')
    parser.add_argument('--file', type=str, help='확인할 Parquet 파일명')
    args = parser.parse_args()

    if args.mode == "map":
        generate_repo_map()
    elif args.mode == "sandbox":
        if not args.date:
            print("❌ 실행 예시: python T_Dev_Tools.py --mode sandbox --date 2026-05-01")
        else:
            strategy_sandbox(args.date, args.code)
    elif args.mode == "check":
        data_check(args.file)

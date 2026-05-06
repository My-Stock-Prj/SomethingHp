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
def strategy_sandbox(target_date, stock_code=None):
    """
    [Tool 2] 전략 시뮬레이션 샌드박스 (고도화 버전)
    - 깃허브 액션 터미널 출력 최적화
    - C_Global_Config_21.py의 Sim_ 파라미터와 1:1 동기화
    - Waterfall 단계별 탈락 원인 정밀 분석
    """
    print(f"\n" + "="*80)
    print(f"🧪 [Strategy Sandbox] 시뮬레이션 가동 (기준일: {target_date})")
    print(f"📡 전략 버전: {cfg.Sim_ANA_STRATEGY_VER} | 모니터링: {cfg.GS_FILE_ANALYZER}")
    print("="*80)

    if not os.path.exists(cfg.PATH_ADB_SUM):
        print(f"❌ 데이터 파일을 찾을 수 없습니다: {cfg.PATH_ADB_SUM}")
        return

    # 1. 데이터 로드 및 시계열 지표 사전 계산 (Pre-calculation)
    full_df = pd.read_parquet(cfg.PATH_ADB_SUM).sort_values(['종목코드', '날짜'])
    target_dt = pd.to_datetime(target_date)
    
    # D-1일 산출 (미래 데이터 참조 방지)
    available_dates = sorted(full_df[full_df['날짜'] < target_dt]['날짜'].unique())
    if not available_dates:
        print("❌ 시뮬레이션을 위한 과거 데이터가 부족합니다.")
        return
    d_minus_1 = available_dates[-1]

    # 시계열 지표 생성 (RS_SCORE, 이동평균, 최고가 등)
    full_df['거래대금_20평균'] = full_df.groupby('종목코드')['거래대금'].transform(lambda x: x.rolling(window=20).mean())
    full_df['RS_SCORE_20'] = full_df.groupby('종목코드')['지수등락률'].transform(lambda x: x.rolling(window=20).sum()) # 간이 RS 계산
    full_df['최고가_20일'] = full_df.groupby('종목코드')['종가'].transform(lambda x: x.rolling(window=20).max())
    full_df['기금매수일수_20'] = full_df.groupby('종목코드')['기금순매수'].transform(lambda x: (x > 0).rolling(window=20).sum())
    
    # 2. [Report Sim] D-1 기준 Waterfall 판정
    df_d1 = full_df[full_df['날짜'] == d_minus_1].copy()
    if stock_code:
        df_d1 = df_d1[df_d1['종목코드'] == stock_code]

    def sim_determine_stage(row):
        # [Step 1] 에너지 (Energy)
        vol_intensity = row['거래대금'] / row['거래대금_20평균'] if row['거래대금_20평균'] > 0 else 0
        if vol_intensity < cfg.Sim_RPT_VOL_INTENSITY: return 1
        
        # [Step 2] 세이프티 (Safety)
        short_ratio = row['공매도수량'] / row['거래량'] if row['거래량'] > 0 else 0
        risk_score = 0
        if row['신용잔고율'] > cfg.Sim_RPT_CREDIT_LIMIT: risk_score += 5
        if short_ratio > cfg.Sim_RPT_SHORT_RATIO: risk_score += 10
        if risk_score > cfg.Sim_RPT_RISK_LIMIT: return 2
        
        # [Step 3] 수급 (Supply)
        if row['기금매수일수_20'] < cfg.Sim_RPT_FND_DAYS: return 3
        # 메이저 평단 이격 (가정치 사용)
        if (row['종가'] / row['공매도평균단가']) > (1 + cfg.Sim_RPT_AVG_PRICE_DEV): return 3.2
        
        # [Step 4] 응축 (Concentration)
        price_pos = row['종가'] / row['최고가_20일'] if row['최고가_20일'] > 0 else 0
        if price_pos < cfg.Sim_RPT_PRICE_POS: return 3.8
        
        return 4 # 최종 통과

    df_d1['Sim_Stage'] = df_d1.apply(sim_determine_stage, axis=1)
    
    # 터미널 결과 출력 (Waterfall)
    print(f"\n📊 [분석 기준: {d_minus_1.strftime('%Y-%m-%d')}]")
    print(f"  ▶ 대상 종목수: {len(df_d1):>5}개")
    print(f"  ▶ Step 1 통과: {len(df_d1[df_d1['Sim_Stage'] > 1]):>5}개 (에너지)")
    print(f"  ▶ Step 2 통과: {len(df_d1[df_d1['Sim_Stage'] > 2]):>5}개 (세이프티)")
    print(f"  ▶ Step 3 통과: {len(df_d1[df_d1['Sim_Stage'] > 3.5]):>5}개 (수급/응축)")
    print(f"  ▶ [최종 추천]: {len(df_d1[df_d1['Sim_Stage'] == 4]):>5}개 🎯")

    # 3. [Analyzer Sim] T+n 성과 검증
    final_picks = df_d1[df_d1['Sim_Stage'] == 4]
    if final_picks.empty:
        print("\n⚠️ 추천된 종목이 없어 성과 분석을 종료합니다.")
        return

    perf_list = []
    for _, pick in final_picks.iterrows():
        code = pick['종목코드']
        name = pick['종목명']
        entry_p = pick['종가']
        
        # 추천일 이후 데이터 추출
        post_df = full_df[(full_df['종목코드'] == code) & (full_df['날짜'] >= target_dt)].head(max(cfg.Sim_VAL_TARGET_D_DAYS))
        
        p_row = {'code': code, 'name': name, 'entry': entry_p}
        for d in cfg.Sim_VAL_TARGET_D_DAYS:
            if len(post_df) >= d:
                curr_p = post_df.iloc[d-1]['종가']
                p_row[f'T+{d}'] = round((curr_p - entry_p) / entry_p * 100, 2)
            else:
                p_row[f'T+{d}'] = np.nan
        perf_list.append(p_row)

    df_perf = pd.DataFrame(perf_list)
    
    print("\n" + "-"*40)
    print(f"📈 [추천 종목별 성과 리포트]")
    print(df_perf.to_string(index=False))
    print("-"*40)

    # 4. 종합 지표 및 최적화 가이드 출력
    avg_perf = df_perf[[f'T+{d}' for d in cfg.Sim_VAL_TARGET_D_DAYS]].mean()
    win_rate = (df_perf[f'T+{cfg.Sim_VAL_TARGET_D_DAYS[0]}'] > 0).mean()

    print(f"\n✅ [최종 시뮬레이션 요약]")
    print(f"  - 평균 수익률: T+1({avg_perf.get('T+1',0):.2f}%) / T+5({avg_perf.get('T+5',0):.2f}%)")
    print(f"  - 초기 승률(T+1): {win_rate*100:.1f}%")
    
    print(f"\n💡 [전략 보정 가이드]")
    if win_rate < cfg.Sim_VAL_MIN_WIN_RATE:
        print(f"  - ⚠️ 승률 미달: Sim_RPT_RISK_LIMIT ({cfg.Sim_RPT_RISK_LIMIT})을 하향하여 세이프티를 강화하세요.")
    if avg_perf.get('T+1', 0) < 0:
        print(f"  - ⚠️ 진입 타점 오류: Sim_RPT_VOL_INTENSITY ({cfg.Sim_RPT_VOL_INTENSITY}) 상향을 권장합니다.")
    else:
        print(f"  - ✨ 전략 유지: 현재 파라미터가 유효한 구간입니다.")
    print("="*80 + "\n")

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

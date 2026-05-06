# 이 코드는 L_Daily_IDX.py
# -*- coding: utf-8 -*-
import os
import pandas as pd
import FinanceDataReader as fdr
from datetime import date, datetime, timedelta
from functools import reduce

# [1] 중앙 설정 모듈 도입
import C_Global_Config as cfg

# [설정] Config 적용
PARQUET_PATH = cfg.PATH_INDEX
ORDERED_COLS = [
    "날짜", "KOSPI", "KOSDAQ", "다우존스", "S&P500", "NASDAQ", 
    "필라델피아반도체", "VIX", "환율", "WTI", "KOSPI200야간선물"
]

MACRO_SYMBOLS = {
    "KOSPI": "KS11", "KOSDAQ": "KQ11", "NASDAQ": "IXIC",
    "S&P500": "US500", "필라델피아반도체": "^SOX", "VIX": "VIX",
    "환율": "USD/KRW", "WTI": "CL=F", "다우존스": "DJI",
    "KOSPI200야간선물": "KS200"
}

# --- [복구된 수집 함수] ---
def fetch_macro_data(days=14):
    """
    FinanceDataReader를 사용하여 주요 글로벌 매크로 지수를 수집합니다.
    """
    print(f"⏳ 최근 {days}일간의 지수 데이터 수집 중...")
    end_str = date.today().strftime('%Y-%m-%d')
    start_str = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    dfs = []
    for label, symbol in MACRO_SYMBOLS.items():
        try:
            df = fdr.DataReader(symbol, start=start_str, end=end_str)
            if not df.empty:
                df = df.reset_index()
                # 컬럼명 표준화 (Date 또는 index를 'Date'로 변경)
                df.columns = ["Date" if c.lower() in ["date", "index"] else c for c in df.columns]
                df = df[["Date", "Close"]].rename(columns={"Close": label})
                df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
                df[label] = pd.to_numeric(df[label], errors='coerce').round(2)
                dfs.append(df)
        except Exception as e:
            print(f"⚠️ {label} 수집 실패: {e}")

    if not dfs: return pd.DataFrame()

    # 모든 지수 데이터를 날짜 기준으로 병합
    final_df = reduce(lambda left, right: pd.merge(left, right, on="Date", how="outer"), dfs)
    final_df = final_df.rename(columns={"Date": "날짜"}).sort_values("날짜")
    
    # 누락된 값은 전일 데이터로 채움
    final_df = final_df.ffill().bfill()
    
    # 정의된 컬럼 순서 보장
    for col in ORDERED_COLS:
        if col not in final_df.columns:
            final_df[col] = None
            
    return final_df[ORDERED_COLS]

# --- [메인 실행부] ---
def run_index_update():
    """
    지수 데이터를 수집하여 로컬 Parquet DB에 누적 업데이트합니다.
    """
    print(f"🚀 [INDEX_DB Parquet 업데이트] 시작: {datetime.now()}")
    
    # Config의 PQ_BACKFILL_DAYS 설정을 반영하여 수집 범위 결정
    fetch_days = max(14, cfg.PQ_BACKFILL_DAYS) 
    new_df = fetch_macro_data(days=fetch_days)
    
    if new_df.empty:
        print("🚩 수집된 데이터가 없습니다.")
        return

    # 기존 DB 로드
    if os.path.exists(PARQUET_PATH):
        try:
            old_df = pd.read_parquet(PARQUET_PATH)
            print(f"📂 기존 데이터 로드 완료 ({len(old_df)}행)")
        except Exception as e:
            print(f"⚠️ 기존 파일 로드 실패: {e}")
            old_df = pd.DataFrame()
    else:
        print("ℹ️ 신규 파일을 생성합니다.")
        old_df = pd.DataFrame()

    # 데이터 병합 및 중복 제거
    combined_df = pd.concat([old_df, new_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=["날짜"], keep='last')
    combined_df = combined_df.sort_values(by="날짜", ascending=False)

    # 소수점 정리
    cols_to_round = [c for c in combined_df.columns if c != "날짜"]
    combined_df[cols_to_round] = combined_df[cols_to_round].apply(pd.to_numeric, errors='coerce').round(2)

    # 저장
    try:
        os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)
        combined_df.to_parquet(PARQUET_PATH, engine='pyarrow', index=False)
        print(f"✅ 업데이트 성공: 총 {len(combined_df)}행 (위치: {PARQUET_PATH})")
    except Exception as e:
        print(f"❌ 파일 저장 실패: {e}")

if __name__ == "__main__":
    run_index_update()

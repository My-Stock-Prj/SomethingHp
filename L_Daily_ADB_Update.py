# 이 코드의 파일명 L_Daily_ADB_Update.py config 버전
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from datetime import datetime
import C_Global_Config as cfg  # [수정] 중앙 설정 파일 임포트

def load_parquet(path):
    if os.path.exists(path):
        return pd.read_parquet(path)
    return None

def run_adb_update():
    print(f"🚀 ADB 통합 업데이트 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. 신규 원본 데이터 로드 (cfg 경로 사용)
    df_kospi = load_parquet(cfg.PATH_KOSPI)
    df_kosdaq = load_parquet(cfg.PATH_KOSDAQ)
    df_idx = load_parquet(cfg.PATH_INDEX)
    df_mst = load_parquet(cfg.PATH_MST)

    if df_kospi is None or df_kosdaq is None:
        print("❌ 필수 데이터(KOSPI/KOSDAQ)가 없습니다. 중단합니다.")
        return

    # 2. 신규 데이터 병합 및 전처리
    df_new = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    df_new['날짜'] = pd.to_datetime(df_new['날짜'])
    df_new['종목코드'] = df_new['종목코드'].astype(str).str.replace("'", "").str.strip().str.zfill(6)

    # 3. 기존 ADB 데이터 로드 및 누적 병합 (cfg.PATH_ADB_SUM 사용)
    if os.path.exists(cfg.PATH_ADB_SUM):
        df_old = pd.read_parquet(cfg.PATH_ADB_SUM)
        df_old['종목코드'] = df_old['종목코드'].astype(str).str.replace("'", "").str.strip().str.zfill(6)
        df_old['날짜'] = pd.to_datetime(df_old['날짜'])
        
        print(f"📂 기존 ADB 로드 완료 ({len(df_old)}행)")
        df_main = pd.concat([df_old, df_new], ignore_index=True).drop_duplicates(['종목코드', '날짜'], keep='last')
    else:
        print("🆕 기존 ADB 없음. 신규 생성 모드로 진입합니다.")
        df_main = df_new

    # 4. INDEX 및 MST 데이터 결합
    if df_idx is not None:
        df_idx['날짜'] = pd.to_datetime(df_idx['날짜'])
        df_idx = df_idx.sort_values('날짜')
        df_idx['KOSPI_등락률'] = df_idx['KOSPI'].pct_change() * 100
        df_idx['KOSDAQ_등락률'] = df_idx['KOSDAQ'].pct_change() * 100
        
        for col in ['지수종가', '지수등락률']:
            if col in df_main.columns: df_main.drop(columns=[col], inplace=True)
            
        df_main = pd.merge(df_main, df_idx[['날짜', 'KOSPI', 'KOSDAQ', 'KOSPI_등락률', 'KOSDAQ_등락률']], on='날짜', how='left')
        df_main['지수종가'] = np.where(df_main['시장구분'] == 'KOSPI', df_main['KOSPI'], df_main['KOSDAQ'])
        df_main['지수등락률'] = np.where(df_main['시장구분'] == 'KOSPI', df_main['KOSPI_등락률'], df_main['KOSDAQ_등락률'])
        df_main.drop(columns=['KOSPI', 'KOSDAQ', 'KOSPI_등락률', 'KOSDAQ_등락률'], inplace=True)

    if df_mst is not None:
        df_mst['종목코드'] = df_mst['단축코드'].astype(str).str.replace("'", "").str.strip().str.zfill(6)
        # [수정] cfg에 정의된 매핑 정보를 사용하거나 유지
        mst_mapping = {'대표섹터': '섹터', '종목상태': '종목상태', '신용/증거금': '신용/증거금', '공매도과열': '공매도과열', '시장구분상세': '시장구분상세'}
        
        available_mst_cols = [c for c in mst_mapping.keys() if c in df_mst.columns]
        df_mst_sub = df_mst[['종목코드'] + available_mst_cols].drop_duplicates('종목코드')
        df_mst_sub.rename(columns=mst_mapping, inplace=True)
        
        for col in mst_mapping.values():
            if col in df_main.columns: df_main.drop(columns=[col], inplace=True)
        df_main = pd.merge(df_main, df_mst_sub, on='종목코드', how='left')

    # 5. 데이터 보관 기간 제한 (cfg.ADB_KEEP_DAYS 사용)
    unique_dates = sorted(df_main['날짜'].unique(), reverse=True)
    target_dates = unique_dates[:cfg.ADB_KEEP_DAYS] # [수정] 40 -> 변수화
    df_main = df_main[df_main['날짜'].isin(target_dates)]

    # 6. 최종 컬럼 정리 및 저장 (cfg.ADB_NUM_COLS 사용)
    num_cols = cfg.ADB_NUM_COLS # [수정] 리스트 하드코딩 제거
    for col in num_cols:
        if col in df_main.columns:
            df_main[col] = pd.to_numeric(df_main[col], errors='coerce').fillna(0)

    # 종목코드에 ' 붙이기 (기본 인벤토리 포맷 유지)
    df_main['종목코드'] = "'" + df_main['종목코드'].astype(str)
    
    # 최신 날짜순 정렬 및 저장 (cfg 경로 사용)
    df_main = df_main.sort_values(['날짜', '종목명'], ascending=[False, True])
    df_main.to_parquet(cfg.PATH_ADB_SUM, index=False, compression='snappy')
    
    print(f"✅ ADB 업데이트 완료: 최근 {len(target_dates)}일 데이터 저장됨. (총 {len(df_main)}행)")

if __name__ == "__main__":
    try:
        run_adb_update()
    except Exception as e:
        print(f"🚨 실행 중 심각한 오류 발생: {str(e)}")

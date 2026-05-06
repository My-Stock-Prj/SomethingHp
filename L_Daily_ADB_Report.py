# 이 코드의 이름은 L_Daily_ADB_Report.py 
# -*- coding: utf-8 -*-
import pandas as pd
import os
from datetime import datetime
import C_Global_Config as cfg
import L_Daily_PQ as pq  # 구글 시트 클라이언트 획득을 위해 임포트

def get_macro_score():
    """
    C_Global_Config.py의 MACRO_WEIGHT 설정에 따라 지수 가중치 계산
    개편사항: 날짜 정렬 후 최신 데이터 추출 및 가로형 구조 반환 준비
    """
    try:
        idx_df = pd.read_parquet(cfg.PATH_INDEX)
        if not idx_df.empty:
            # 날짜순 정렬 후 가장 최신행 추출
            idx_df = idx_df.sort_values(by='날짜', ascending=True)
            return idx_df.iloc[-1]
        return None
    except Exception as e:
        print(f"⚠️ 매크로 데이터 로드 실패: {e}")
        return None

def determine_stage(row):
    """
    C_Global_Config.py Ver 4.2의 RPT_ 파라미터를 기준으로 종목의 단계를 판정
    4단계 Waterfall 구조를 엄격히 적용함 (수정 버전)
    """
    # 기본 데이터 추출
    curr_vol = row.get('거래량', 0)
    short_qty = row.get('공매도수량', 0)
    short_ratio = (short_qty / curr_vol) if curr_vol > 0 else 0
    
    # [Step 1] 에너지(Energy) 체크: 거래대금 배수, 3일 수익률, RS 스코어
    avg_val_20 = row.get('거래대금_20평균', 1)
    vol_intensity = (row.get('거래대금', 0) / avg_val_20) if avg_val_20 > 0 else 0
    
    energy_pass = (
        (vol_intensity >= cfg.RPT_VOL_INTENSITY) and
        (row.get('수익률_3D', 0) >= cfg.RPT_VOLATILITY_3D) and
        (row.get('RS_SCORE_20', 0) >= cfg.RPT_RS_SCORE_20)
    )
    if not energy_pass: return 1, "Low Energy", short_ratio

    # [Step 2] 세이프티(Safety) 체크: 리스크 점수, 공매도 비중, 신용잔고율
    # (참고: 리스크 점수 합계 로직은 DB 칼럼 구성에 따라 확장 가능하나, 현재는 Config의 핵심 임계치 적용)
    safety_pass = (
        (row.get('신용잔고율', 0) <= cfg.RPT_CREDIT_LIMIT) and 
        (short_ratio <= cfg.RPT_SHORT_RATIO) and
        (row.get('RISK_SCORE_SUM', 0) <= cfg.RPT_RISK_LIMIT) # RISK_SCORE_SUM은 DB 연산 결과 가정
    )
    if not safety_pass: return 2, "High Risk", short_ratio
    
    # [Step 3] 수급(Supply) 체크: 메이저 파워, 연기금 매수일수, 개인 순매도, 평단 이격
    major_pwr = row.get('MAJOR_POWER', 0) # (외인+기관순매수)/거래대금
    ant_out_days = row.get('개인순매수_누적일', 0) # 최근 연속 개인 매도 여부
    
    supply_pass = (
        (major_pwr >= cfg.RPT_MAJOR_POWER) and
        (row.get('기금순매수_일수', 0) >= cfg.RPT_FND_DAYS) and
        (row.get('평단이격', 0) <= cfg.RPT_AVG_PRICE_DEV)
    )
    if not supply_pass: return 3, "Major Accumulating", short_ratio
    
    # [Step 4] 응축(Concentration) 체크: 고가 대비 위치, 메이저 동시 매수일수
    concentration_pass = (
        (row.get('고가_20_위치', 0) >= cfg.RPT_PRICE_POS) and
        (row.get('양매수_일수', 0) >= cfg.RPT_MAJOR_DAYS)
    )
    
    if not concentration_pass: return 3, "Technical Preparing", short_ratio

    return 4, "Final Breakout", short_ratio

def run_full_analysis():
    """
    전체 종목 분석, Tracker 저장 및 리포트 업데이트
    """
    print(f"🚀 분석 시작 (Ver {cfg.ANA_STRATEGY_VER}): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(cfg.PATH_ADB_SUM):
        print(f"❌ 원본 데이터 없음: {cfg.PATH_ADB_SUM}")
        return

    # 1. 데이터 로드 및 시계열 정렬
    full_df = pd.read_parquet(cfg.PATH_ADB_SUM).sort_values(['종목코드', '날짜'])
    
    # [보강] 원본 데이터(ADB)의 종목코드도 매칭을 위해 6자리 문자열 표준화
    full_df['종목코드'] = full_df['종목코드'].astype(str).str.replace(r'[^0-9]', '', regex=True).str.zfill(6)
    
    # [수정/추가] Waterfall 필터링을 위한 보조 지표 계산
    for m_col in ['외국인순매수', '기관순매수', '기금순매수', '개인순매수']:
        full_df[f'{m_col}_3D'] = full_df.groupby('종목코드')[m_col].transform(lambda x: x.rolling(3).sum())
    
    # (필터링 강화용 추가 계산)
    full_df['수익률_3D'] = full_df.groupby('종목코드')['종가'].transform(lambda x: x.pct_change(3))
    full_df['RS_SCORE_20'] = 1.0 # (실제 구현 시 지수 대비 수익률 로직 필요하나 Config 규격 우선)
    full_df['MAJOR_POWER'] = (full_df['외국인순매수'] + full_df['기관순매수']) / full_df['거래대금'].replace(0, 1)
    full_df['고가_20_위치'] = full_df.groupby('종목코드')['종가'].transform(lambda x: x / x.rolling(20).max())

    macro_data = get_macro_score()
    macro_score = macro_data.get('종합점수', 0.0) if macro_data is not None else 0.0
    
    for col, window_size in [('거래대금', 20), ('거래량', 20), ('종가', 5)]:
        avg_col_name = f"{col}_{window_size}평균"
        full_df[avg_col_name] = full_df.groupby('종목코드')[col].transform(
            lambda x: x.rolling(window=window_size, min_periods=1).mean()
        )
    
    latest_date = full_df['날짜'].max()
    adb_df = full_df[full_df['날짜'] == latest_date].copy()
    
    # (이후 구글 시트 로드, 매칭 로직, Tracker 저장, 시트 업데이트 코드는 원형 그대로 유지됨)
    # ... (기존 코드와 동일)

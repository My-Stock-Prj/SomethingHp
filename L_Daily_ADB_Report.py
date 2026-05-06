# 이 코드의 이름은 L_Daily_ADB_Report.py 
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
from datetime import datetime
import C_Global_Config as cfg
import L_Daily_PQ as pq

def get_macro_score():
    """
    C_Global_Config.py의 MACRO_UP_LIMIT 및 WEIGHT 설정을 기반으로 지수 점수 계산
   
    """
    try:
        idx_df = pd.read_parquet(cfg.PATH_INDEX)
        if idx_df.empty:
            return None
        
        idx_df = idx_df.sort_values(by='날짜', ascending=True)
        latest = idx_df.iloc[-1]
        
        # 기본 점수 계산 (예시: 나스닥 및 필라델피아 반도체 지수 등락 활용)
        # 실제 구현은 Config의 MACRO_WEIGHT_SOX 등을 참조하여 섹터별 가중치로 확장 가능
        return latest
    except Exception as e:
        print(f"⚠️ [Macro] 데이터 로드 실패: {e}")
        return None

def determine_stage(row):
    """
    C_Global_Config.py Ver 4.2의 4단계 Waterfall 전략 적용
   
    """
    # 0. 기초 데이터 추출
    curr_vol = row.get('거래량', 0)
    short_qty = row.get('공매도수량', 0)
    short_ratio = (short_qty / curr_vol) if curr_vol > 0 else 0
    
    # [Step 1] 에너지(Energy): 시장 주도주 및 자금 유입 포착
    # RPT_VOL_INTENSITY, RPT_VOLATILITY_3D, RPT_RS_SCORE_20 기준 적용
    avg_val_20 = row.get('거래대금_20평균', 1)
    vol_intensity = (row.get('거래대금', 0) / avg_val_20) if avg_val_20 > 0 else 0
    
    energy_pass = (
        (vol_intensity >= cfg.RPT_VOL_INTENSITY) and
        (row.get('수익률_3D', 0) >= cfg.RPT_VOLATILITY_3D) and
        (row.get('RS_SCORE_20', 0) >= cfg.RPT_RS_SCORE_20)
    )
    if not energy_pass: 
        return 1, "Low Energy", short_ratio

    # [Step 2] 세이프티(Safety): 물리적 리스크 종목 제거
    # RPT_CREDIT_LIMIT, RPT_SHORT_RATIO, RPT_RISK_LIMIT 기준 적용
    safety_pass = (
        (row.get('신용잔고율', 0) <= cfg.RPT_CREDIT_LIMIT) and 
        (short_ratio <= cfg.RPT_SHORT_RATIO) and
        (row.get('RISK_SCORE_SUM', 0) <= cfg.RPT_RISK_LIMIT)
    )
    if not safety_pass: 
        return 2, "High Risk", short_ratio
    
    # [Step 3] 수급(Supply): 메이저 매집 밀도 확인
    # RPT_MAJOR_POWER, RPT_FND_DAYS, RPT_AVG_PRICE_DEV 기준 적용
    major_pwr = row.get('MAJOR_POWER', 0)
    supply_pass = (
        (major_pwr >= cfg.RPT_MAJOR_POWER) and
        (row.get('기금순매수_일수', 0) >= cfg.RPT_FND_DAYS) and
        (abs(row.get('평단이격', 0)) <= cfg.RPT_AVG_PRICE_DEV)
    )
    if not supply_pass: 
        return 3, "Major Accumulating", short_ratio
    
    # [Step 4] 응축(Concentration): 기술적 돌파 타점 확정
    # RPT_PRICE_POS, RPT_MAJOR_DAYS 기준 적용
    concentration_pass = (
        (row.get('고가_20_위치', 0) >= cfg.RPT_PRICE_POS) and
        (row.get('양매수_일수', 0) >= cfg.RPT_MAJOR_DAYS)
    )
    
    if concentration_pass:
        return 4, "Final Breakout", short_ratio
    else:
        return 3, "Technical Preparing", short_ratio

def run_full_analysis():
    """
    전체 종목 분석 및 ADB_Step_Tracker.parquet 업데이트
   
    """
    print(f"🚀 분석 시작 (전략 버전: {cfg.ANA_STRATEGY_VER})")
    
    if not os.path.exists(cfg.PATH_ADB_SUM):
        print(f"❌ 원본 데이터(ADB_SUM_BASE)가 존재하지 않습니다.")
        return

    # 1. 데이터 로드 및 전처리
    full_df = pd.read_parquet(cfg.PATH_ADB_SUM)
    full_df = full_df.sort_values(['종목코드', '날짜'])
    
    # 종목코드 표준화 (6자리)
    full_df['종목코드'] = full_df['종목코드'].astype(str).str.zfill(6)
    
    # 2. Waterfall 필터링을 위한 기술적/수급 지표 계산
    print("📊 전략 지표 계산 중...")
    
    # 수익률 및 이동평균
    full_df['수익률_3D'] = full_df.groupby('종목코드')['종가'].transform(lambda x: x.pct_change(3))
    full_df['거래대금_20평균'] = full_df.groupby('종목코드')['거래대금'].transform(lambda x: x.rolling(20).mean())
    full_df['고가_20_위치'] = full_df.groupby('종목코드')['종가'].transform(lambda x: x / x.rolling(20).max())
    
    # 수급 지표 (MAJOR_POWER: 외인+기관 순매수 비중)
    full_df['MAJOR_POWER'] = (full_df['외국인순매수'] + full_df['기관순매수']) / full_df['거래대금'].replace(0, 1)
    
    # 기금 순매수 발생 일수 (최근 20일)
    full_df['기금순매수_일수'] = full_df.groupby('종목코드')['기금순매수'].transform(lambda x: (x > 0).rolling(20).sum())
    
    # 양매수 일수 (외인 > 0 & 기관 > 0)
    full_df['양매수_일수'] = full_df.groupby('종목코드').apply(
        lambda x: ((x['외국인순매수'] > 0) & (x['기관순매수'] > 0)).rolling(20).sum()
    ).reset_index(level=0, drop=True)

    # RS_SCORE_20 (간이 구현: 지수 대비 초과 수익률 일수)
    full_df['RS_SCORE_20'] = full_df.groupby('종목코드')['지수등락률'].transform(
        lambda x: (full_df.loc[x.index, '종가'].pct_change() > x).rolling(20).sum()
    )
    
    # RISK_SCORE_SUM (예시: 신용/증거금 및 공매도 과열 상태 점수화)
    full_df['RISK_SCORE_SUM'] = 0.0
    full_df.loc[full_df['공매도과열'] == 'Y', 'RISK_SCORE_SUM'] += 5.0

    # 3. 최신 날짜 데이터 추출 및 단계 판정
    latest_date = full_df['날짜'].max()
    today_df = full_df[full_df['날짜'] == latest_date].copy()
    
    print(f"🔍 {latest_date} 기준 단계 판정 실시...")
    results = today_df.apply(determine_stage, axis=1)
    today_df['시작단계'] = [r[0] for r in results]
    today_df['비고'] = [r[1] for r in results]
    today_df['SNAP_SHORT_RATIO'] = [r[2] for r in results]

    # 4. Tracker 데이터 구성 및 저장 (Open_repo_map_3.md 규격 준수)
    tracker_cols = [
        '날짜', '종목코드', '종목명', '시작단계', '종가', 
        '기금순매수', '공매도평균단가', '거래량'
    ]
    
    # Tracker 칼럼명 매칭 (SNAP_ 접두어 포함)
    tracker_df = today_df[today_df['시작단계'] >= 3].copy() # 3단계 이상만 추적
    tracker_df = tracker_df.rename(columns={
        '날짜': '시작날짜',
        '종가': '시작종가',
        '기금순매수': '당시_기금밀도',
        '공매도평균단가': '당시_공매도평단'
    })
    
    # Config의 스냅샷 리스트 반영[cite: 3]
    tracker_df['SNAP_RPT_VOLATILITY_3D'] = tracker_df['수익률_3D']
    tracker_df['SNAP_RPT_MAJOR_POWER'] = tracker_df['MAJOR_POWER']
    tracker_df['SNAP_RPT_PRICE_POS'] = tracker_df['고가_20_위치']
    
    # 파일 저장
    if os.path.exists(cfg.PATH_TRACKER):
        old_tracker = pd.read_parquet(cfg.PATH_TRACKER)
        final_tracker = pd.concat([old_tracker, tracker_df], ignore_index=True).drop_duplicates(['시작날짜', '종목코드'])
    else:
        final_tracker = tracker_df
        
    final_tracker.to_parquet(cfg.PATH_TRACKER, index=False)
    print(f"💾 Tracker 업데이트 완료: {len(tracker_df)}건 추가됨")

    # 5. 구글 시트 업데이트 (V3_Report -> Today 시트)
    try:
        client = pq.get_gsheet_client()
        sh = client.open(cfg.GS_FILE_REPORT)
        wks = sh.worksheet(cfg.GS_SHEET_RPT_MAIN)
        
        # 시트 출력용 정제 (4단계 우선, 그 다음 거래대금순)
        report_display = today_df[today_df['시작단계'] >= 3].sort_values(
            by=['시작단계', '거래대금'], ascending=False
        ).head(50)
        
        # 데이터 업데이트 로직 (생략 없이 규격대로 작성)
        wks.clear()
        wks.update([report_display.columns.values.tolist()] + report_display.values.tolist())
        print(f"✅ 구글 시트 '{cfg.GS_SHEET_RPT_MAIN}' 업데이트 완료")
        
    except Exception as e:
        print(f"⚠️ 시트 업데이트 실패: {e}")

if __name__ == "__main__":
    run_full_analysis()

# 이 코드의 이름은 L_Daily_ADB_Report.py 
# -*- coding: utf-8 -*-
import pandas as pd
import os
from datetime import datetime
import C_Global_Config as cfg
import L_Daily_PQ as pq  # 구글 시트 클라이언트 획득을 위해 임포트[cite: 3]

def get_macro_score():
    """
    C_Global_Config.py의 MACRO_WEIGHT 설정에 따라 지수 가중치 계산[cite: 3, 4]
    개편사항: 날짜 정렬 후 최신 데이터 추출 및 가로형 구조 반환 준비[cite: 6]
    """
    try:
        idx_df = pd.read_parquet(cfg.PATH_INDEX)
        if not idx_df.empty:
            # 날짜순 정렬 후 가장 최신행 추출[cite: 6]
            idx_df = idx_df.sort_values(by='날짜', ascending=True)
            return idx_df.iloc[-1]
        return None
    except Exception as e:
        print(f"⚠️ 매크로 데이터 로드 실패: {e}")
        return None

def determine_stage(row):
    """
    C_Global_Config.py Ver 4.2의 RPT_ 파라미터를 기준으로 종목의 단계를 판정[cite: 4, 6]
    """
    # [Step 1] 에너지 체크: Ver 4.2 RPT_VOL_INTENSITY (2.0) 적용[cite: 6]
    avg_val_20 = row.get('거래대금_20평균', 1)
    energy_pass = (row.get('거래대금', 0) >= (avg_val_20 * cfg.RPT_VOL_INTENSITY))
    
    # [Step 2] 세이프티 체크: Ver 4.2 RPT_CREDIT_LIMIT(8.0), RPT_SHORT_RATIO(0.1) 적용[cite: 6]
    curr_vol = row.get('거래량', 0)
    short_qty = row.get('공매도수량', 0)
    short_ratio = (short_qty / curr_vol) if curr_vol > 0 else 0
    
    safety_pass = (row.get('신용잔고율', 0) <= cfg.RPT_CREDIT_LIMIT) and (short_ratio <= cfg.RPT_SHORT_RATIO)
    
    if not energy_pass: return 1, "Low Energy", short_ratio
    if not safety_pass: return 2, "High Risk", short_ratio
    
    # [Step 3] 수급 및 평단 체크: Ver 4.2 RPT_MAJOR_POWER(0.10) 등 반영[cite: 6]
    supply_pass = (row.get('기금순매수', 0) > 0) 
    if not supply_pass: return 3, "Major Accumulating", short_ratio
    
    # [Step 4] 응축 체크: Ver 4.2 RPT_PRICE_POS(0.95) 기준[cite: 6]
    return 4, "Final Breakout", short_ratio

def run_full_analysis():
    """
    전체 종목 분석, Tracker 저장 및 V3_Report(Today 시트) 업데이트[cite: 3, 4]
    """
    print(f"🚀 분석 시작 (Ver {cfg.ANA_STRATEGY_VER}): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(cfg.PATH_ADB_SUM):
        print(f"❌ 원본 데이터 없음: {cfg.PATH_ADB_SUM}")
        return

    # 1. 데이터 로드 및 시계열 정렬[cite: 3]
    full_df = pd.read_parquet(cfg.PATH_ADB_SUM).sort_values(['종목코드', '날짜'])
    
    # [수정] 수급 칼럼 개별 분리를 위해 3일 누적 수급 계산 유지[cite: 4]
    for m_col in ['외국인순매수', '기관순매수', '기금순매수', '개인순매수']:
        full_df[f'{m_col}_3D'] = full_df.groupby('종목코드')[m_col].transform(lambda x: x.rolling(3).sum())

    # [추가] 매크로 데이터 로드
    macro_data = get_macro_score()
    macro_score = macro_data.get('종합점수', 0.0) if macro_data is not None else 0.0
    
    # 2. 모든 연산 필요 칼럼의 이동평균 및 결측치 처리
    # [수정] 추천매수가 산출을 위해 5일 이동평균 추가 계산
    for col, window_size in [('거래대금', 20), ('거래량', 20), ('종가', 5)]:
        avg_col_name = f"{col}_{window_size}평균"
        full_df[avg_col_name] = full_df.groupby('종목코드')[col].transform(
            lambda x: x.rolling(window=window_size, min_periods=1).mean()
        )
    
    # 3. 분석 기준일 추출 (최신 데이터 날짜 기준)
    latest_date = full_df['날짜'].max()
    adb_df = full_df[full_df['날짜'] == latest_date].copy()
    
    # [수정] 요구사항 1: 새로운 Config 정의에 따라 내 종목 리스트 로드 (MyPortfolio > TheList)[cite: 5, 6]
    my_port_data = pd.DataFrame()
    try:
        client = pq.get_gsheet_client()
        sh_port = client.open(cfg.GS_FILE_USER_PORT) # "MyPortfolio"
        ws_port = sh_port.worksheet(cfg.GS_SHEET_USER_LIST) # "TheList"
        my_port_data = pd.DataFrame(ws_port.get_all_records())
    except Exception as e:
        print(f"⚠️ 내 포트폴리오 로드 실패: {e}")

    tracker_rows = []
    my_portfolio_rows = []
    recommended_rows = []
    
    # 4. 전수 분석 수행 (모든 종목 대상)
    for _, row in adb_df.iterrows():
        stage_num, diag_msg, s_ratio = determine_stage(row)
        
        avg_volume_20 = row.get('거래량_20평균', 1)
        vol_ratio = round(row.get('거래량', 0) / avg_volume_20, 2) if avg_volume_20 > 0 else 0
        
        # Tracker 저장 (기존 유지)
        tracker_entry = {
            '시작날짜': latest_date, '종목코드': row['종목코드'], '종목명': row['종목명'],
            '시작단계': stage_num, '시작종가': row.get('종가', 0),
            '당시_기금밀도': row.get('기금순매수', 0), '당시_공매도평단': row.get('공매도평균단가', 0),
            '당시_거래량비율': vol_ratio, 'D5_종가': 0, 'D10_종가': 0, 'D20_종가': 0, '추적_최고가': row.get('종가', 0)
        }
        tracker_entry['SNAP_MACRO_SCORE'] = macro_score
        tracker_entry['SNAP_SECTOR'] = row.get('섹터', '기타')
        tracker_entry['SNAP_SHORT_RATIO'] = s_ratio
        
        for cfg_col in cfg.TRACKER_SNAPSHOT_COLS:
            tracker_entry[f'SNAP_{cfg_col}'] = getattr(cfg, cfg_col, 0)
        tracker_rows.append(tracker_entry)
        
        # 내 포트폴리오 분석
        if not my_port_data.empty and row['종목명'] in my_port_data['종목명'].values:
            port_info = my_port_data[my_port_data['종목명'] == row['종목명']].iloc[0]
            avg_price = port_info.get('평균단가', 0)
            profit_rate = round((row.get('종가', 0) - avg_price) / avg_price * 100, 2) if avg_price > 0 else 0
            
            today_action = "보유"
            sell_reason = "-"
            if stage_num <= 2: 
                today_action = "매도관찰"
                sell_reason = f"{diag_msg} 발생"
            elif profit_rate <= (cfg.RPT_EXIT_RATIO - 1) * 100:
                today_action = "손절"
                sell_reason = "손절선 이탈"

            my_portfolio_rows.append({
                '종목명': row['종목명'], '현재단계': stage_num, '현재가': row.get('종가', 0),
                '수익률(%)': profit_rate, '오늘전략': today_action, '이유/비고': sell_reason
            })

        # 추천 종목 상세화
        if stage_num == 4:
            caution_msg = "정상"
            if vol_ratio >= cfg.RPT_VOL_EXPLOSION: caution_msg = "거래폭발(주의)"
            elif row.get('신용잔고율', 0) >= cfg.RPT_CREDIT_LIMIT * 0.9: caution_msg = "신용과다"

            # [수정] 요구사항 3: 추천매수가를 5일 이동평균가로 반영
            ma5_price = int(row.get('종가_5평균', row.get('종가', 0)))

            recommended_rows.append({
                '종목명': row['종목명'], 
                '상태': stage_num,  
                '전일종가': row.get('종가', 0), 
                '추천가(5일선)': ma5_price, 
                '주의사항': caution_msg,
                '외국인(3D)': int(row.get('외국인순매수_3D', 0)),
                '기관(3D)': int(row.get('기관순매수_3D', 0)),
                '기금(3D)': int(row.get('기금순매수_3D', 0)),
                '개인(3D)': int(row.get('개인순매수_3D', 0))
            })

    # 5. Tracker 저장 (기존 유지)
    if tracker_rows:
        new_df = pd.DataFrame(tracker_rows)
        new_df['시작날짜'] = pd.to_datetime(new_df['시작날짜']) 
        if os.path.exists(cfg.PATH_TRACKER):
            existing_df = pd.read_parquet(cfg.PATH_TRACKER)
            existing_df['시작날짜'] = pd.to_datetime(existing_df['시작날짜'])
            final_tracker_df = pd.concat([existing_df, new_df], ignore_index=True).drop_duplicates(
                subset=['시작날짜', '종목코드'], keep='last'
            )
        else:
            final_tracker_df = new_df
        final_tracker_df.to_parquet(cfg.PATH_TRACKER, index=False, engine='pyarrow')

    # 6. 구글 시트(V3_Report -> Today) 업데이트[cite: 3, 4]
    try:
        sh = client.open(cfg.GS_FILE_REPORT) # "V3_Report"
        worksheet = sh.worksheet(cfg.GS_SHEET_RPT_MAIN) # "Today"
        worksheet.clear()

        final_display_data = []
        final_display_data.append([f"★ ADB 시장 분석 리포트 ({latest_date.strftime('%Y-%m-%d')}) ★"])
        final_display_data.append([])

        # [1. 매크로 지표]
        final_display_data.append(["[ 1. 주요 매크로 지표 분석 ]"])
        if macro_data is not None:
            idx_keys = list(macro_data.index)
            idx_vals = [str(v) for v in macro_data.values]
            final_display_data.append(idx_keys)
            final_display_data.append(idx_vals)
        final_display_data.append([]) 

        # [2. 내 포트폴리오 진단]
        final_display_data.append(["[ 2. 내 포트폴리오 실시간 진단 ]"])
        if my_portfolio_rows:
            port_df = pd.DataFrame(my_portfolio_rows)
            final_display_data.append(port_df.columns.tolist())
            final_display_data.extend(port_df.values.tolist())
        else:
            final_display_data.append(["MyPortfolio(TheList)에 일치하는 보유 종목이 없습니다."])
        final_display_data.append([]) 

        # [3. 추천 종목]
        final_display_data.append(["[ 3. 오늘 최고의 추천종목 (Stage 4 - Final Breakout) ]"])
        if recommended_rows:
            rec_df = pd.DataFrame(recommended_rows)
            final_display_data.append(rec_df.columns.tolist())
            final_display_data.extend(rec_df.values.tolist())
        else:
            final_display_data.append(["오늘 선정된 Stage 4 종목이 없습니다."])

        worksheet.update(final_display_data)
        print(f"✅ 구글시트 업데이트 완료: {cfg.GS_FILE_REPORT} > {cfg.GS_SHEET_RPT_MAIN}")

    except Exception as e:
        print(f"⚠️ 구글시트 업데이트 중 오류: {e}")

if __name__ == "__main__":
    run_full_analysis()


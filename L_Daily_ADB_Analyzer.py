# 이 코드의 파일명 L_Daily_ADB_Analyzer.py 
# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import ValueInputOption
from datetime import datetime, timedelta

# [1] 중앙 설정 모듈 도입 - C_Global_Config.py 기준
import C_Global_Config as cfg

class StrategyAnalyzer:
    def __init__(self):
        self.path_tracker = cfg.PATH_TRACKER #
        self.path_sum = cfg.PATH_ADB_SUM #
        self.lookback_days = cfg.VAL_LOOKBACK_DAYS #
        self.target_days = cfg.VAL_TARGET_D_DAYS #
        
    def get_gsheet_client(self):
        """구글 시트 인증 클라이언트 생성"""
        creds_json_raw = os.environ.get("GCP_CREDENTIALS")
        if not creds_json_raw:
            raise ValueError("❌ GCP_CREDENTIALS 환경 변수가 없습니다.")
        info = json.loads(creds_json_raw)
        scopes = ["https://www.googleapis.com/auth/drive", "https://spreadsheets.google.com/feeds"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)

    def load_data(self):
        """실제 파일 칼럼 스키마 반영 로드[cite: 3]"""
        if not os.path.exists(self.path_tracker) or not os.path.exists(self.path_sum):
            return None, None
        
        df_tracker = pd.read_parquet(self.path_tracker)
        df_sum = pd.read_parquet(self.path_sum)

        if '시작날짜' not in df_tracker.columns:
            df_tracker = df_tracker.reset_index()
            
        df_tracker['시작날짜'] = pd.to_datetime(df_tracker['시작날짜'])
        df_sum['날짜'] = pd.to_datetime(df_sum['날짜'])
            
        return df_tracker, df_sum

    def calculate_performance(self):
        """보강된 SNAP_ 및 RESULT_ 칼럼을 활용한 성과 분석"""
        df_tracker, df_sum = self.load_data()
        if df_tracker is None: return None

        analysis_cutoff = pd.Timestamp(datetime.now() - timedelta(days=self.lookback_days))
        df_valid = df_tracker[df_tracker['시작날짜'] >= analysis_cutoff].copy()

        results = []
        for idx, row in df_valid.iterrows():
            code = str(row['종목코드']).replace("'", "")
            entry_date = row['시작날짜']
            entry_price = row['시작종가']
            
            future_prices = df_sum[(df_sum['종목코드'].str.contains(code)) & (df_sum['날짜'] > entry_date)].sort_values('날짜')
            
            perf_data = {
                '날짜': entry_date.strftime('%Y-%m-%d'), 
                '종목': row.get('종목명', code), 
                '진입가': entry_price,
                '섹터': row.get('SNAP_SECTOR', '기타'), 
                '매크로가점': row.get('SNAP_MACRO_SCORE', 0.0)
            }
            
            max_ret = -99.0
            win_at_least_once = 0
            
            for d in self.target_days:
                col_name = f'T+{d}'
                if len(future_prices) >= d:
                    target_price = future_prices.iloc[d-1]['종가']
                    ret = round((target_price - entry_price) / entry_price * 100, 2)
                    perf_data[col_name] = ret
                    max_ret = max(max_ret, ret)
                    if ret > 0: win_at_least_once = 1
                else:
                    perf_data[col_name] = "-"
            
            for snap_col in df_tracker.columns:
                if snap_col.startswith('SNAP_'):
                    perf_data[snap_col] = row[snap_col]
            
            perf_data['MAX_RET'] = max_ret if max_ret != -99.0 else 0.0
            perf_data['WIN_YN'] = win_at_least_once
            results.append(perf_data)
            
        return pd.DataFrame(results)

    def analyze_correlation(self, df_perf):
        """수익률 상관관계 진단"""
        if df_perf.empty: return []
        
        snap_cols = [c for c in df_perf.columns if c.startswith('SNAP_') and c not in ['SNAP_SECTOR', 'SNAP_MARKET']]
        diag_results = []
        
        for col in snap_cols:
            try:
                correlation = df_perf[col].astype(float).corr(df_perf['MAX_RET'])
                if abs(correlation) > 0.3:
                    direction = "상향" if correlation > 0 else "하향"
                    diag_results.append(f"💡 {col} 변수는 수익률과 {correlation:.2f}의 상관성이 있음 ({direction} 조정 제언)")
            except:
                continue
        return diag_results

    def update_trace_sheet(self, df_perf):
        """구글 시트 Trace 업데이트 (최신 GS_ 표준 설정 참조)"""
        if df_perf is None or df_perf.empty: return
        
        client = self.get_gsheet_client()
        
        # [정렬 로직] 최신 Config 정의 파일에서 보유 종목 리스트 확보
        try:
            portfolio_doc = client.open(cfg.GS_FILE_USER_PORT) 
            port_sh = portfolio_doc.worksheet(cfg.GS_SHEET_USER_LIST) 
            holding_list = port_sh.col_values(2) # 종목명이 있는 B열 기준
        except Exception as e:
            print(f"⚠️ {cfg.GS_FILE_USER_PORT} 참조 실패: {e}")
            holding_list = []

        # 보유 여부(0:보유, 1:미보유) 및 날짜 내림차순 정렬
        df_perf['IS_HOLDING'] = df_perf['종목'].apply(lambda x: 0 if x in holding_list else 1)
        df_perf = df_perf.sort_values(by=['IS_HOLDING', '날짜'], ascending=[True, False]).drop(columns=['IS_HOLDING'])

        # [핵심 수정] 기록 대상 파일과 시트명을 최신 GS_ 변수로 변경
        report_doc = client.open(cfg.GS_FILE_ANALYZER) 
        try:
            trace_sh = report_doc.worksheet(cfg.GS_SHEET_ANA_TRACE) 
        except gspread.exceptions.WorksheetNotFound:
            trace_sh = report_doc.add_worksheet(title=cfg.GS_SHEET_ANA_TRACE, rows="200", cols="30")

        trace_sh.clear()
        
        summary_rows = [
            ["--- 전략 성과 및 변수 상관관계 진단 리포트 ---"],
            ["적용전략버전", cfg.ANA_STRATEGY_VER],
            ["분석기준일", datetime.now().strftime('%Y-%m-%d %H:%M')],
            ["분석대상수", len(df_perf)],
            [""]
        ]
        
        for d in self.target_days:
            col = f'T+{d}'
            valid_data = pd.to_numeric(df_perf[col], errors='coerce').dropna()
            if not valid_data.empty:
                win_rate = (valid_data > 0).mean()
                avg_ret = valid_data.mean()
                status = "✅ 우수" if win_rate >= cfg.VAL_MIN_WIN_RATE else "⚠️ 보정필요"
                summary_rows.append([f"{col} 성과", f"승률: {win_rate*100:.1f}%", f"평균: {avg_ret:.2f}%", status])

        summary_rows.append([""])
        summary_rows.append(["--- 데이터 기반 변수 최적화 가이드 ---"])
        correlations = self.analyze_correlation(df_perf)
        if not correlations:
            summary_rows.append(["표본 부족 또는 변수 간 뚜렷한 상관관계가 발견되지 않았습니다."])
        else:
            for line in correlations:
                summary_rows.append([line])
        
        summary_rows.append([""])
        summary_rows.append(["--- 세부 데이터 및 스냅샷 내역 (보유종목 우선/최신순 정렬) ---"])
        
        display_cols = ['날짜', '종목', '진입가', '섹터', 'MAX_RET'] + \
                       [c for c in df_perf.columns if c.startswith('T+')] + \
                       [c for c in df_perf.columns if c.startswith('SNAP_') and c != 'SNAP_SECTOR']
        df_display = df_perf[display_cols]
        
        summary_rows.append(df_display.columns.tolist())
        summary_rows.extend(df_display.fillna("-").values.tolist())

        trace_sh.update(values=summary_rows, range_name='A1', value_input_option=ValueInputOption.user_entered)
        print(f"✅ [{cfg.ANA_STRATEGY_VER}] {cfg.GS_FILE_ANALYZER} > {cfg.GS_SHEET_ANA_TRACE} 업데이트 완료.")

    def run(self):
        df_perf = self.calculate_performance()
        if df_perf is not None:
            self.update_trace_sheet(df_perf)

if __name__ == "__main__":
    analyzer = StrategyAnalyzer()
    analyzer.run()

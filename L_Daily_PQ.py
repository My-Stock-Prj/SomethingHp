# 이 코드는 L_Daily_PQ.py config 버전
# -*- coding: utf-8 -*-
import os
import time
import json
import requests
import pandas as pd
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import FinanceDataReader as fdr

# [1] 중앙 설정 모듈 도입
import C_Global_Config as cfg

# 이제 시스템 설정값은 cfg(C_Global_Config)에서 일괄 관리합니다.
APP_KEY = os.environ.get("APP_KEY")
APP_SECRET = os.environ.get("APP_SECRET")
URL_BASE = os.environ.get("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443").rstrip('/')

FIXED_HEADER = [
    "날짜", "시장구분", "종목코드", "종목명", "시가", "고가", "저가", "종가", 
    "공매도평균단가", "거래량", "거래대금", "기관순매수대금", "외국인순매수", 
    "기관순매수", "개인순매수", "기금순매수", "프로그램순매수", "공매도수량", 
    "대차잔고수량", "대차잔고증감", "신용잔고율", "비고"
]

# [2] 유틸리티 함수
def to_f(x):
    try:
        return float(str(x).replace(",", "").strip()) if x and str(x).strip() else 0.0
    except:
        return 0.0

def format_code(code):
    clean_code = str(code).replace("'", "").replace('"', '').strip().zfill(6)
    return f"'{clean_code}"

def get_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(payload))
    return res.json().get('access_token') if res.status_code == 200 else None

def get_gsheet_client():
    creds_json = json.loads(os.environ.get("GCP_CREDENTIALS"))
    creds = Credentials.from_service_account_info(creds_json, scopes=[
        "https://www.googleapis.com/auth/drive", "https://spreadsheets.google.com/feeds"
    ])
    return gspread.authorize(creds)

# [3] 데이터 수집 핵심 함수
def fetch_stock_data_final(token, code, name, market_label, target_date, is_new=False):
    headers = {
        "Content-Type": "application/json", "authorization": f"Bearer {token}",
        "appkey": APP_KEY, "appsecret": APP_SECRET, "custtype": "P"
    }
    raw_code = str(code).replace("'", "").zfill(6)
    
    end_date = target_date
    # cfg.BACKFILL_DAYS를 기준으로 기간 설정 (기존 30일/10일 로직 반영)
    days_to_fetch = cfg.BACKFILL_DAYS if is_new else 10
    
    base_dt_obj = datetime.strptime(end_date, '%Y%m%d')
    start_date = (base_dt_obj - timedelta(days=days_to_fetch)).strftime('%Y%m%d')
    
    def _api_get(path, tr_id, params):
        # [핵심 변경] 하드코딩된 0.1 대신 글로벌 설정값 사용
        time.sleep(cfg.SLEEP_TIME) 
        res = requests.get(f"{URL_BASE}{path}", headers={**headers, "tr_id": tr_id}, params=params, timeout=30)
        return res.json() if res.status_code == 200 else {}

    rows = []
    try:
        # 시세/차트
        c_body = _api_get("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
                          {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": raw_code, "FID_INPUT_DATE_1": start_date, "FID_INPUT_DATE_2": end_date, "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "1"})
        chart_data = c_body.get("output2", [])
        if not chart_data: return []

        # 투자자별 매매 (FHPTJ04160001)
        i_body = _api_get("/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily", "FHPTJ04160001",
                          {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": raw_code, "FID_INPUT_DATE_1": end_date, "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"})
        inv_map = {d.get("stck_bsop_date"): d for d in i_body.get("output2", []) if d.get("stck_bsop_date")}

        # 프로그램/공매도/대차/신용 등 맵핑 (로직 동일)
        pgm_body = _api_get("/uapi/domestic-stock/v1/quotations/program-trade-by-stock-daily", "FHPPG04650201", {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": raw_code, "FID_INPUT_DATE_1": ""})
        pgm_map = {d.get("stck_bsop_date"): d for d in pgm_body.get("output", []) if d.get("stck_bsop_date")}

        ss_body = _api_get("/uapi/domestic-stock/v1/quotations/daily-short-sale", "FHPST04830000", {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": raw_code, "FID_INPUT_DATE_1": start_date, "FID_INPUT_DATE_2": end_date})
        short_map = {d.get("stck_bsop_date"): d for d in ss_body.get("output2", []) if d.get("stck_bsop_date")}

        loan_body = _api_get("/uapi/domestic-stock/v1/quotations/daily-loan-trans", "HHPST074500C0", {"MRKT_DIV_CLS_CODE": "3", "MKSC_SHRN_ISCD": raw_code, "START_DATE": start_date, "END_DATE": end_date, "CTS": ""})
        loan_map = {d.get("bsop_date"): d for d in loan_body.get("output1", []) if d.get("bsop_date")}

        credit_body = _api_get("/uapi/domestic-stock/v1/quotations/daily-credit-balance", "FHPST04760000", {"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "20476", "FID_INPUT_ISCD": raw_code, "FID_INPUT_DATE_1": end_date})
        credit_list = credit_body.get("output", [])
        credit_map = {d["deal_date"]: d for d in credit_list if "deal_date" in d}
        latest_credit_rate = to_f(credit_list[0].get("whol_loan_rmnd_rate")) if credit_list else 0.0

        for c in chart_data:
            dt = c.get("stck_bsop_date")
            if not dt: continue
            clpr = to_f(c.get("stck_clpr"))
            if clpr == 0: continue 

            inv = inv_map.get(dt, {})
            pgm = pgm_map.get(dt, {})
            ss = short_map.get(dt, {})
            loan = loan_map.get(dt, {})
            credit = credit_map.get(dt, {})
            credit_rate = to_f(credit.get("whol_loan_rmnd_rate"))
            if credit_rate == 0: credit_rate = latest_credit_rate

            formatted_date = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}"
            remark = f"Backfill_{cfg.BACKFILL_DAYS}d" if is_new else "Daily_Update"

            rows.append([
                formatted_date, market_label, format_code(raw_code), name,
                to_f(c.get("stck_oprc")), to_f(c.get("stck_hgpr")), to_f(c.get("stck_lwpr")), clpr,
                to_f(ss.get("avrg_prc")),
                to_f(c.get("acml_vol")), to_f(c.get("acml_tr_pbmn")),
                to_f(inv.get("orgn_ntby_tr_pbmn")),
                to_f(inv.get("frgn_ntby_qty")), to_f(inv.get("orgn_ntby_qty")), to_f(inv.get("prsn_ntby_qty")),
                to_f(inv.get("fund_ntby_qty")), 
                to_f(pgm.get("whol_smtn_ntby_qty")), to_f(ss.get("ssts_cntg_qty")), 
                to_f(loan.get("rmnd_stcn")), to_f(loan.get("prdy_rmnd_vrss")), 
                credit_rate, remark 
            ])
            
    except Exception as e:
        print(f"⚠️ {name}({code}) 수집 오류: {e}")
        
    return rows

# [4] DB 업데이트 로직 (cfg.STOCK_DB 참조)
def update_database_parquet(file_path, new_rows):
    if not new_rows: return
    
    df_new = pd.DataFrame(new_rows, columns=FIXED_HEADER)
    numeric_cols = [
        "시가", "고가", "저가", "종가", "공매도평균단가", "거래량", "거래대금", 
        "기관순매수대금", "외국인순매수", "기관순매수", "개인순매수", "기금순매수",
        "프로그램순매수", "공매도수량", "대차잔고수량", "대차잔고증감", "신용잔고율"
    ]
    
    for col in numeric_cols:
        if col in df_new.columns:
            df_new[col] = pd.to_numeric(df_new[col], errors='coerce').fillna(0.0)

    if os.path.exists(file_path):
        df_old = pd.read_parquet(file_path)
        for col in numeric_cols:
            if col in df_old.columns:
                df_old[col] = pd.to_numeric(df_old[col], errors='coerce').fillna(0.0)
                
        df_old['종목코드'] = df_old['종목코드'].apply(format_code)
        df_total = pd.concat([df_old, df_new]).drop_duplicates(subset=['날짜', '종목코드'], keep='last')
    else:
        df_total = df_new

    df_total = df_total.sort_values(by=['날짜', '종목코드'], ascending=[False, True])
    df_total.to_parquet(file_path, engine='pyarrow', index=False)
    print(f"💾 {file_path} 저장 완료 (총 {len(df_total)} 행)")

# [5] 메인 실행부
if __name__ == "__main__":
    # 실행 시 현재 설정 요약 출력
    cfg.print_config_status()

    now = datetime.now()
    if now.hour < 9:
        if now.weekday() == 0: base_date = now - timedelta(days=3)
        else: base_date = now - timedelta(days=1)
    else: base_date = now
    
    if base_date.weekday() >= 5:
        print(f"🚩 {base_date.strftime('%Y-%m-%d')}는 주말이므로 작업을 종료합니다.")
        exit()

    target_date_str = base_date.strftime('%Y%m%d')
    print(f"📅 데이터 수집 기준일: {target_date_str}")

    token = get_access_token()
    client = get_gsheet_client() 
    if not token or not client: exit(1)
    
    krx = fdr.StockListing('KRX')
    
    try:
        # [최신화] cfg의 새로운 구글 시트 명명 규칙 반영
        # GS_FILE_USER_PORT ("MyPortfolio") 파일에서 GS_SHEET_USER_LIST ("TheList") 시트를 읽어옵니다.
        port_sh = client.open(cfg.GS_FILE_USER_PORT).worksheet(cfg.GS_SHEET_USER_LIST)
        port_codes = [format_code(row[0]) for row in port_sh.get_all_values()[1:] if row and row[0]]

    except Exception as e:
        print(f"⚠️ 포트폴리오 로드 실패: {e}")
        port_codes = []

    # [핵심 변경] KOSPI 200, KOSDAQ 150 숫자 대신 cfg의 리미트값 사용
    market_tasks = [
        ("KOSPI", cfg.STOCK_DB_KOSPI, cfg.LIMIT_KOSPI),
        ("KOSDAQ", cfg.STOCK_DB_KOSDAQ, cfg.LIMIT_KOSDAQ)
    ]

    for label, file_path, top_n in market_tasks:
        print(f"\n🚀 {label} 엔진 가동 중 (대상: {file_path}, 목표: {top_n}개)")
        
        if os.path.exists(file_path):
            existing_df = pd.read_parquet(file_path)
            existing_codes = set(existing_df['종목코드'].unique())
        else:
            existing_codes = set()
        
        m_df = krx[krx['Market'].str.contains(label, na=False)]
        top_codes = m_df.sort_values('Marcap', ascending=False).head(top_n)['Code'].apply(format_code).tolist()
        my_codes_in_market = [c for c in port_codes if c.replace("'","") in m_df['Code'].values]
        final_targets = list(set(top_codes + my_codes_in_market))
        
        all_collected = []
        for i, code in enumerate(final_targets):
            raw_code = code.replace("'", "")
            try:
                name = m_df[m_df['Code'] == raw_code]['Name'].values[0]
                is_new = code not in existing_codes
                all_collected.extend(fetch_stock_data_final(token, code, name, label, target_date_str, is_new=is_new))
            except Exception as e:
                print(f"❌ {code} 처리 중 에러: {e}")
            if (i+1) % 10 == 0: print(f"    ({i+1}/{len(final_targets)}) 수집 완료...")
            
        update_database_parquet(file_path, all_collected)
        print(f"✅ {label} 업데이트 완료!")

    print(f"\n🏁 전체 공정 종료: {datetime.now()}")

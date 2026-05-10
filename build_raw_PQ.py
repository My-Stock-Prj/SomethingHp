# 이 코드는 build_raw_PQ.py full > 20일 확장
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import time
import traceback
from datetime import datetime
import kis_auth as ka  # 보강된 kis_auth 사용
import gspread

# --- [설정 로드] ---
BASE_DIR = os.environ.get('DATA_BASE_DIR', './DB')
SAVE_PATH = os.path.join(BASE_DIR, 'raw_daily_PQ.parquet')
MST_PATH = os.path.join(BASE_DIR, 'raw_mst_krx_full.parquet')

def get_combined_targets():
    print("🔍 [DEBUG] 1. 수집 대상 종목 분석 시작...")
    try:
        idx_tickers = []
        # 마스터 정보를 메모리에 사전에 담아두기 위한 딕셔너리
        mst_info_map = {}
        
        if os.path.exists(MST_PATH):
            df_mst = pd.read_parquet(MST_PATH)
            
            if not df_mst.empty:
                # [라벨링 로직] K200 > K150 > MY 순으로 구분값 부여
                cond_k200 = (df_mst.get('KOSPI200') == 'Y')
                cond_k150 = (df_mst.get('KOSDAQ150') == 'Y')
                
                # 타겟 추출
                idx_tickers = df_mst[cond_k200 | cond_k150]['단축코드'].unique().tolist()
                
                # 메모리 맵 생성 (종목명, 구분 저장)
                for _, row in df_mst.iterrows():
                    code = str(row['단축코드']).strip().zfill(6)
                    label = "MY"
                    if row.get('KOSPI200') == 'Y': label = "K200"
                    elif row.get('KOSDAQ150') == 'Y': label = "K150"
                    
                    mst_info_map[code] = {
                        "종목명": row.get('종목명', ''),
                        "구분": label
                    }
                
                print(f"   - 마스터 필터링 결과: {len(idx_tickers)} 건")
        
        # 구글 시트 부분
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = ka.get_gcp_creds(scopes)
        gsheet_tickers = []
        if creds:
            try:
                client = gspread.authorize(creds)
                sheet = client.open("my").worksheet("goingup")
                gsheet_tickers = sheet.col_values(1)[1:]
            except Exception as e:
                print(f"   ⚠️ 구글 시트 로드 실패: {e}")
        
        # 최종 합계 및 중복 제거
        all_tickers = list(set([str(t).strip().zfill(6) for t in (idx_tickers + gsheet_tickers) if t]))
        print(f"🚀 최종 수집 대상: {len(all_tickers)} 건")
        
        return all_tickers, mst_info_map
        
    except Exception as e:
        print(f"❌ [ERROR] get_combined_targets 실패: {str(e)}")
        return [], {}
        
def fetch_daily_price(ticker, target_date, mst_info):
    try:
        # 1. 일별 차트 시세 조회 (FHKST03010100)
        res = ka.get_daily_price(ticker, target_date, target_date)
        out1 = res.get('output1', ka.AttrDict({}))
        out2_list = res.get('output2', [])
        
        if not out2_list:
            return None
            
        d2 = ka.AttrDict(out2_list[0])
        
        # [검증] 날짜 일치 여부 확인
        if d2.stck_bsop_date != target_date:
            return None

        # 2. 투자자 매매동향 조회 (FHPTJ04160001)
        res_inv = ka.get_investor_trade(ticker, target_date)
        inv_list = res_inv.get('output2', [])
        inv = ka.AttrDict(inv_list[0]) if inv_list else ka.AttrDict({})

        # 3. 프로그램 매매추이 조회 (FHPPG04650201)
        res_pgm = ka.get_program_trade(ticker, target_date)
        pgm_list = res_pgm.get('output', [])  # [수정 반영] output2 -> output
        pgm = ka.AttrDict(pgm_list[0]) if pgm_list else ka.AttrDict({})

        # 4. 공매도 일별추이 조회 (FHPST04830000)
        res_shrt = ka.get_short_sale_daily(ticker, target_date)
        shrt_list = res_shrt.get('output2', []) # [수정 반영] output -> output2
        shrt = ka.AttrDict(shrt_list[0]) if shrt_list else ka.AttrDict({})

        # 5. 대차거래추이 조회 (HHPST074500C0)
        res_loan = ka.get_loan_trans_daily(ticker, target_date)
        loan_list = res_loan.get('output', [])
        loan = ka.AttrDict(loan_list[0]) if loan_list else ka.AttrDict({})

        # 6. 신용잔고추이 조회 (FHPST04760000)
        res_cred = ka.get_credit_balance_daily(ticker, target_date)
        cred_list = res_cred.get('output', [])
        cred = ka.AttrDict(cred_list[0]) if cred_list else ka.AttrDict({})

        # [36개 칼럼 정밀 매핑]
        return {
            # --- 시세 데이터 (14개) ---
            "날짜": target_date,
            "종목코드": ticker,
            "종목명": mst_info.get("종목명", ""),
            "구분(출처)": mst_info.get("구분", "MY"),
            "종가": ka.to_int(d2.stck_clpr),
            "시가": ka.to_int(d2.stck_oprc),
            "고가": ka.to_int(d2.stck_hgpr),
            "저가": ka.to_int(d2.stck_lwpr),
            "거래량": ka.to_int(out1.acml_vol),
            "거래대금": ka.to_int(out1.acml_tr_pbmn),
            "회전율": ka.to_float(out1.vol_tnrt),
            "상장주수": ka.to_int(out1.lstn_stcn),
            "락구분": d2.flng_cls_code,
            "재평가사유": d2.revl_issu_reas,
            
            # --- 투자자 매매동향 (14개) ---
            "외국인순매수수량": ka.to_int(inv.frgn_ntby_qty),
            "외국인순매수대금": ka.to_int(inv.frgn_ntby_tr_pbmn),
            "기관계순매수수량": ka.to_int(inv.orgn_ntby_qty),
            "기관계순매수대금": ka.to_int(inv.orgn_ntby_tr_pbmn),
            "기금순매수수량": ka.to_int(inv.fund_ntby_qty),
            "기금순매수대금": ka.to_int(inv.fund_ntby_tr_pbmn),
            "개인순매수수량": ka.to_int(inv.prsn_ntby_qty),
            "개인순매수대금": ka.to_int(inv.prsn_ntby_tr_pbmn),
            "증권순매수수량": ka.to_int(inv.scrt_ntby_qty),
            "투자신탁순매수수량": ka.to_int(inv.ivtr_ntby_qty),
            "사모펀드순매수수량": ka.to_int(inv.pe_fund_ntby_vol),
            "은행순매수수량": ka.to_int(inv.bank_ntby_qty),
            "보험순매수수량": ka.to_int(inv.insu_ntby_qty),
            "종금순매수수량": ka.to_int(inv.mrbn_ntby_qty),

            # --- 프로그램 매매추이 (2개) ---
            "프로그램순매수수량": ka.to_int(pgm.whol_smtn_ntby_qty),
            "프로그램순매수대금": ka.to_int(pgm.whol_smtn_ntby_tr_pbmn),

            # --- 공매도/대차/신용 (신규 6개) ---
            "공매도체결수량": ka.to_int(shrt.ssts_cntg_qty),
            "누적공매도체결수량": ka.to_int(shrt.acml_ssts_cntg_qty),
            "공매도거래량비중": ka.to_float(shrt.ssts_vol_rlim),
            "당일대차잔고주수": ka.to_int(loan.rmnd_stcn),
            "전체융자잔고주수": ka.to_int(cred.whol_loan_rmnd_stcn),
            "전체융자잔고비율": ka.to_float(cred.whol_loan_rmnd_rate)
        }
            
    except Exception as e:
        print(f"⚠️ [{ticker}] 데이터 처리 실패: {str(e)}")
    return None

def main():
    print(f"🚀 {datetime.now()} 프로세스 시작 (최근 20거래일 소급 수집)")
    try:
        # 1. 대상 리스트 및 마스터 맵 확보
        tickers, mst_info_map = get_combined_targets()
        if not tickers:
            print("⚠️ 수집할 종목이 없습니다.")
            return

        # 2. 최근 20영업일(평일) 날짜 리스트 생성
        date_list = pd.bdate_range(end=datetime.now(), periods=20).strftime('%Y%m%d').tolist()
        target_dates = date_list[:10]  # 앞부분 10일만 선택
        print(f"📅 1차 수집 기간: {target_dates[0]} ~ {target_dates[-1]}")
        
        collected = []
        total_tickers = len(tickers)
        
        # 3. 이중 루프: 날짜별 -> 종목별 수집
        for target_date in date_list:
            print(f"📂 {target_date} 수집 시작...")
            day_count = 0
            
            for i, ticker in enumerate(tickers):
                mst_info = mst_info_map.get(ticker, {"종목명": "", "구분": "MY"})
                
                res = fetch_daily_price(ticker, target_date, mst_info)
                if res: 
                    collected.append(res)
                    day_count += 1
                
                if (i + 1) % 50 == 0:
                    print(f"   ⏳ {target_date} 진행 중... ({i+1}/{total_tickers})")
            
            print(f"✅ {target_date} 완료: {day_count}건 수집됨")

        # 4. 저장 로직
        if collected:
            df_new = pd.DataFrame(collected)
            
            # 칼럼 순서 보장
            base_cols = ["날짜", "종목코드", "종목명", "구분(출처)", "종가", "시가", "고가", "저가", "거래량", "거래대금", "회전율", "상장주수", "락구분", "재평가사유"]
            investor_cols = [
                "외국인순매수수량", "외국인순매수대금", "기관계순매수수량", "기관계순매수대금", "기금순매수수량", "기금순매수대금",
                "개인순매수수량", "개인순매수대금", "증권순매수수량", "투자신탁순매수수량", "사모펀드순매수수량", "은행순매수수량",
                "보험순매수수량", "종금순매수수량"
            ]
            program_cols = ["프로그램순매수수량", "프로그램순매수대금"]
            extended_cols = ["공매도체결수량", "누적공매도체결수량", "공매도거래량비중", "당일대차잔고주수", "전체융자잔고주수", "전체융자잔고비율"]
            
            df_new = df_new[base_cols + investor_cols + program_cols + extended_cols]

            if os.path.exists(SAVE_PATH):
                df_old = pd.read_parquet(SAVE_PATH)
                # 기존 데이터와 병합 후 중복 제거 (날짜, 종목코드 기준 최신값 유지)
                df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=['날짜', '종목코드'], keep='last')
            else:
                df_final = df_new
            
            # 상위 폴더 생성 및 저장
            os.makedirs(os.path.dirname(SAVE_PATH), exist_ok=True)
            df_final.to_parquet(SAVE_PATH, index=False)
            print(f"✅ 저장 완료: {SAVE_PATH}")
            print(f"📊 최종 데이터 총계: {len(df_final)} rows")
        else:
            print("ℹ️ 수집된 신규 데이터가 없습니다.")

    except Exception as e:
        print(f"❌ [CRITICAL ERROR] {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

# 이 코드는 L_Monthly_MST.py config 적용 버전
# -*- coding: utf-8 -*-
import os
import ssl
import zipfile
import urllib.request
import pandas as pd
import gspread
import json
from google.oauth2.service_account import Credentials

# [1] 중앙 설정 모듈 도입
import C_Global_Config as cfg

# 1. SSL 설정 및 경로 준비 (Config 기반)
ssl._create_default_https_context = ssl._create_unverified_context
base_dir = os.getcwd()
save_path = cfg.BASE_DIR  # Config의 BASE_DIR 사용

if not os.path.exists(save_path):
    os.makedirs(save_path)

def download_and_extract(url, zip_name):
    print(f"📡 {zip_name} 다운로드 중...")
    target_path = os.path.join(base_dir, zip_name)
    urllib.request.urlretrieve(url, target_path)
    with zipfile.ZipFile(target_path) as z:
        z.extractall(base_dir)
    os.remove(target_path)

def get_domestic_master_df(market_type):
    """국내 종목 마스터 정제 (로직 유지)"""
    file_name = "kospi_code.mst" if market_type == "KOSPI" else "kosdaq_code.mst"
    if market_type == "KOSPI":
        specs = [2, 1, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21, 2, 7, 1, 1, 1, 1, 1, 9, 9, 9, 5, 9, 8, 9, 3, 1, 1, 1]
        cols = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류', '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100', 'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100', 'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC', 'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설', 'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송', 'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지', '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시', '우회상장', '락구분', '액면변경', '증자구분', '증거금비율', '신용가능', '신용기간', '전일거래량', '액면가', '상장일자', '상장주수', '자본금', '결산월', '공모가', '우선주', '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액', '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월', '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능']
    else:
        specs = [2, 1, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 9, 5, 5, 1, 1, 1, 2, 1, 1, 1, 2, 2, 2, 3, 1, 3, 12, 12, 8, 15, 21, 2, 7, 1, 1, 1, 1, 9, 9, 9, 5, 9, 8, 9, 3, 1, 1]
        cols = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '벤처기업', '저유동성', 'KRX', 'ETP', 'ELW발행', 'KRX100', 'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC', 'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설', 'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송', 'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지', '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시', '우회상장', '락구분', '액면변경', '증자구분', '증거금비율', '신용가능', '신용기간', '전일거래량', '액면가', '상장일자', '상장주수', '자본금', '결산월', '공모가', '우선주', '공매도과열', '이상급등', 'KRX300', 'KOSDAQ', '매출액', '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월', '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능']

    data = []
    with open(os.path.join(base_dir, file_name), mode="r", encoding="cp949") as f:
        for row in f:
            part1 = row[0:len(row)-sum(specs)-1]
            iscd, stnd_iscd, name = part1[0:9].strip(), part1[9:21].strip(), part1[21:].strip()
            part2 = row[-sum(specs)-1:-1]
            row_dict, curr_pos = {'단축코드': iscd, '표준코드': stnd_iscd, '한글명': name}, 0
            for i, width in enumerate(specs):
                row_dict[cols[i]] = part2[curr_pos:curr_pos+width].strip()
                curr_pos += width
            data.append(row_dict)
    return pd.DataFrame(data)

def refine_krx_data(df, market_label):
    """마스터 DB 축약 및 정제"""
    refined_list = []
    sector_cols = [c for c in df.columns if 'KRX' in c and c not in ['KRX', 'KRX100', 'KRX300']]

    for _, row in df.iterrows():
        main_sector = "기타"
        for s in sector_cols:
            if row[s] == '1':
                main_sector = s.replace('KRX', '')
                break
        
        detail_market = "일반"
        if row.get('KOSPI200섹터업종') and row['KOSPI200섹터업종'] != '': detail_market = "KOSPI200"
        if row.get('KRX300') == '1': detail_market = "KRX300"

        status = "정상"
        if row.get('거래정지') == '1': status = "거래정지"
        elif row.get('정리매매') == '1': status = "정리매매"
        elif row.get('관리종목') == '1': status = "관리종목"
        elif row.get('시장경고') != '0': status = "시장경고"
        elif row.get('락구분') != '00': status = f"락({row['락구분']})"
        elif row.get('증자구분') != '00': status = "증자"
        elif row.get('액면변경') != '00': status = "액면변경"

        refined_list.append({
            '단축코드': row['단축코드'],
            '한글명': row['한글명'],
            '시장구분': market_label,
            '시장구분상세': detail_market,
            '대표섹터': main_sector,
            '종목상태': status,
            '상장주수': pd.to_numeric(row.get('상장주수', 0), errors='coerce'),
            '시가총액': pd.to_numeric(row.get('시가총액', 0), errors='coerce'),
            '신용/증거금': f"{row.get('신용가능','N')}/{row.get('증거금비율','100')}%",
            '공매도과열': "YES" if row.get('공매도과열') == '1' else "NO"
        })
    return pd.DataFrame(refined_list)

def update_google_sheet_krx(df, sheet_name, tab_name):
    """MyPortfolio 파일 내 KRX 시트 업데이트"""
    creds_json = json.loads(os.environ.get("GCP_CREDENTIALS"))
    gc = gspread.authorize(Credentials.from_service_account_info(creds_json, scopes=["https://www.googleapis.com/auth/drive", "https://spreadsheets.google.com/feeds"]))
    sh = gc.open(sheet_name)
    
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="5000", cols="20")
    
    ws.update([df.columns.values.tolist()] + df.fillna("").values.tolist())
    note = "중요도 순서: 거래정지 > 정리매매 > 관리종목 > 시장경고 > 락/증자/액면변경.\n정상 이외의 상태는 분석 제외 권장."
    ws.update_notes({'F1': note})
    print(f"✅ {tab_name} 시트 업데이트 및 가이드 메모 완료!")

if __name__ == "__main__":
    # 1. 파일 다운로드
    download_and_extract("https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip", "kospi_code.zip")
    download_and_extract("https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip", "kosdaq_code.zip")

    # 2. 로우 데이터프레임 생성
    df_kospi_raw = get_domestic_master_df("KOSPI")
    df_kosdaq_raw = get_domestic_master_df("KOSDAQ")

    # 3. 개별 Parquet 저장 (경로 수정됨)
    df_kospi_raw.to_parquet(os.path.join(save_path, "DB_MST_KOSPI.parquet"), index=False)
    df_kosdaq_raw.to_parquet(os.path.join(save_path, "DB_MST_KOSDAQ.parquet"), index=False)
    print(f"💾 개별 Parquet 파일 저장 완료 ({save_path}/)")

    # 4. 데이터 축약 및 통합
    df_kospi_refined = refine_krx_data(df_kospi_raw, "KOSPI")
    df_kosdaq_refined = refine_krx_data(df_kosdaq_raw, "KOSDAQ")
    df_krx_total = pd.concat([df_kospi_refined, df_kosdaq_refined], ignore_index=True)

    # 5. 통합 Parquet 저장 (Config 경로 사용)
    df_krx_total.to_parquet(cfg.PATH_MST, index=False)
    print(f"💾 통합 마스터 {cfg.PATH_MST} 저장 완료")

    # 6. 구글 시트 업데이트 (Config 변수 사용)
    update_google_sheet_krx(df_krx_total, cfg.RPT_PORTFOLIO_NAME, cfg.RPT_SHEET_KRX)

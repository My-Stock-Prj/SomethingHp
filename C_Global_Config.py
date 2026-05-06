# 이 코드의 파일명은 C_Global_Config.py 사후분석 적용버전
# -*- coding: utf-8 -*-
import os

"""
[C_Global_Config.py] - Ver 4.2 (사후 검증 및 매크로 가점 시스템 도입 버전)
- 전략 파라미터 통합: L_Daily_ADB_Report.py 내 하드코딩 수치를 관리 변수로 이관
- 사후 검증 엔진(Analyzer) 지원: 과거 승률 분석 및 변수 최적화 파라미터 추가
- 매크로 가점(Macro Weight) 지원: 글로벌 지표(NASDAQ, SOX, WTI)와 ADB_SUM_BASE '섹터' 연동
"""

# --- [1] 경로 및 공통 시스템 설정 (PATH_) ---
BASE_DIR = "StockPrj0"

PATH_KOSPI    = os.path.join(BASE_DIR, "DB_Daily_KOSPI.parquet")
PATH_KOSDAQ   = os.path.join(BASE_DIR, "DB_Daily_KOSDAQ.parquet")
PATH_INDEX    = os.path.join(BASE_DIR, "DB_Daily_INDEX.parquet")
PATH_MST      = os.path.join(BASE_DIR, "DB_MST_KRX.parquet")
PATH_ADB_SUM  = os.path.join(BASE_DIR, "ADB_SUM_BASE.parquet")
PATH_TRACKER  = os.path.join(BASE_DIR, "ADB_Step_Tracker.parquet")

# (기존 코드 호환용 별칭)
STOCK_DB_KOSPI = PATH_KOSPI
STOCK_DB_KOSDAQ = PATH_KOSDAQ


# --- [2] L_Daily_ADB_PQ.py 관련 (PQ_) ---
PQ_SLEEP_TIME    = float(os.environ.get('K_API_SLEEP', 0.1))
PQ_KOSPI_LIMIT   = int(os.environ.get('TOP_N_KOSPI', 200))
PQ_KOSDAQ_LIMIT  = int(os.environ.get('TOP_N_KOSDAQ', 150))
PQ_BACKFILL_DAYS = int(os.environ.get('BACKFILL_DAYS', 40))

# (기존 코드 호환용 별칭)[cite: 7]
SLEEP_TIME = PQ_SLEEP_TIME
LIMIT_KOSPI = PQ_KOSPI_LIMIT
LIMIT_KOSDAQ = PQ_KOSDAQ_LIMIT
BACKFILL_DAYS = PQ_BACKFILL_DAYS


# --- [3] L_Daily_ADB_Update.py 관련 (ADB_) ---
ADB_KEEP_DAYS = int(os.environ.get('ADB_KEEP_DAYS') or 40)
ADB_NUM_COLS = ['거래량', '거래대금', '외국인순매수', '기관순매수', '기금순매수', '공매도평균단가', '공매도수량']


# --- [4] 구글 시트 시스템 설정 (GS_) ---
# [4-1] 구글 시트 파일(Spreadsheet) 정의
GS_FILE_REPORT      = "V3_Report"         # [출력] 최종 분석 리포트 파일
GS_FILE_T_REPORT    = "Report2Tomorrow"   # [출력] 최종 분석 리포트 파일 T버전용
GS_FILE_ANALYZER    = "V3_Report"         # [기록] 성과 추적 및 사후 분석 파일
GS_FILE_T_ANALYZER  = "Report2Tomorrow"   # [기록] T버전 성과 추적 및 분석 파일
GS_FILE_USER_PORT   = "MyPortfolio"       # [입력] 사용자 보유 종목 관리 파일 (신규 표준)

# [4-2] 세부 시트(Worksheet) 정의
# (1) 운영(Main) 리포트 관련
GS_SHEET_RPT_MAIN   = "Today"             # 리포트 메인 출력 시트
GS_SHEET_ANA_TRACE  = "Trace"             # 사후 분석 데이터 기록 시트
GS_SHEET_ANA_KRX    = "KRX"               # 마스터 데이터 참조 시트

# (2) 테스트(T) 리포트 관련
GS_SHEET_TRPT_MAIN  = "T_Today"           # T-리포트 메인 출력 시트
GS_SHEET_TANA_TRACE = "T_Trace"           # T-사후 분석 데이터 기록 시트

# (3) 사용자 포트폴리오 관련
GS_SHEET_USER_LIST  = "TheList"           # 사용자 보유 종목 리스트 시트

# --- [하위 호환성 유지를 위한 기존 변수 매핑 (RPT_ / GS_)] ---
# RPT_GSHEET_NAME     = GS_FILE_REPORT
# RPT_PORTFOLIO_NAME  = GS_FILE_ANALYZER
# RPT_TRACE_SHEET     = GS_SHEET_ANA_TRACE
# RPT_SHEET_MAIN      = GS_SHEET_RPT_MAIN
# RPT_SHEET_PORT      = GS_SHEET_USER_LIST
# RPT_SHEET_KRX       = GS_SHEET_ANA_KRX

# 기존 코드 호환용 별칭 (Deprecated 예정)
# GSHEET_NAME         = RPT_PORTFOLIO_NAME 
# RPT_SHEET_TITLE     = RPT_SHEET_MAIN


# ⭐ [전략 파라미터] 리포트 판정 로직 제어 (20일 시계열 Waterfall 구조)[cite: 7]
# ------------------------------------------------------------------------------
# [Step 1] 에너지(Energy): 시장 주도주 및 자금 유입 포착
RPT_VOLATILITY_3D  = 0.05    # [1단계] 3일 누적 수익률 기준
RPT_RS_SCORE_20    = 1.0     # [1단계] 20일 지수대비 상대강도(RS) 합계
RPT_VOL_INTENSITY  = 2.0     # [1단계] 20일 평균대비 당일 거래대금 배수

# [Step 2] 세이프티(Safety): 물리적 리스크 종목 조기 제거
RPT_RISK_LIMIT     = 13.5    # [2단계] 최대 허용 리스크 점수 합계 (↓ 작을수록 엄격)
RPT_SHORT_RATIO    = 0.1     # [2단계] 당일 거래량 대비 공매도 수량 비중
RPT_CREDIT_LIMIT   = 8.0     # [2단계] 종목의 신용잔고율 절대치 한계

# [Step 3] 수급(Supply): 메이저 매집 밀도 및 수급 교체 확인
RPT_MAJOR_POWER    = 0.10    # [3단계] 20일 누적 거래대금 중 메이저 순매수 비중
RPT_FND_DAYS       = 7       # [3단계] 20일 중 연기금 매수 발생 일수
RPT_ANT_OUT_DAYS   = 4       # [3단계] 최근 n일 누적 개인 순매수량 (0 미만이어야 함)
RPT_AVG_PRICE_DEV  = 0.03    # [3단계] 메이저 추정평단 대비 현재가 이격

# [Step 4] 응축(Concentration): 기술적 돌파 및 정배열 타점 확정
RPT_PRICE_POS      = 0.95    # [4단계] 20일 고가 대비 현재가 위치
RPT_MAJOR_DAYS     = 5       # [4단계] 20일 중 외인/기관 동시 매수 일수

# [기타 진단/목표 파라미터]
RPT_VOL_EXPLOSION  = 3.8     # [진단] 10일 평균 대비 당일 거래량 폭발 배수
RPT_OVERHEAT_RATIO = 1.15    # [진단] 5일 이평선 대비 현재가 이격 비율
RPT_PORT_RISK_MAX  = 20.0    # [진단] 포트폴리오 위험회피 리스크 점수
RPT_TARGET_RATIO   = 1.07    # [진단] 20일 최고가 대비 목표가 설정 비율
RPT_EXIT_RATIO     = 0.97    # [진단] 추정 평단가 대비 손절 기준 비율

# 🧪 [시뮬레이션 전용 파라미터 (Sim_)]
# ------------------------------------------------------------------------------
# [Step 1] 에너지(Energy)
Sim_RPT_VOLATILITY_3D  = 0.05    
Sim_RPT_RS_SCORE_20    = 1.0     
Sim_RPT_VOL_INTENSITY  = 2.0     

# [Step 2] 세이프티(Safety)
Sim_RPT_RISK_LIMIT     = 13.5    
Sim_RPT_SHORT_RATIO    = 0.1     
Sim_RPT_CREDIT_LIMIT   = 8.0     

# [Step 3] 수급(Supply)
Sim_RPT_MAJOR_POWER    = 0.05    
Sim_RPT_FND_DAYS       = 7       
Sim_RPT_ANT_OUT_DAYS   = 2       
Sim_RPT_AVG_PRICE_DEV  = 0.05    

# [Step 4] 응축(Concentration)
Sim_RPT_PRICE_POS      = 0.90    
Sim_RPT_MAJOR_DAYS     = 3       

# [기타 진단/목표]
Sim_RPT_VOL_EXPLOSION  = 3.8     
Sim_RPT_OVERHEAT_RATIO = 1.15    
Sim_RPT_PORT_RISK_MAX  = 20.0    
Sim_RPT_TARGET_RATIO   = 1.07    
Sim_RPT_EXIT_RATIO     = 0.97    
# ------------------------------------------------------------------------------


# --- [5] L_Daily_ADB_Analyzer.py 관련 (VAL_ / ANA_) ---
# 사후 검증 엔진 및 전략 보정용 파라미터[cite: 7]
ANA_STRATEGY_VER   = "V4.2_Sector_Macro" # 분석에 사용된 현재 전략 버전
VAL_LOOKBACK_DAYS  = 20            # [수정] 충분한 표본 확보를 위해 20일로 조정[cite: 7]
VAL_TARGET_D_DAYS  = [1, 3, 5]     # 추천 이후 성과 측정 기준일 (T+1, T+3, T+5)
VAL_MIN_WIN_RATE   = 0.60          # 전략 신뢰도 임계치 (60% 미만 시 보정 제언)
VAL_SIM_RANGE_PCT  = 0.10          # [수정] 미세 조정을 위해 10% 범위로 설정[cite: 7]

# 🧪 [시뮬레이션 전용 파라미터 (Sim_)]
Sim_ANA_STRATEGY_VER   = "Sim_V4.2_Test" 
Sim_VAL_LOOKBACK_DAYS  = 20            
Sim_VAL_TARGET_D_DAYS  = [1, 3, 5]     
Sim_VAL_MIN_WIN_RATE   = 0.60          
Sim_VAL_SIM_RANGE_PCT  = 0.10          

# Tracker 적재 시 스냅샷을 남길 설정 변수 리스트[cite: 7]
TRACKER_SNAPSHOT_COLS = [
    'RPT_VOLATILITY_3D', 'RPT_VOL_INTENSITY', 'RPT_RISK_LIMIT', 
    'RPT_MAJOR_POWER', 'RPT_PRICE_POS', 'RPT_AVG_PRICE_DEV'
]


# --- [6] 글로벌 매크로 지표 가점 관련 (MACRO_) ---
# INDEX 파일의 지표 변화에 따른 섹터별 가중치 제어[cite: 7]
MACRO_UP_LIMIT     = 0.015         # [수정] 유의미한 가점을 위해 1.5% 등락률 기준 적용
MACRO_WEIGHT_GEN   = 0.05          # 일반 시장 지수(NASDAQ, S&P) 강세 가점
MACRO_WEIGHT_SOX   = 0.15          # 반도체 지수 강세 시 반도체 섹터 가점
MACRO_WEIGHT_WTI   = 0.10          # 유가 변동 시 에너지 섹터 가점

# 매크로 지표와 ADB_SUM_BASE '섹터' 칼럼 간의 매칭 정의[cite: 7]
MACRO_SECTOR_MAP = {
    'SOX': ['반도체', 'IT', '소부장', '전기전자'], # 반도체 지수 관련 키워드
    'WTI': ['정유', '화학', '에너지', '조선'],     # 유가 관련 키워드
    'NDAQ': ['소프트웨어', '바이오', '인터넷']      # 나스닥 가점 섹터 키워드
}


# --- [7] 설정 확인용 유틸리티 ---
def print_config_status():
    print(f"\n🚀 [System Config Loaded - Ver {ANA_STRATEGY_VER} (Sector-Macro Adaptive)]")
    print(f"    - [PATH] ADB SUM: {PATH_ADB_SUM} / Tracker: {PATH_TRACKER}")
    print(f"    - [GS] Report: {GS_FILE_REPORT} / Analyzer: {GS_FILE_ANALYZER}")
    print(f"    - [MACRO] SOX Keywords: {MACRO_SECTOR_MAP['SOX']}")
    print(f"    - [RPT] Risk Limit: {RPT_RISK_LIMIT} pts / AntOut: {RPT_ANT_OUT_DAYS}d\n")

if __name__ == "__main__":
    print_config_status()

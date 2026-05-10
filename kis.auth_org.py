# -*- coding: utf-8 -*-
import os
import json
import requests
import time

# [설정] GitHub Secrets 환경변수 참조 - 무조건 실전용 URL 사용
URL = os.environ.get("URL", "https://openapi.koreainvestment.com:9443")
APP_KEY = os.environ.get("APP_KEY")
APP_SECRET = os.environ.get("APP_SECRET")

_ACCESS_TOKEN = None

class TREnv:
    """기존 코드에서 tre.my_token 식으로 접근하므로 해당 구조 유지"""
    def __init__(self, token, app, sec, url):
        self.my_token = token
        self.my_app = app
        self.my_sec = sec
        self.my_url = url

def get_access_token():
    global _ACCESS_TOKEN
    if _ACCESS_TOKEN:
        return _ACCESS_TOKEN

    if not APP_KEY or not APP_SECRET:
        raise ValueError("[ERROR] APP_KEY 또는 APP_SECRET 환경변수가 없습니다. GitHub Secrets 설정을 확인하세요.")

    path = "oauth2/tokenP"
    url = f"{URL}/{path}"
    data = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    
    # 한국투자증권 규격: 인증 시에는 Content-Type만 필요
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(data))
    
    if res.status_code == 200:
        _ACCESS_TOKEN = res.json()["access_token"]
        return _ACCESS_TOKEN
    else:
        raise Exception(f"인증 실패 (접근토큰 발급 불가): {res.text}")

def getTREnv():
    """상태 확인 및 환경 객체 반환"""
    token = get_access_token()
    return TREnv(token, APP_KEY, APP_SECRET, URL)

def _url_fetch(api_url, tr_id, tr_cont, params):
    """
    사용자가 요청한 Response/Body/Header 클래스 구조를 완벽히 구현한 호출 도구
    """
    class Response:
        def __init__(self, r):
            self.r = r

        def isOK(self): 
            # rt_cd가 '0'인 경우 성공으로 간주하는 한투 규격 반영 가능
            return self.r.status_code == 200

        def getBody(self):
            class Body:
                def __init__(self, d): 
                    self.__dict__.update(d)
                
                @property
                def output(self): return self.__dict__.get('output', [])
                
                @property
                def output2(self): return self.__dict__.get('output2', [])

            return Body(self.r.json())

        def getHeader(self):
            class Header:
                def __init__(self, h): 
                    # 한투는 응답 헤더에 tr_cont를 담아 보냄 (M: 다음 있음, D/공백: 마지막)
                    self.tr_cont = h.get('tr_cont', '')
            return Header(self.r.headers)

        def getErrorCode(self): return self.r.json().get('rt_cd', 'Unknown')

        def getErrorMessage(self): return self.r.json().get('msg1', 'No Message')

        def printError(self, url): 
            print(f"[ERROR] API Error: {self.r.status_code} URL: {url}\n{self.r.text}")

    tre = getTREnv()
    
    # [중요] Authorization: Bearer 뒤에 반드시 한 칸의 공백이 필요함
    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {tre.my_token}",
        "appkey": tre.my_app,
        "appsecret": tre.my_sec,
        "tr_id": tr_id,
        "custtype": "P",
        "tr_cont": tr_cont
    }

    # API 호출 (GET 방식 기준)
    res = requests.get(f"{tre.my_url}{api_url}", headers=headers, params=params)
    return Response(res)

def smart_sleep():
    """한투 API 초당 호출 제한(TPS) 방지를 위한 지연 (실전용 초당 2건 제한 대응)"""
    time.sleep(0.5)

import os
import sys
import time
import json
import csv
import logging
import select
import requests
import xmltodict
import xml.etree.ElementTree as ET
import re  # ★ 추가
from datetime import datetime, timezone, timedelta
from io import StringIO
from uuid import uuid4, uuid5, NAMESPACE_DNS
from dotenv import load_dotenv

# ★ 추가된 부분: 형태소 분석기
# Konlpy 설치가 필요합니다. (pip install konlpy)
from konlpy.tag import Okt
okt = Okt()

# ★ 추가된 부분: BeautifulSoup 임포트
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service

# 환경 변수 로드
load_dotenv()
API_KEY = os.getenv("API_KEY", "7dWUeNJAqaan8oJAs5CbDWKnWaJpLWoxd+lB97UDDRgFfSjfKD7ZGHxM+kRAoZqsga+WlheugBMS2q9WCSaUNg==")
EQ_API_KEY = os.getenv("EQ_API_KEY", "F5Iz7aHpRUSSM-2h6ZVE2w")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Cassandra 연결
class CassandraConnector:
    def __init__(self, keyspace="disaster_service"):
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.setup_cassandra_connection()

    def setup_cassandra_connection(self):
        for attempt in range(5):
            try:
                logging.info(f"Cassandra 연결 시도 중... (시도 {attempt + 1}/5)")
                from cassandra.auth import PlainTextAuthProvider
                auth_provider = PlainTextAuthProvider(username="andy013", password="1212")
                from cassandra.cluster import Cluster
                self.cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth_provider)
                self.session = self.cluster.connect(self.keyspace)
                logging.info("✅ Cassandra 연결 완료.")
                return
            except Exception as e:
                logging.error(f"❌ 연결 실패: {e}")
                time.sleep(10)
        raise Exception("Cassandra 연결 실패")

connector = CassandraConnector()

def insert_rtd_data(rtd_code: int, rtd_time: datetime, rtd_loc: str, rtd_details: list):
    """
    통합 데이터 테이블(rtd_db)에 데이터를 저장하는 함수입니다.
    결정론적 id 생성을 위해 uuid5를 사용합니다.
    """
    details_str = "_".join(rtd_details)
    # rtd_time이 None이 되지 않도록 주의
    if not rtd_time:
        rtd_time = datetime.now(timezone.utc)

    unique_str = f"{rtd_code}_{rtd_time.strftime('%Y%m%d%H%M%S')}_{rtd_loc}_{details_str}"
    record_id = uuid5(NAMESPACE_DNS, unique_str)

    query = """
    INSERT INTO rtd_db (rtd_code, rtd_time, id, rtd_loc, rtd_details)
    VALUES (%s, %s, %s, %s, %s) IF NOT EXISTS
    """
    try:
        from cassandra.query import SimpleStatement
        connector.session.execute(
            SimpleStatement(query),
            (rtd_code, rtd_time, record_id, rtd_loc, rtd_details)
        )
        logging.info(f"통합 데이터 저장 성공: {record_id} (코드: {rtd_code})")
    except Exception as e:
        logging.error(f"통합 데이터 저장 실패 (코드: {rtd_code}): {e}")

# --------------------------------------------------------------------------------
# 0. 기상특보(주의보/경보) 수집을 위한 STATION_CODES, WARNING_CODES 설정
# --------------------------------------------------------------------------------

# 전국의 기상청 지점 코드 (기상청 API 공식 문서 참고)
STATION_CODES = {
    90: "속초", 93: "북춘천", 95: "철원", 96: "독도", 98: "동두천",
    99: "파주", 100: "대관령", 101: "춘천", 102: "백령도", 104: "북강릉",
    105: "강릉", 106: "동해", 108: "서울", 112: "인천", 114: "원주",
    115: "울릉도", 116: "관악(레)", 119: "수원", 121: "영월", 127: "충주",
    129: "서산", 130: "울진", 131: "청주", 133: "대전", 135: "추풍령",
    136: "안동", 137: "상주", 138: "포항", 140: "군산", 143: "대구",
    146: "전주", 155: "창원", 156: "광주", 162: "통영", 165: "목포",
    168: "여수", 169: "흑산도", 170: "완도", 172: "고창", 174: "순천",
    175: "진도(레)", 177: "홍성", 184: "제주", 185: "고산", 188: "성산",
    189: "서귀포", 192: "진주", 201: "강화", 202: "양평", 203: "이천",
    211: "인제", 212: "홍천", 216: "태백", 217: "정선군", 221: "제천",
    226: "보은", 229: "북격렬비도", 232: "천안", 235: "보령", 236: "부여",
    238: "금산", 239: "세종", 243: "부안", 244: "임실", 245: "정읍",
    247: "남원", 248: "장수", 251: "고창군", 252: "영광군", 253: "김해시",
    254: "순창군", 255: "북창원", 257: "양산시", 258: "보성군", 259: "강진군",
    260: "장흥", 261: "해남", 262: "고흥", 263: "의령군", 264: "함양군",
    266: "광양시", 268: "진도군", 271: "봉화", 272: "영주", 273: "문경",
    276: "청송군", 277: "영덕", 278: "의성", 279: "구미", 281: "영천",
    283: "경주시", 284: "거창", 285: "합천", 288: "밀양", 289: "산청",
    294: "거제", 295: "남해", 296: "북부산", 300: "말도", 301: "임자도",
    302: "장산도", 303: "가거도", 304: "신지도", 305: "여서도", 306: "소리도",
    308: "옥도", 310: "궁촌", 311: "가야산", 312: "주왕산", 313: "양지암",
    314: "덕유봉", 315: "성삼재", 316: "무등산", 317: "모악산", 318: "용평",
    319: "천부", 320: "향로봉", 321: "원통", 322: "상서", 323: "마현",
    324: "송계", 325: "백운", 326: "용문산", 327: "우암산", 328: "중문",
    329: "산천단", 330: "대흘", 351: "남면", 352: "장흥면", 353: "덕정동",
    355: "서탄면", 356: "고덕면", 358: "현덕면", 359: "선단동", 360: "내촌면",
    361: "영중면", 364: "분당구", 365: "석수동", 366: "오전동", 367: "신현동",
    368: "수택동", 369: "수리산길", 370: "이동묵리", 371: "기흥구", 372: "은현면",
    373: "남방", 374: "청북", 375: "백석읍", 400: "강남", 401: "서초",
    402: "강동", 403: "송파", 404: "강서", 405: "양천", 406: "도봉",
    407: "노원", 408: "동대문", 409: "중랑", 410: "기상청", 411: "마포",
    412: "서대문", 413: "광진", 414: "성북", 415: "용산", 416: "은평",
    417: "금천", 418: "한강", 419: "중구", 421: "성동", 423: "구로",
    424: "강북", 425: "남현", 426: "백령(레)", 427: "김포장기", 428: "하남덕풍",
    430: "경기", 431: "신곡", 432: "향남", 433: "부천", 434: "안양",
    435: "고잔", 436: "역삼", 437: "광명", 438: "군포", 439: "진안",
    440: "설봉", 441: "김포", 442: "지월", 443: "보개", 444: "하남",
    445: "의왕", 446: "남촌", 447: "북내", 448: "산북", 449: "옥천",
    450: "주교", 451: "오남", 452: "신북", 453: "소하", 454: "하봉암",
    455: "읍내", 456: "연천", 457: "춘궁", 458: "퇴촌", 459: "오포",
    460: "실촌", 461: "마장", 462: "모가", 463: "흥천", 464: "점동",
    465: "가남", 466: "금사", 467: "양성", 468: "서운", 469: "일죽",
    470: "고삼", 471: "송탄", 472: "포승", 473: "가산", 474: "영북",
    475: "관인", 476: "화현", 477: "상패", 478: "왕징", 479: "장남"
}

# 재난 코드 매핑 (기상특보용)
WARNING_CODES = {
    "호우": 32,
    "강풍": 34,
    "대설": 35,
    "폭염": 41,
    "한파": 42
}

# --------------------------------------------------------------------------------
# 1. 대기질 예보 데이터 수집 및 저장 (rtd_code 72)
# --------------------------------------------------------------------------------
def get_air_inform():
    logging.info("대기질 예보 데이터 수집 시작")
    now = datetime.now()
    search_date = (now - timedelta(days=1)).strftime("%Y-%m-%d") if now.hour < 9 else now.strftime("%Y-%m-%d")
    params = {
        "searchDate": search_date,
        "returnType": "xml",
        "numOfRows": "100",
        "pageNo": "1",
        "serviceKey": API_KEY
    }
    try:
        response = requests.get("http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustFrcstDspth",
                                params=params, timeout=10)
        response.raise_for_status()
        logging.info("대기질 예보 API 연결 확인")
    except Exception as e:
        logging.error(f"Air Inform API 호출 실패: {e}")
        return {"status": "error", "data": []}

    data_dict = xmltodict.parse(response.text)
    items = data_dict.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    if not isinstance(items, list):
        items = [items]
    total_items = len(items)
    saved_count = 0

    from cassandra.query import SimpleStatement

    for item in items:
        record_id = f"{item.get('informData')}_{item.get('dataTime')}_{item.get('informCode')}"
        try:
            data_time = datetime.strptime(item["dataTime"].replace("시 발표", "").strip(), "%Y-%m-%d %H")
            forecast_date = datetime.strptime(item["informData"], "%Y-%m-%d").date()
        except Exception:
            data_time = datetime.now()
            forecast_date = None

        query = """
        INSERT INTO airinform (record_id, cause, code, data_time, forecast_date, grade, overall, search_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) IF NOT EXISTS
        """
        try:
            connector.session.execute(
                SimpleStatement(query),
                (
                    record_id,
                    item.get("informCause", ""),
                    item.get("informCode", ""),
                    data_time,
                    forecast_date,
                    item.get("informGrade", ""),
                    item.get("informOverall", ""),
                    datetime.now().date()
                )
            )
            saved_count += 1

            # 통합 데이터 저장 (대기질 예보: 72)
            rtd_details = [
                f"cause: {item.get('informCause', '')}",
                f"code: {item.get('informCode', '')}",
                f"grade: {item.get('informGrade', '')}",
                f"overall: {item.get('informOverall', '')}",
                f"search_date: {datetime.now().date()}"
            ]
            insert_rtd_data(72, data_time, "", rtd_details)
        except Exception as e:
            logging.error(f"DB 저장 실패 (record_id: {record_id}): {e}")

    logging.info(f"대기질 예보 데이터 저장 완료: 총 {total_items}건 중 {saved_count}건 저장됨")
    return {"status": "success", "data": items}

# --------------------------------------------------------------------------------
# 2. 실시간 대기질 등급 수집 및 저장 (rtd_code 71)
# --------------------------------------------------------------------------------
def get_air_grade():
    logging.info("실시간 대기질 등급 데이터 수집 시작")
    params = {
        "sidoName": "전국",
        "returnType": "xml",
        "serviceKey": API_KEY,
        "numOfRows": "1000",
        "pageNo": "1",
        "ver": "1.3"
    }
    try:
        response = requests.get(
            "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty",
            params=params, timeout=10
        )
        response.raise_for_status()
        logging.info("실시간 미세먼지 API 연결 확인")
    except Exception as e:
        logging.error(f"Air Grade API 실패: {e}")
        return {"status": "error", "data": []}

    data_dict = xmltodict.parse(response.text)
    items = data_dict.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    if isinstance(items, dict):
        items = [items]

    total_items = len(items)
    saved_count = 0
    korea_tz = timezone(timedelta(hours=9))
    from cassandra.query import SimpleStatement

    for item in items:
        if item.get("pm10Grade1h") is None and item.get("pm25Grade1h") is None:
            continue

        try:
            dt = datetime.strptime(item["dataTime"], "%Y-%m-%d %H:%M") \
                         .replace(tzinfo=korea_tz).astimezone(timezone.utc)
        except Exception:
            dt = datetime.utcnow()

        station = item.get("stationName")
        check_query = "SELECT pm_no FROM airgrade WHERE stationname=%s ALLOW FILTERING"
        result = connector.session.execute(SimpleStatement(check_query), (station,))
        row = result.one()

        pm10_grade = int(item.get("pm10Grade1h") or 0)
        pm25_grade = int(item.get("pm25Grade1h") or 0)

        if row:
            update = """
            UPDATE airgrade
            SET data_time=%s, pm10_grade=%s, pm25_grade=%s, sido=%s
            WHERE pm_no=%s
            """
            try:
                connector.session.execute(
                    SimpleStatement(update),
                    (
                        dt,
                        pm10_grade,
                        pm25_grade,
                        item.get("sidoName", ""),
                        row.pm_no
                    )
                )
                saved_count += 1
            except Exception as e:
                logging.error(f"업데이트 실패 ({station}): {e}")
        else:
            insert = """
            INSERT INTO airgrade (pm_no, data_time, pm10_grade, pm25_grade, sido, stationname)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            try:
                connector.session.execute(
                    SimpleStatement(insert),
                    (
                        uuid4(),
                        dt,
                        pm10_grade,
                        pm25_grade,
                        item.get("sidoName", ""),
                        station
                    )
                )
                saved_count += 1
            except Exception as e:
                logging.error(f"삽입 실패 ({station}): {e}")

        # 통합 데이터 저장 (미세먼지 시도별 측정정보: 71)
        # pm10_grade 혹은 pm25_grade 중 하나라도 3 이상인 경우에만 저장
        if pm10_grade >= 3 or pm25_grade >= 3:
            try:
                rtd_details = [
                    f"pm10_grade: {pm10_grade}",
                    f"pm25_grade: {pm25_grade}",
                    f"sido: {item.get('sidoName', '')}",
                    f"station: {station}"
                ]
                insert_rtd_data(71, dt, station, rtd_details)
            except Exception as e:
                logging.error(f"통합 미세먼지 데이터 저장 오류 ({station}): {e}")

    logging.info(f"실시간 대기질 등급 데이터 저장 완료: 총 {total_items}건 중 {saved_count}건 처리됨")
    return {"status": "success", "data": items}

# --------------------------------------------------------------------------------
# 3. 지진 정보 수집 및 저장 (rtd_code 51)
# --------------------------------------------------------------------------------
def fetch_earthquake_data():
    logging.info("지진 정보 수집 시작")
    kst = timezone(timedelta(hours=9))
    current_time = datetime.now(kst).strftime('%Y%m%d%H%M%S')
    url = f"https://apihub.kma.go.kr/api/typ01/url/eqk_now.php?tm={current_time}&disp=0&help=1&authKey={EQ_API_KEY}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        logging.info("지진 API 연결 확인")
        response.encoding = 'euc-kr'
        csv_data = csv.reader(StringIO(response.text))
    except Exception as e:
        logging.error(f"지진 API 오류: {e}")
        return

    # 최신 지진 시간 조회
    try:
        from cassandra.query import SimpleStatement
        max_time_result = connector.session.execute("SELECT eq_time FROM domestic_earthquake LIMIT 1")
        max_time_row = max_time_result.one()
        latest_eq_time = max_time_row.eq_time if max_time_row is not None else None
        if latest_eq_time is not None and latest_eq_time.tzinfo is None:
            latest_eq_time = latest_eq_time.replace(tzinfo=timezone.utc)
    except Exception as e:
        logging.error(f"지진 데이터 최신 eq_time 조회 오류: {e}")
        latest_eq_time = None

    total_rows = 0
    saved_count = 0
    for row in csv_data:
        if not row or row[0].strip().startswith("#"):
            continue
        total_rows += 1

        combined = " ".join(row)
        tokens = combined.strip().split()
        if len(tokens) < 7:
            continue
        tp = tokens[0]
        # tp=3 → 국내 지진 (기상청 API 문서 기준)
        if tp != "3":
            continue

        try:
            tm_eqk = tokens[3]
            dt = datetime.strptime(tm_eqk[:14], "%Y%m%d%H%M%S") \
                         .replace(tzinfo=kst).astimezone(timezone.utc)

            # 이미 저장된 시간보다 과거면 스킵
            if latest_eq_time is not None and dt <= latest_eq_time:
                logging.info(f"이미 저장된 최신 eq_time({latest_eq_time})보다 이전이므로 저장 안 함: {dt}")
                continue

            magnitude = float(tokens[4])
            lat_num = float(tokens[5])
            lon_num = float(tokens[6])
            location = " ".join(tokens[7:])
            msg = f"[{location}] 규모 {magnitude}"

            record_str = f"{dt.strftime('%Y%m%d%H%M%S')}_{lat_num}_{lon_num}_{magnitude}"
            record_id = uuid5(NAMESPACE_DNS, record_str)
            logging.info(f"생성된 record_id: {record_id} (type: {type(record_id)})")

            insert_stmt = """
            INSERT INTO domestic_earthquake (eq_no, eq_time, eq_lat, eq_lot, eq_mag, eq_msg)
            VALUES (%s, %s, %s, %s, %s, %s) IF NOT EXISTS
            """
            connector.session.execute(
                SimpleStatement(insert_stmt),
                (record_id, dt, lat_num, lon_num, magnitude, msg)
            )
            saved_count += 1

            # 통합 데이터 저장 (지진: 51)
            rtd_details = [
                f"magnitude: {magnitude}",
                f"location: {location}",
                f"latitude: {lat_num}",
                f"longitude: {lon_num}"
            ]
            insert_rtd_data(51, dt, location, rtd_details)

        except Exception as e:
            logging.error(f"지진 파싱 오류 (row: {row}): {e}")

    logging.info(f"지진 정보 저장 완료: 총 {total_rows} 행 중 {saved_count}건 저장됨")

# --------------------------------------------------------------------------------
# 4. 태풍 데이터 (rtd_code 31)
# --------------------------------------------------------------------------------
TYPHOON_CODE = 31
last_forecast_time = None  # 간단 예시로 전역 변수 사용

def fetch_typhoon_data():
    """
    기상청 태풍정보조회서비스(getTyphoonInfo) API를 호출하여
    태풍 정보를 파싱 후 리스트 형태로 반환한다.
    """
    global last_forecast_time

    # 현재 날짜 (yyyyMMdd)
    kst = timezone(timedelta(hours=9))
    current_date = datetime.now(kst).strftime('%Y%m%d')
    # API 호출 URL 및 파라미터 설정
    url = 'http://apis.data.go.kr/1360000/TyphoonInfoService/getTyphoonInfo'
    params = {
        'serviceKey': 'D0I8CLciGzwIaBmM6g6XitlVfgkLBO83zDl4EnUUoxifvRlSZHu78BqoixtzJg17Gb06up+NHzPXjN0cA7sLOg==',
        'pageNo': '1',
        'numOfRows': '10',
        'dataType': 'XML',
        'fromTmFc': current_date,
        'toTmFc': current_date
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"태풍 API 호출 실패: {e}")
        return []

    # XML 파싱
    root = ET.fromstring(response.content)
    items = root.findall('.//item')
    if not items:
        logging.info("태풍 데이터가 없습니다.")
        return []

    typhoon_data = []
    for item in items:
        forecast_time = item.findtext('tmFc')  # 예보 시각 (예: 202503201600)
        if not forecast_time:
            continue

        # 간단 중복 예시: 이전에 처리한 예보 시각이면 스킵
        if forecast_time == last_forecast_time:
            continue

        # KST -> UTC 변환
        korea_timezone = timezone(timedelta(hours=9))
        formatted_forecast_time = datetime.strptime(forecast_time, "%Y%m%d%H%M")
        formatted_forecast_time = formatted_forecast_time.replace(tzinfo=korea_timezone).astimezone(timezone.utc)

        # 태풍 정보 추출
        name = item.findtext('typName')       # 태풍 이름
        direction = item.findtext('typDir')   # 진행 방향
        lat_str = item.findtext('typLat')     # 위도
        lon_str = item.findtext('typLon')     # 경도
        loc = item.findtext('typLoc')         # 위치
        intensity = item.findtext('typInt')   # 강도 (예: "중", "강", "매우강")
        wind_str = item.findtext('typ15')     # 강풍 반경(15m/s)

        try:
            lat = float(lat_str) if lat_str else 0.0
        except:
            lat = 0.0
        try:
            lon = float(lon_str) if lon_str else 0.0
        except:
            lon = 0.0
        try:
            wind_radius = int(wind_str) if wind_str else 0
        except:
            wind_radius = 0

        typhoon_data.append({
            "forecast_time": formatted_forecast_time,
            "typ_name": name or "",
            "typ_dir": direction or "",
            "typ_lat": lat,
            "typ_lon": lon,
            "typ_location": loc or "",
            "intensity": intensity or "",
            "wind_radius": wind_radius
        })

        # 마지막 예보 시각 갱신
        last_forecast_time = forecast_time

    return typhoon_data

def get_typhoon_data():
    """
    fetch_typhoon_data()로부터 태풍 정보를 가져와
    domestic_typhoon 테이블에 저장한다.
    """
    logging.info("태풍 정보 수집 시작")
    data = fetch_typhoon_data()
    if not data:
        logging.info("새로운 태풍 정보가 없습니다.")
        return

    from cassandra.query import SimpleStatement
    saved_count = 0
    for item in data:
        # 중복 방지를 위해 uuid5를 사용 (태풍명+시간+위도+경도 등)
        unique_str = f"{item['forecast_time'].strftime('%Y%m%d%H%M')}_{item['typ_name']}_{item['typ_lat']}_{item['typ_lon']}"
        typ_no = uuid5(NAMESPACE_DNS, unique_str)

        insert_query = """
        INSERT INTO domestic_typhoon (
            typ_no,
            forecast_time,
            typ_name,
            typ_dir,
            typ_lat,
            typ_lon,
            typ_location,
            intensity,
            wind_radius
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        IF NOT EXISTS
        """
        try:
            connector.session.execute(SimpleStatement(insert_query), (
                typ_no,
                item['forecast_time'],
                item['typ_name'],
                item['typ_dir'],
                item['typ_lat'],
                item['typ_lon'],
                item['typ_location'],
                item['intensity'],
                item['wind_radius']
            ))
            saved_count += 1

            # 통합 데이터 저장 (태풍: 31)
            rtd_details = [
                f"typ_name: {item['typ_name']}",
                f"typ_dir: {item['typ_dir']}",
                f"intensity: {item['intensity']}",
                f"wind_radius: {item['wind_radius']}"
            ]
            insert_rtd_data(31, item['forecast_time'], item['typ_location'], rtd_details)
        except Exception as e:
            logging.error(f"태풍 정보 저장 실패: {e}")

    logging.info(f"태풍 정보 저장 완료: 총 {len(data)}건 중 {saved_count}건 저장됨")

# --------------------------------------------------------------------------------
# 5. 재난문자 크롤러 (재난소식 - 문자: 21)
# --------------------------------------------------------------------------------
class DisasterMessageCrawler:
    def __init__(self):
        chrome_driver_path = '/usr/local/bin/chromedriver'
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(service=Service(chrome_driver_path), options=chrome_options)
        self.driver.set_page_load_timeout(30)
        self.wait = WebDriverWait(self.driver, 20)
        self.session = connector.session
        self.seen_ids = set()

    def message_exists(self, msg_id):
        from cassandra.query import SimpleStatement
        result = self.session.execute(
            SimpleStatement("SELECT message_id FROM disaster_message WHERE message_id = %s"),
            (msg_id,)
        )
        return result.one() is not None

    def backup_messages(self, messages):
        from cassandra.query import SimpleStatement
        for msg in messages:
            try:
                self.session.execute(SimpleStatement("""
                    INSERT INTO disaster_message (
                        message_id,
                        emergency_level,
                        DM_ntype,
                        DM_stype,
                        issuing_agency,
                        issued_at,
                        message_content
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s) IF NOT EXISTS
                """), (
                    msg['message_id'],
                    msg['emergency_level'],
                    msg['DM_ntype'],
                    msg['DM_stype'],
                    msg['issuing_agency'],
                    msg['issued_at'],
                    msg['message_content']
                ))
                logging.info(f"메시지 저장됨: {msg['message_id']}")

                # 통합 데이터 저장 (재난소식 - 문자: 21)
                rtd_details = [
                    f"emergency_level: {msg['emergency_level']}",
                    f"DM_ntype: {msg['DM_ntype']}",
                    f"issuing_agency: {msg['issuing_agency']}",
                    f"content: {msg['message_content']}"
                ]
                insert_rtd_data(21, msg['issued_at'], msg['issuing_agency'], rtd_details)
            except Exception as e:
                logging.error(f"메시지 저장 오류 ({msg['message_id']}): {e}")

    def show_status(self):
        from cassandra.query import SimpleStatement
        print("=== 저장 현황 ===")
        for table in [
            "airinform",
            "airgrade",
            "domestic_earthquake",
            "domestic_typhoon",
            "disaster_message",
            "rtd_db"
        ]:
            result = connector.session.execute(SimpleStatement(f"SELECT count(*) FROM {table};"))
            for row in result:
                print(f"{table}: {row.count}건")
        print("=================")

    def process_command(self, cmd):
        if cmd in ["q", "exit"]:
            logging.info("모니터링 종료")
            return True
        elif cmd == "1":
            self.show_status()
        elif cmd == "2":
            logging.info("대기 예보 수집 시작")
            get_air_inform()
            logging.info("대기 예보 수집 완료")
        elif cmd == "3":
            logging.info("실시간 미세먼지 수집 시작")
            get_air_grade()
            logging.info("미세먼지 수집 완료")
        elif cmd == "4":
            logging.info("지진 정보 수집 시작")
            fetch_earthquake_data()
            logging.info("지진 정보 수집 완료")
        elif cmd == "5":
            logging.info("전체 수집 시작")
            get_air_inform()
            get_air_grade()
            fetch_earthquake_data()
            get_typhoon_data()
            logging.info("전체 수집 완료")
        elif cmd == "6":
            logging.info("태풍 정보 수집 시작")
            get_typhoon_data()
            logging.info("태풍 정보 수집 완료")
        elif cmd == "7":
            logging.info("홍수 정보 수집 시작")
            get_flood_data()
            logging.info("홍수 정보 수집 완료")
        elif cmd == "8":
            logging.info("주의보 정보 수집 시작")
            get_warning_data()
            logging.info("주의보 정보 수집 완료")
        elif cmd == "?":
            self.display_help()
        else:
            print("알 수 없는 명령입니다. 다시 입력해주세요.")
        return False

    def display_help(self):
        print("명령어 안내:")
        print(" 1 → 저장 현황 보기")
        print(" 2 → 대기 예보 정보 수집")
        print(" 3 → 실시간 미세먼지 수집")
        print(" 4 → 지진 정보 수집")
        print(" 5 → 전체 수집 (대기 예보 + 미세먼지 + 지진 + 태풍)")
        print(" 6 → 태풍 정보 수집")
        print(" 7 → 홍수 정보 수집")
        print(" 8 → 기상특보(주의보/경보) 정보 수집")  # 추가
        print(" ? → 명령어 도움말")
        print(" q 또는 exit → 종료")

    def monitor(self):
        logging.info("실시간 재난문자 수집 시작")
        self.display_help()
        last_check_time = time.time()
        while True:
            try:
                # 비동기적으로 명령어 입력 확인
                if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                    cmd = input().strip().lower()
                    if self.process_command(cmd):
                        break

                # 60초마다 신규 메시지 체크
                if time.time() - last_check_time > 60:
                    messages = self.check_messages()
                    if messages:
                        logging.info("신규 메시지 발견")
                        print(json.dumps(messages, ensure_ascii=False, indent=2, default=str))
                        self.backup_messages(messages)
                    else:
                        logging.info("신규 메시지 없음")
                        print("60초 대기 중... (명령어 입력 가능: 1~8, q 등)")
                    last_check_time = time.time()
                time.sleep(1)
            except Exception as e:
                logging.error(f"오류 발생: {e}")
                time.sleep(60)

    def check_messages(self):
        self.driver.get('https://www.safekorea.go.kr/idsiSFK/neo/sfk/cs/sfc/dis/disasterMsgList.jsp?menuSeq=603')
        time.sleep(5)
        messages = []
        for i in range(10):
            try:
                msg_id = int(self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MD101_SN").text.strip())
                if msg_id in self.seen_ids or self.message_exists(msg_id):
                    continue
                self.seen_ids.add(msg_id)
                messages.append({
                    "message_id": msg_id,
                    "emergency_level": self.driver.find_element(By.ID, f"disasterSms_tr_{i}_EMRGNCY_STEP_NM").text,
                    "DM_ntype": self.driver.find_element(By.ID, f"disasterSms_tr_{i}_DSSTR_SE_NM").text,
                    "DM_stype": "",
                    "issuing_agency": self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MSG_LOC").text,
                    "issued_at": datetime.strptime(
                        self.driver.find_element(By.ID, f"disasterSms_tr_{i}_CREATE_DT").text,
                        "%Y/%m/%d %H:%M:%S"
                    ),
                    "message_content": self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MSG_CN")
                                                 .get_attribute("title")
                })
            except Exception as e:
                logging.error(f"메시지 추출 오류 (인덱스 {i}): {e}")
                continue
        return messages

# --------------------------------------------------------------------------------
# 6. 홍수 데이터 (rtd_code 33)
# --------------------------------------------------------------------------------
FLOOD_CODE = 33
FLOOD_URL = (
    "https://www.water.or.kr/kor/flood/floodwarning/index.do?mode=list&types=1&menuId=16_166_170_172"
)

def fetch_flood_data():
    """홍수 데이터를 크롤링하여 수집 (BeautifulSoup)"""
    response = requests.get(FLOOD_URL)
    if response.status_code != 200:
        logging.error(f"홍수 데이터 페이지 응답 오류: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', class_='basic_table')
    if not table or not table.find('tbody'):
        logging.info("홍수 테이블 또는 tbody를 찾을 수 없습니다.")
        return []

    rows = table.find('tbody').find_all('tr')
    flood_data = []
    for row in rows:
        cells = row.find_all('td')
        if cells and len(cells) >= 7:
            region = cells[0].text.strip()            # 지역
            current_level = cells[1].text.strip()     # 현재 수위
            advisory_level = cells[2].text.strip()    # 주의보 수위
            warning_level = cells[3].text.strip()     # 경보 수위
            flow_rate = cells[4].text.strip()         # 유량
            alert_status = cells[5].text.strip()      # 예경보 현황
            issued_time = cells[6].text.strip()       # 발령 일시 (yyyy-MM-dd HH:mm)

            # 시간 변환 (KST → UTC)
            korea_timezone = timezone(timedelta(hours=9))
            try:
                formatted_issued_time = datetime.strptime(
                    issued_time, "%Y-%m-%d %H:%M"
                ).replace(tzinfo=korea_timezone).astimezone(timezone.utc)
            except Exception:
                formatted_issued_time = datetime.now(timezone.utc)

            flood_data.append({
                "rtd_code": FLOOD_CODE,
                "rtd_time": formatted_issued_time,
                "rtd_loc": region,
                "rtd_details": [
                    f"현재 수위: {current_level}m",
                    f"주의보 수위: {advisory_level}m",
                    f"경보 수위: {warning_level}m",
                    f"유량: {flow_rate}㎥/s",
                    f"예경보 현황: {alert_status}"
                ]
            })
    return flood_data

def get_flood_data():
    """
    fetch_flood_data()로부터 홍수 정보를 가져와
    RealTimeFlood 테이블에 저장한 뒤 rtd_db에도 기록한다.
    """
    logging.info("홍수 정보 수집 함수 get_flood_data() 실행")
    data = fetch_flood_data()
    if not data:
        logging.info("새로운 홍수 정보가 없습니다.")
        return

    from cassandra.query import SimpleStatement
    saved_count = 0
    for item in data:
        try:
            comment_str = "; ".join(item["rtd_details"])
            unique_str = f"{item['rtd_time'].strftime('%Y%m%d%H%M')}_{item['rtd_loc']}_{comment_str}"
            fld_no = uuid5(NAMESPACE_DNS, unique_str)

            insert_flood = """
            INSERT INTO RealTimeFlood (fld_no, fld_region, fld_alert, fld_time, comment)
            VALUES (%s, %s, %s, %s, %s)
            IF NOT EXISTS
            """
            alert_status_str = item["rtd_details"][-1].replace("예경보 현황: ", "")
            connector.session.execute(
                SimpleStatement(insert_flood),
                (fld_no, item["rtd_loc"], alert_status_str, item["rtd_time"], comment_str)
            )
            saved_count += 1

            # 통합 데이터 테이블(rtd_db)에 저장
            insert_rtd_data(
                item["rtd_code"],
                item["rtd_time"],
                item["rtd_loc"],
                item["rtd_details"]
            )
        except Exception as e:
            logging.error(f"홍수 정보 저장 실패: {e}")

    logging.info(f"홍수 정보 저장 완료: 총 {len(data)}건 중 {saved_count}건 저장됨")

# --------------------------------------------------------------------------------
# 7. 기상특보(주의보/경보) 수집 (rtd_code는 WARNING_CODES 사용)
# --------------------------------------------------------------------------------
def fetch_warning_data():
    """전국의 주의보 데이터를 기상청 API에서 수집"""
    current_date = datetime.now().strftime('%Y%m%d')
    all_warnings = []  # 모든 지역의 데이터를 저장할 리스트

    for stn_id, region in STATION_CODES.items():
        url = 'http://apis.data.go.kr/1360000/WthrWrnInfoService/getWthrWrnList'
        params = {
            'serviceKey': 'D0I8CLciGzwIaBmM6g6XitlVfgkLBO83zDl4EnUUoxifvRlSZHu78BqoixtzJg17Gb06up+NHzPXjN0cA7sLOg==',
            'pageNo': '1',
            'numOfRows': '10',
            'dataType': 'XML',
            'stnId': str(stn_id),
            'fromTmFc': current_date,
            'toTmFc': current_date
        }

        response = requests.get(url, params=params)
        root = ET.fromstring(response.content)

        # 데이터 유효성 확인
        result_code = root.find('.//resultCode')
        if result_code is not None and result_code.text == '03':
            # '03' = 해당 지역에 특보 데이터 없음
            continue

        # XML에서 title 태그의 내용만 추출
        titles = [item.find('title').text for item in root.findall('.//item') if item.find('title') is not None]

        warnings = preprocess_alert_data(titles, region)
        all_warnings.extend(warnings)

    return all_warnings

def preprocess_alert_data(titles, region):
    """특보 데이터를 정제하여 파싱"""
    processed_data = []

    for title in titles:
        # "[특보]" 삭제
        title = re.sub(r'\[특보\]\s*', '', title)

        # '제XX호 :' 패턴 제거 (예: '제2023-10호 :' 등)
        title = re.sub(r'제\d+-\d+호\s*:\s*', '', title)

        # 날짜와 특보 상태로 나누기 (예: '2024.09.02.17:00 / 호우주의보 발표')
        parts = title.split(' / ')
        if len(parts) != 2:
            continue

        date_str, alert_info = parts

        # 날짜 형식 변환 (예: '2024.09.02.17:00' → '2024-09-02 17:00')
        date_str = re.sub(r'(\d{4})\.(\d{2})\.(\d{2})\.(\d{2}):(\d{2})', r'\1-\2-\3 \4:\5', date_str)

        # 형태소 분석으로 alert_info 분리
        words = okt.morphs(alert_info)

        # 주의보/경보 종류 추출 및 상태 추출
        alert_types = []
        alert_status = ''

        for i in range(len(words)):
            if '주의보' in words[i] or '경보' in words[i]:
                # 앞 단어가 '호우', '강풍', '대설', '폭염', '한파' 등을 의미할 수 있음
                if i - 1 >= 0:
                    alert_type = words[i-1]
                    if alert_type in WARNING_CODES and alert_type not in alert_types:
                        alert_types.append(alert_type)
            elif len(words[i]) == 2:
                # 두 글자 상태 (예: 발효, 해제 등)
                alert_status = words[i]

        # KST -> UTC 변환
        korea_timezone = timezone(timedelta(hours=9))
        try:
            formatted_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M") \
                                     .replace(tzinfo=korea_timezone).astimezone(timezone.utc)
        except:
            # 형식이 다를 경우 현재 시간으로 대체
            formatted_date = datetime.now(timezone.utc)

        # 재난 코드가 존재하는 경우에만 저장
        for alert in alert_types:
            processed_data.append({
                "rtd_code": WARNING_CODES[alert],  # ex) 호우=32, 강풍=34, ...
                "rtd_time": formatted_date,
                "rtd_loc": region,
                "rtd_details": [f"{alert} {alert_status}"]  # 예: '호우 발효'
            })

    return processed_data

def get_warning_data():
    """
    fetch_warning_data()로 수집한 기상특보(주의보/경보) 정보를
    ForecastAnnouncement 테이블과 rtd_db 테이블에 저장한다.
    """
    logging.info("주의보 정보 수집 함수 get_warning_data() 실행")
    data = fetch_warning_data()
    if not data:
        logging.info("새로운 주의보 정보가 없습니다.")
        return

    from cassandra.query import SimpleStatement
    saved_count = 0
    for item in data:
        rtd_code = item['rtd_code']
        rtd_time = item['rtd_time']
        rtd_loc = item['rtd_loc']
        rtd_details = item['rtd_details']  # 예: ['호우 발효']

        # rtd_details[0]에서 alert_type, alert_stat 분리 (예: "호우 발효")
        if len(rtd_details) > 0:
            splitted = rtd_details[0].split()
            if len(splitted) == 2:
                alert_type, alert_stat = splitted
            else:
                alert_type = splitted[0]
                alert_stat = "정보없음"
        else:
            alert_type = "정보없음"
            alert_stat = "정보없음"

        # ForecastAnnouncement에 저장
        # announce_no (UUID, PK), disaster_region, alert_type, alert_stat, announce_time, comment
        unique_str = f"{rtd_loc}_{alert_type}_{alert_stat}_{rtd_time.strftime('%Y%m%d%H%M')}"
        announce_no = uuid5(NAMESPACE_DNS, unique_str)
        insert_query = """
        INSERT INTO ForecastAnnouncement (
            announce_no,
            disaster_region,
            alert_type,
            alert_stat,
            announce_time,
            comment
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        IF NOT EXISTS
        """
        comment = f"기상특보 자동 수집 / {alert_type} {alert_stat}"
        try:
            connector.session.execute(SimpleStatement(insert_query), (
                announce_no,
                rtd_loc,
                alert_type,
                alert_stat,
                rtd_time,
                comment
            ))
            saved_count += 1

            # 2) rtd_db 테이블에 저장
            insert_rtd_data(rtd_code, rtd_time, rtd_loc, rtd_details)

        except Exception as e:
            logging.error(f"주의보 정보 저장 실패 ({announce_no}): {e}")

    logging.info(f"주의보 정보 저장 완료: 총 {len(data)}건 중 {saved_count}건 저장됨")

# --------------------------------------------------------------------------------
# 메인 함수
# --------------------------------------------------------------------------------
def main():
    logging.info("데이터 수집 시작")

    # 1. 대기 예보
    get_air_inform()
    # 2. 실시간 미세먼지
    get_air_grade()
    # 3. 지진 정보
    fetch_earthquake_data()
    # 4. 태풍 정보
    get_typhoon_data()
    # 5. 홍수 정보
    logging.info("홍수 정보 수집 시작")
    get_flood_data()
    logging.info("홍수 정보 수집 완료")
    # 6. 기상특보(주의보/경보) 정보
    logging.info("기상특보(주의보/경보) 정보 수집 시작")
    get_warning_data()
    logging.info("기상특보 정보 수집 완료")

    # 재난문자 모니터링 시작
    logging.info("재난문자 수집 시작")
    DisasterMessageCrawler().monitor()

if __name__ == "__main__":
    main()

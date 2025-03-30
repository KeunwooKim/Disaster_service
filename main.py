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
from io import StringIO
from uuid import uuid4, uuid5, NAMESPACE_DNS
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

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
                auth_provider = PlainTextAuthProvider(username="andy013", password="1212")
                self.cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth_provider)
                self.session = self.cluster.connect(self.keyspace)
                logging.info("✅ Cassandra 연결 완료.")
                return
            except Exception as e:
                logging.error(f"❌ 연결 실패: {e}")
                time.sleep(10)
        raise Exception("Cassandra 연결 실패")

connector = CassandraConnector()

# 통합 데이터 저장 함수
def insert_rtd_data(rtd_code: int, rtd_time: datetime, rtd_loc: str, rtd_details: list):
    """
    통합 데이터 테이블(rtd_db)에 데이터를 저장하는 함수입니다.
    각 재난별 코드(rtd_code)는 아래와 같이 매핑됩니다:
      - 사용자 제보: 11
      - 재난소식 - 문자: 21
      - 재난소식 - 뉴스: 22
      - 태풍: 31
      - 호우: 32
      - 홍수: 33
      - 강풍: 34
      - 대설: 35
      - 폭염: 41
      - 한파: 42
      - 지진: 51
      - 화재 - 산불: 61
      - 화재 - 일일화재: 62
      - 미세먼지 시도별 측정정보: 71
      - 대기질 예보: 72
    """
    record_id = uuid4()
    query = """
    INSERT INTO rtd_db (rtd_code, rtd_time, id, rtd_loc, rtd_details)
    VALUES (%s, %s, %s, %s, %s)
    """
    try:
        connector.session.execute(SimpleStatement(query), (rtd_code, rtd_time, record_id, rtd_loc, rtd_details))
        logging.info(f"통합 데이터 저장 성공: {record_id} (코드: {rtd_code})")
    except Exception as e:
        logging.error(f"통합 데이터 저장 실패 (코드: {rtd_code}): {e}")

# 1. 대기질 예보 데이터 수집 및 저장 (rtd_code 72)
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
        response = requests.get("http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustFrcstDspth", params=params, timeout=10)
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
            connector.session.execute(SimpleStatement(query), (
                record_id,
                item.get("informCause", ""),
                item.get("informCode", ""),
                data_time,
                forecast_date,
                item.get("informGrade", ""),
                item.get("informOverall", ""),
                datetime.now().date()
            ))
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

# 2. 실시간 대기질 등급 수집 및 저장 (rtd_code 71)
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
        response = requests.get("http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty", params=params, timeout=10)
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
    for item in items:
        if item.get("pm10Grade1h") is None and item.get("pm25Grade1h") is None:
            continue

        try:
            dt = datetime.strptime(item["dataTime"], "%Y-%m-%d %H:%M").replace(tzinfo=korea_tz).astimezone(timezone.utc)
        except Exception:
            dt = datetime.utcnow()

        station = item.get("stationName")
        check_query = "SELECT pm_no FROM airgrade WHERE stationname=%s ALLOW FILTERING"
        result = connector.session.execute(SimpleStatement(check_query), (station,))
        row = result.one()
        pm10_grade = int(item.get("pm10Grade1h") or 0)
        pm25_grade = int(item.get("pm25Grade1h") or 0)
        if row:
            update = "UPDATE airgrade SET data_time=%s, pm10_grade=%s, pm25_grade=%s, sido=%s WHERE pm_no=%s"
            try:
                connector.session.execute(SimpleStatement(update), (
                    dt,
                    pm10_grade,
                    pm25_grade,
                    item.get("sidoName", ""),
                    row.pm_no
                ))
                saved_count += 1
            except Exception as e:
                logging.error(f"업데이트 실패 ({station}): {e}")
        else:
            insert = "INSERT INTO airgrade (pm_no, data_time, pm10_grade, pm25_grade, sido, stationname) VALUES (%s, %s, %s, %s, %s, %s)"
            try:
                connector.session.execute(SimpleStatement(insert), (
                    uuid4(), dt,
                    pm10_grade,
                    pm25_grade,
                    item.get("sidoName", ""),
                    station
                ))
                saved_count += 1
            except Exception as e:
                logging.error(f"삽입 실패 ({station}): {e}")

        # 통합 데이터 저장 (미세먼지 시도별 측정정보: 71)
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

# 3. 지진 정보 수집 및 저장 (rtd_code 51)
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

    try:
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
        if tp != "3":
            continue

        try:
            tm_eqk = tokens[3]
            dt = datetime.strptime(tm_eqk[:14], "%Y%m%d%H%M%S").replace(tzinfo=kst).astimezone(timezone.utc)
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
            connector.session.execute(SimpleStatement(insert_stmt), (record_id, dt, lat_num, lon_num, magnitude, msg))
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

# ----------------------------------------------------
# 4. 태풍 데이터 (rtd_code 31)
# ----------------------------------------------------
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

        # 태풍 정보 추출 (필드명은 실제 API 응답에 맞춰 조정하세요)
        name = item.findtext('typName')         # 태풍 이름
        direction = item.findtext('typDir')       # 진행 방향
        lat_str = item.findtext('typLat')         # 위도
        lon_str = item.findtext('typLon')         # 경도
        loc = item.findtext('typLoc')             # 위치
        intensity = item.findtext('typInt')       # 강도 (예: "중", "강", "매우강")
        wind_str = item.findtext('typ15')         # 강풍 반경(15m/s)

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

# 5. 재난문자 크롤러 (재난소식 - 문자: 21)
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
        result = self.session.execute("SELECT message_id FROM disaster_message WHERE message_id = %s", (msg_id,))
        return result.one() is not None

    def backup_messages(self, messages):
        for msg in messages:
            try:
                self.session.execute(SimpleStatement("""
                    INSERT INTO disaster_message (message_id, emergency_level, DM_ntype, DM_stype,
                    issuing_agency, issued_at, message_content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) IF NOT EXISTS
                """), (
                    msg['message_id'], msg['emergency_level'], msg['DM_ntype'], msg['DM_stype'],
                    msg['issuing_agency'], msg['issued_at'], msg['message_content']
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
        print("=== 저장 현황 ===")
        for table in ["airinform", "airgrade", "domestic_earthquake", "domestic_typhoon", "disaster_message", "rtd_db"]:
            result = connector.session.execute(f"SELECT count(*) FROM {table};")
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
            # ★ 태풍 정보만 단독으로 수집하는 명령어 추가
            logging.info("태풍 정보 수집 시작")
            get_typhoon_data()
            logging.info("태풍 정보 수집 완료")
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
        print(" 6 → 태풍 정보 수집")  # ★ 도움말에 태풍 명령 추가
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
                        print("60초 대기 중... (명령어 입력 가능: 1~6, q 등)")
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
                    "message_content": self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MSG_CN").get_attribute("title")
                })
            except Exception as e:
                logging.error(f"메시지 추출 오류 (인덱스 {i}): {e}")
                continue
        return messages

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

    logging.info("재난문자 수집 시작")
    DisasterMessageCrawler().monitor()

if __name__ == "__main__":
    main()

import os
import sys
import time
import json
import csv
import logging
import select
import requests
import xmltodict
from io import StringIO
from uuid import uuid4
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
API_KEY = os.getenv("API_KEY", "기본값")
EQ_API_KEY = os.getenv("EQ_API_KEY", "F5Iz7aHpRUSSM-2h6ZVE2w")

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
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

# 1. 대기질 예보 데이터 수집 및 저장
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
        logging.debug("Air Inform API 호출 성공")
    except Exception as e:
        logging.error(f"❌ Air Inform API 호출 실패: {e}")
        return {"status": "error", "data": []}

    data_dict = xmltodict.parse(response.text)
    items = data_dict.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    if not isinstance(items, list):
        items = [items]
    logging.info(f"총 {len(items)}개의 레코드를 처리합니다.")

    result_data = []
    for idx, item in enumerate(items):
        record_id = f"{item.get('informData')}_{item.get('dataTime')}_{item.get('informCode')}"
        try:
            data_time = datetime.strptime(item["dataTime"].replace("시 발표", "").strip(), "%Y-%m-%d %H")
            forecast_date = datetime.strptime(item["informData"], "%Y-%m-%d").date()
        except Exception as ex:
            logging.warning(f"날짜 파싱 오류 (record_id: {record_id}): {ex}")
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
            logging.debug(f"[{idx+1}/{len(items)}] 저장 완료 - {record_id}")
        except Exception as e:
            logging.error(f"DB 저장 실패 (record_id: {record_id}): {e}")
        result_data.append(item)
    logging.info("대기질 예보 데이터 수집 완료")
    return {"status": "success", "data": result_data}

# 2. 실시간 대기질 등급 수집 및 저장
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
        logging.debug("Air Grade API 호출 성공")
    except Exception as e:
        logging.error(f"❌ Air Grade API 실패: {e}")
        return {"status": "error", "data": []}

    data_dict = xmltodict.parse(response.text)
    items = data_dict.get("response", {}).get("body", {}).get("items", {}).get("item", [])
    if isinstance(items, dict):
        items = [items]

    korea_tz = timezone(timedelta(hours=9))
    for item in items:
        if item.get("pm10Grade1h") is None and item.get("pm25Grade1h") is None:
            continue

        try:
            dt = datetime.strptime(item["dataTime"], "%Y-%m-%d %H:%M").replace(tzinfo=korea_tz).astimezone(timezone.utc)
        except Exception as ex:
            logging.warning(f"시간 파싱 오류: {ex}")
            dt = datetime.utcnow()

        station = item.get("stationName")
        check_query = "SELECT pm_no FROM airgrade WHERE stationname=%s ALLOW FILTERING"
        result = connector.session.execute(SimpleStatement(check_query), (station,))
        row = result.one()
        if row:
            update = "UPDATE airgrade SET data_time=%s, pm10_grade=%s, pm25_grade=%s, sido=%s WHERE pm_no=%s"
            try:
                connector.session.execute(SimpleStatement(update), (
                    dt,
                    int(item.get("pm10Grade1h", 0)),
                    int(item.get("pm25Grade1h", 0)),
                    item.get("sidoName", ""),
                    row.pm_no
                ))
                logging.debug(f"🔁 업데이트됨: {station}")
            except Exception as e:
                logging.error(f"업데이트 실패 ({station}): {e}")
        else:
            insert = "INSERT INTO airgrade (pm_no, data_time, pm10_grade, pm25_grade, sido, stationname) VALUES (%s, %s, %s, %s, %s, %s)"
            try:
                connector.session.execute(SimpleStatement(insert), (
                    uuid4(), dt,
                    int(item.get("pm10Grade1h", 0)),
                    int(item.get("pm25Grade1h", 0)),
                    item.get("sidoName", ""),
                    station
                ))
                logging.debug(f"✅ 삽입됨: {station}")
            except Exception as e:
                logging.error(f"삽입 실패 ({station}): {e}")
    logging.info("실시간 대기질 등급 데이터 수집 완료")
    return {"status": "success", "data": items}

# 3. 지진 정보 수집 및 저장 (시간대 처리 수정)
def fetch_earthquake_data():
    logging.info("지진 정보 수집 시작")
    # airgrade와 유사하게 한국 시간 기준 현재 시각 사용 (초까지 포함)
    korea_time = datetime.now(timezone(timedelta(hours=9)))
    current_time = korea_time.strftime('%Y%m%d%H%M%S')
    url = f"https://apihub.kma.go.kr/api/typ01/url/eqk_now.php?tm={current_time}&disp=1&help=0&authKey={EQ_API_KEY}"
    logging.debug(f"지진 API 호출 URL: {url}")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        response.encoding = 'euc-kr'
        csv_data = csv.reader(StringIO(response.text))
        logging.debug("지진 API 호출 성공")
    except Exception as e:
        logging.error(f"❌ 지진 API 오류: {e}")
        return

    korea_tz = timezone(timedelta(hours=9))
    row_count = 0
    for row in csv_data:
        if not row or row[0] == "TP":
            continue
        row_count += 1
        try:
            if row[0] != "3":
                continue
            # row[3]의 앞 14자리를 이용하여 시간 변환 (YYYYMMDDHHMMSS)
            dt = datetime.strptime(row[3][:14], "%Y%m%d%H%M%S").replace(tzinfo=korea_tz).astimezone(timezone.utc)
            magnitude = float(row[5])
            lat = float(row[6])
            lon = float(row[7])
            msg = f"[{row[8]}] 규모 {magnitude}, 진도: {row[9]}, 참고: {row[10]}"
            insert_stmt = """
                INSERT INTO domestic_earthquake (eq_no, eq_time, eq_lat, eq_lot, eq_mag, eq_msg)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            connector.session.execute(SimpleStatement(insert_stmt), (uuid4(), dt, lat, lon, magnitude, msg))
            logging.debug(f"✅ 지진 저장: {dt} / {row[8]}")
        except Exception as e:
            logging.error(f"⚠️ 지진 파싱 오류 (row: {row}): {e}")
    logging.info(f"지진 정보 수집 완료. 처리한 행 수: {row_count}")

# 4. 재난문자 크롤러
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
                logging.debug(f"✅ 메시지 저장: {msg['message_id']}")
            except Exception as e:
                logging.error(f"메시지 저장 오류 ({msg['message_id']}): {e}")

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
                        self.driver.find_element(By.ID, f"disasterSms_tr_{i}_CREATE_DT").text, "%Y-%m-%d %H:%M"
                    ),
                    "message_content": self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MSG_CN").get_attribute("title")
                })
            except Exception as e:
                logging.debug(f"메시지 크롤링 중 오류 (index: {i}): {e}")
                continue
        return messages

    def monitor(self):
        logging.info("[실시간 재난문자 수집 시작]")
        print("명령어 안내:")
        print(" 1 → 저장 현황 보기")
        print(" 2 → 대기 예보 정보 수집")
        print(" 3 → 실시간 미세먼지 수집")
        print(" 4 → 지진 정보 수집")
        print(" 5 → 전체 수집 (대기 예보 + 미세먼지 + 지진)")
        print(" q 또는 exit → 종료")

        while True:
            try:
                if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                    cmd = input().strip().lower()
                    if cmd in ["q", "exit"]:
                        logging.info("[모니터링 종료]")
                        break
                    elif cmd == "1":
                        print("=== 저장 현황 ===")
                        for table in ["airinform", "airgrade", "domestic_earthquake", "disaster_message"]:
                            result = connector.session.execute(f"SELECT count(*) FROM {table};")
                            for row in result:
                                print(f"{table}: {row.count}건")
                        print("=================")
                    elif cmd == "2":
                        logging.info("[대기 예보 정보 수집 중...]")
                        get_air_inform()
                        logging.info("[대기 예보 수집 완료]")
                    elif cmd == "3":
                        logging.info("[실시간 미세먼지 수집 중...]")
                        get_air_grade()
                        logging.info("[미세먼지 수집 완료]")
                    elif cmd == "4":
                        logging.info("[지진 정보 수집 중...]")
                        fetch_earthquake_data()
                        logging.info("[지진 정보 수집 완료]")
                    elif cmd == "5":
                        logging.info("[전체 수집 중...]")
                        get_air_inform()
                        get_air_grade()
                        fetch_earthquake_data()
                        logging.info("[전체 수집 완료]")
                    else:
                        print("[알 수 없는 명령입니다. 다시 입력해주세요.]")

                messages = self.check_messages()
                if messages:
                    logging.info("[신규 메시지 발견]")
                    print(json.dumps(messages, ensure_ascii=False, indent=2, default=str))
                    self.backup_messages(messages)
                else:
                    logging.info("[신규 메시지 없음]")
                    print("[60초 대기 중... (명령어 입력 가능: 1~5, q 등)]")
                    for i in range(60):
                        if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
                            cmd = input().strip().lower()
                            if cmd in ["q", "exit"]:
                                logging.info("[모니터링 종료]")
                                return
                            elif cmd == "1":
                                print("=== 저장 현황 ===")
                                for table in ["airinform", "airgrade", "domestic_earthquake", "disaster_message"]:
                                    result = connector.session.execute(f"SELECT count(*) FROM {table};")
                                    for row in result:
                                        print(f"{table}: {row.count}건")
                                print("=================")
                            elif cmd == "2":
                                logging.info("[대기 예보 수집 중...]")
                                get_air_inform()
                                logging.info("[대기 예보 수집 완료]")
                            elif cmd == "3":
                                logging.info("[미세먼지 수집 중...]")
                                get_air_grade()
                                logging.info("[미세먼지 수집 완료]")
                            elif cmd == "4":
                                logging.info("[지진 수집 중...]")
                                fetch_earthquake_data()
                                logging.info("[지진 수집 완료]")
                            elif cmd == "5":
                                logging.info("[전체 수집 중...]")
                                get_air_inform()
                                get_air_grade()
                                fetch_earthquake_data()
                                logging.info("[전체 수집 완료]")
                            else:
                                print("[알 수 없는 명령입니다.]")
            except Exception as e:
                logging.error(f"[오류 발생]: {e}")
                time.sleep(60)

# 실행 메인
def main():
    logging.info("📦 데이터 수집 시작")
    get_air_inform()
    get_air_grade()
    fetch_earthquake_data()
    logging.info("\n🛑 재난문자 수집 시작")
    DisasterMessageCrawler().monitor()

if __name__ == "__main__":
    main()

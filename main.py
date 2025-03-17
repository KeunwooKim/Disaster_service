import os
import sys
import time
import json
import logging
import select
import requests
import xmltodict
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

# 환경 변수 로드 및 API_KEY 설정
load_dotenv()
API_KEY = os.getenv(
    "API_KEY",
    "7dWUeNJAqaan8oJAs5CbDWKnWaJpLWoxd+lB97UDDRgFfSjfKD7ZGHxM+kRAoZqsga+WlheugBMS2q9WCSaUNg=="
)


# Cassandra 연결을 관리하는 클래스 (keyspace: disaster_service)
class CassandraConnector:
    def __init__(self, keyspace="disaster_service"):
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.setup_cassandra_connection()

    def setup_cassandra_connection(self):
        max_retries = 5
        retry_interval = 10
        attempt = 0
        connected = False
        while attempt < max_retries and not connected:
            try:
                print(f"Cassandra 연결 시도 중... (시도 {attempt + 1}/{max_retries})")
                auth_provider = PlainTextAuthProvider(username="andy013", password="1212")
                self.cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth_provider)
                self.session = self.cluster.connect(self.keyspace)
                print("Cassandra 연결 완료.")
                connected = True
            except Exception as e:
                print(f"Cassandra 연결 설정 중 오류 발생: {e}")
                attempt += 1
                if attempt < max_retries:
                    print(f"{retry_interval}초 후에 재시도합니다...")
                    time.sleep(retry_interval)
                else:
                    raise Exception("Cassandra 연결에 실패하였습니다. 종료합니다.")


# 전역 CassandraConnector 객체 생성
connector = CassandraConnector()

# Air API 엔드포인트
AIR_INFORM_API = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustFrcstDspth"
AIR_GRADE_API = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"


# (1) 대기질 예보통보 (airinform) API 호출 및 중복 없이 Cassandra 저장
def get_air_inform():
    params = {
        "searchDate": datetime.now().strftime("%Y-%m-%d"),
        "returnType": "xml",
        "numOfRows": "100",
        "pageNo": "1",
        "serviceKey": API_KEY
    }
    try:
        response = requests.get(AIR_INFORM_API, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Air Inform API 호출 실패: {e}")
        raise Exception("Air Inform API 호출 실패")

    data_dict = xmltodict.parse(response.text)
    body = data_dict.get("response", {}).get("body", {})
    total_count = int(body.get("totalCount", "0"))
    if total_count == 0:
        print("Air Inform API: 해당 날짜에 데이터가 없습니다.")
        return {"status": "success", "data": []}

    items_section = body.get("items")
    if items_section is None:
        raise Exception("API 응답에 'items' 섹션이 없습니다. 응답 내용: " + response.text)
    items = items_section.get("item")
    if items is None:
        raise Exception("API 응답에 'item' 데이터가 없습니다. 응답 내용: " + response.text)
    if not isinstance(items, list):
        items = [items]

    filtered_data = []
    for item in items:
        extracted = {
            "informCode": item.get("informCode"),
            "informCause": item.get("informCause"),
            "informOverall": item.get("informOverall"),
            "informData": item.get("informData"),
            "informGrade": item.get("informGrade"),
            "dataTime": item.get("dataTime")
        }
        filtered_data.append(extracted)

        # 자연키 생성: 예보일자, 발표시간, 예보코드를 결합하여 고유키 생성
        record_id = f"{extracted['informData']}_{extracted['dataTime']}_{extracted['informCode']}"
        try:
            dt_str = extracted["dataTime"].replace("시 발표", "").strip()
            data_time = datetime.strptime(dt_str, "%Y-%m-%d %H")
        except Exception as e:
            logging.error(f"날짜 변환 실패 (airinform data_time): {e}")
            data_time = datetime.now()
        try:
            forecast_date = datetime.strptime(extracted["informData"], "%Y-%m-%d").date()
        except Exception as e:
            logging.error(f"날짜 변환 실패 (airinform forecast_date): {e}")
            forecast_date = None

        search_date = datetime.now().date()
        cause = extracted["informCause"] if extracted["informCause"] is not None else ""
        code = extracted["informCode"] if extracted["informCode"] is not None else ""
        grade = extracted["informGrade"] if extracted["informGrade"] is not None else ""
        overall = extracted["informOverall"] if extracted["informOverall"] is not None else ""

        insert_stmt = SimpleStatement("""
            INSERT INTO airinform (record_id, cause, code, data_time, forecast_date, grade, overall, search_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) IF NOT EXISTS
        """)
        connector.session.execute(insert_stmt, (
            record_id,
            cause,
            code,
            data_time,
            forecast_date,
            grade,
            overall,
            search_date
        ))
        print(f"Air Inform 데이터 저장 완료 - record_id: {record_id}")

    return {"status": "success", "data": filtered_data}

# (2) 시도별 실시간 미세먼지 정보 (air_grade) API 호출 및 중복 없이 Cassandra 저장
# 중복 방지를 위해, 동일 지역(sido)과 발표시간(data_time)에 대해 먼저 조회 후, 없으면 INSERT
def get_air_grade_all_regions():
    regions = ["서울", "부산", "대구", "인천", "광주", "대전", "울산",
               "경기", "강원", "충북", "충남", "전북", "전남", "경북", "경남", "제주", "세종"]
    all_filtered_data = []
    for region in regions:
        params = {
            "sidoName": region,
            "returnType": "xml",
            "numOfRows": "100",
            "pageNo": "1",
            "serviceKey": API_KEY,
            "ver": "1.0"
        }
        try:
            response = requests.get(AIR_GRADE_API, params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Air Grade API 호출 실패 for {region}: {e}")
            continue

        data_dict = xmltodict.parse(response.text)
        items = data_dict.get("response", {}).get("body", {}).get("items", {}).get("item")
        if items is None:
            logging.warning(f"API 응답에 'item' 데이터가 없습니다 for {region}. 응답 내용: {response.text}")
            continue
        if isinstance(items, dict):
            items = [items]
        # 각 지역 대표 데이터: 첫 번째 항목만 사용
        representative = items[0]
        all_filtered_data.append(representative)

        record_key = f"{representative['sidoName']}_{representative['dataTime']}"
        raw_time = representative["dataTime"]
        korea_timezone = timezone(timedelta(hours=9))
        try:
            dt_grade = datetime.strptime(raw_time, "%Y-%m-%d %H:%M") \
                .replace(tzinfo=korea_timezone).astimezone(timezone.utc)
        except Exception as e:
            logging.error(f"날짜 변환 실패 (airgrade data_time) for {region}: {e}")
            dt_grade = datetime.utcnow()

        # 만약 대표 데이터의 미세먼지 정보가 모두 null이면 해당 지역은 저장하지 않음
        if representative.get("pm25Grade1h") is None and representative.get("pm10Grade1h") is None:
            print(f"{region}의 {raw_time} 데이터는 미세먼지 정보가 없으므로 저장하지 않습니다.")
            continue

        try:
            pm10_grade = int(representative.get("pm10Grade1h") or 0)
        except Exception as e:
            logging.error(f"pm10_grade 변환 실패 for {region}: {e}")
            pm10_grade = 0

        try:
            pm25_grade = int(representative.get("pm25Grade1h") or 0)
        except Exception as e:
            logging.error(f"pm25_grade 변환 실패 for {region}: {e}")
            pm25_grade = 0

        sido = representative["sidoName"] if representative["sidoName"] is not None else ""

        # 중복 체크: 동일 지역, 동일 발표시간 데이터가 있는지 DB 조회 (ALLOW FILTERING 사용)
        check_query = SimpleStatement("""
            SELECT count(*) FROM airgrade WHERE sido=%s AND data_time=%s ALLOW FILTERING
        """)
        result = connector.session.execute(check_query, (sido, dt_grade))
        exists = result.one().count > 0
        if exists:
            print(f"{region}의 {raw_time} 데이터는 이미 저장되어 있습니다.")
            continue

        insert_stmt = SimpleStatement("""
            INSERT INTO airgrade (pm_no, data_time, pm10_grade, pm25_grade, sido)
            VALUES (%s, %s, %s, %s, %s) IF NOT EXISTS
        """)
        connector.session.execute(insert_stmt, (
            uuid4(),
            dt_grade,
            pm10_grade,
            pm25_grade,
            sido
        ))
        print(f"Air Grade 데이터 저장 완료 for {region} - record_key: {record_key}")
    return {"status": "success", "data": all_filtered_data}

# 재난문자 크롤러 클래스 (출력 형식을 JSON으로 통일, 중복 확인은 message_id 기준)
class DisasterMessageCrawler:
    def __init__(self):
        print("크롤러 초기화 중...")
        self.setup_driver()
        self.session = connector.session
        self.seen_message_ids = set()

    def message_exists_in_db(self, message_id):
        query = "SELECT message_id FROM disaster_message WHERE message_id = %s"
        result = self.session.execute(query, (message_id,))
        return result.one() is not None

    def backup_to_db(self, messages_list):
        if not messages_list:
            return
        try:
            for msg in messages_list:
                query = SimpleStatement("""
                    INSERT INTO disaster_message (
                        message_id, emergency_level, DM_ntype, DM_stype,
                        issuing_agency, issued_at, message_content
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    IF NOT EXISTS
                """)
                self.session.execute(query, (
                    msg['message_id'],
                    msg['emergency_level'],
                    msg['DM_ntype'],
                    msg['DM_stype'],
                    msg['issuing_agency'],
                    msg['issued_at'],
                    msg['message_content']
                ))
                print(f"DB에 메시지 삽입 완료 - message_id: {msg['message_id']}")
            print("DB 백업 완료.")
        except Exception as e:
            print(f"DB 삽입 중 오류 발생: {e}")

    def setup_driver(self):
        try:
            print("웹 드라이버 설정 중...")
            chrome_driver_path = '/usr/local/bin/chromedriver'
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            print("크롬 드라이버 초기화 중...")
            service = Service(chrome_driver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(30)
            self.wait = WebDriverWait(self.driver, 20)
            print("드라이버 설정 완료")
        except Exception as e:
            print(f"드라이버 설정 중 오류 발생: {e}")
            raise

    def check_disaster_messages(self):
        try:
            print("웹페이지 접속 시도 중...")
            self.driver.get('https://www.safekorea.go.kr/idsiSFK/neo/sfk/cs/sfc/dis/disasterMsgList.jsp?menuSeq=603')
            print("페이지 로딩 대기 중...")
            time.sleep(5)
            print("메시지 추출 시작...")

            disaster_messages = []
            for i in range(10):
                try:
                    raw_message_id = self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MD101_SN").text
                    try:
                        message_id = int(raw_message_id.strip())
                    except ValueError:
                        message_id = 0

                    if message_id in self.seen_message_ids:
                        continue
                    self.seen_message_ids.add(message_id)

                    disaster_type = self.driver.find_element(By.ID, f"disasterSms_tr_{i}_DSSTR_SE_NM").text
                    emergency_step = self.driver.find_element(By.ID, f"disasterSms_tr_{i}_EMRGNCY_STEP_NM").text
                    location = self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MSG_LOC").text
                    raw_issued_at = self.driver.find_element(By.ID, f"disasterSms_tr_{i}_CREATE_DT").text
                    message_content = self.driver.find_element(By.ID, f"disasterSms_tr_{i}_MSG_CN").get_attribute(
                        "title")

                    try:
                        issued_dt = datetime.strptime(raw_issued_at, "%Y-%m-%d %H:%M")
                    except ValueError:
                        try:
                            issued_dt = datetime.strptime(raw_issued_at, "%Y/%m/%d %H:%M:%S")
                        except ValueError:
                            issued_dt = datetime.now()

                    disaster_messages.append({
                        "message_id": message_id,
                        "emergency_level": emergency_step,
                        "DM_ntype": disaster_type,
                        "DM_stype": "",
                        "issuing_agency": location,
                        "issued_at": issued_dt,
                        "message_content": message_content
                    })
                    print(f"메시지 {i} 추출 성공: {message_id}")
                except Exception as e:
                    print(f"인덱스 {i}에서 메시지 추출 중 오류 발생: {e}")
                    break

            return disaster_messages
        except Exception as e:
            print(f"페이지 접속 중 오류 발생: {e}")
            return []

    def monitor_disaster_messages(self):
        print("실시간 재난문자 모니터링을 시작합니다...")
        print("종료하려면 'q' 또는 'exit'를 입력하고, 저장 현황을 보려면 '1'을 입력하세요.")
        while True:
            try:
                if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                    user_input = sys.stdin.readline().strip().lower()
                    if user_input in ["q", "exit"]:
                        print("모니터링을 종료합니다.")
                        break
                    elif user_input == "1":
                        print("=== 저장 현황 ===")
                        for table in ["airinform", "airgrade", "disaster_message"]:
                            query = f"SELECT count(*) FROM {table};"
                            result = connector.session.execute(query)
                            for row in result:
                                print(f"{table} 테이블 레코드 수: {row.count}")
                        print("=================")

                disaster_messages = self.check_disaster_messages()
                new_messages = []
                for msg in disaster_messages:
                    if not self.message_exists_in_db(msg['message_id']):
                        new_messages.append(msg)
                if new_messages:
                    print("=== 신규 재난 메시지 (JSON 형식) ===")
                    print(json.dumps(new_messages, ensure_ascii=False, indent=2, default=str))
                    print("====================================")
                    self.backup_to_db(new_messages)
                else:
                    print("신규 재난 메시지가 없습니다.")

                print("다음 확인까지 60초 대기 중... (종료: q/exit, 현황보기: 1)")
                for i in range(60):
                    if sys.stdin in select.select([sys.stdin], [], [], 1)[0]:
                        user_input = sys.stdin.readline().strip().lower()
                        if user_input in ["q", "exit"]:
                            print("모니터링을 종료합니다.")
                            return
                        elif user_input == "1":
                            print("=== 저장 현황 ===")
                            for table in ["airinform", "airgrade", "disaster_message"]:
                                query = f"SELECT count(*) FROM {table};"
                                result = connector.session.execute(query)
                                for row in result:
                                    print(f"{table} 테이블 레코드 수: {row.count}")
                            print("=================")
            except KeyboardInterrupt:
                print("\n모니터링을 종료합니다.")
                break
            except Exception as e:
                print(f"\n오류 발생: {e}")
                print("1분 후 다시 시도합니다...")
                time.sleep(60)


def main():
    try:
        print("프로그램 시작")
        air_inform_data = get_air_inform()
        air_grade_data = get_air_grade_all_regions()
        unified_air_output = {
            "air_inform_data": air_inform_data["data"] if isinstance(air_inform_data,
                                                                     dict) and "data" in air_inform_data else air_inform_data,
            "air_grade_data": air_grade_data["data"] if isinstance(air_grade_data,
                                                                   dict) and "data" in air_grade_data else air_grade_data
        }
        print("=== Air API 데이터 (JSON 형식) ===")
        print(json.dumps(unified_air_output, ensure_ascii=False, indent=2))
        print("=================================\n")
    except Exception as e:
        print(f"Air API 호출 또는 DB 저장 중 오류 발생: {e}")
        sys.exit(1)

    try:
        print("\nDisaster Message Monitoring 시작합니다...")
        crawler = DisasterMessageCrawler()
        crawler.monitor_disaster_messages()
    except Exception as e:
        print(f"Disaster Message Monitoring 중 오류 발생: {e}")


if __name__ == "__main__":
    main()

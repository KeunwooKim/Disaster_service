import os
import sys
import time
import json
import logging
import requests
import xmltodict
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from uuid import uuid4

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

# 환경 변수 로드 및 API_KEY 설정
load_dotenv()
API_KEY = os.getenv(
    "API_KEY",
    "7dWUeNJAqaan8oJAs5CbDWKnWaJpLWoxd+lB97UDDRgFfSjfKD7ZGHxM+kRAoZqsga+WlheugBMS2q9WCSaUNg=="
)


# Cassandra 연결을 관리하는 클래스
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


def store_air_inform_three_days():
    """
    3일치 air 정보를 받아와서, 각 forecast_date(예보일자)에 대해 기존 데이터를 삭제한 후
    새롭게 저장하는 함수입니다.
    """
    params = {
        "searchDate": datetime.now().strftime("%Y-%m-%d"),
        "returnType": "xml",
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

    # totalCount 확인
    total_count = int(body.get("totalCount", "0"))
    if total_count == 0:
        print("Air Inform API: 해당 날짜에 데이터가 없습니다.")
        return

    items_section = body.get("items")
    if items_section is None:
        raise Exception("API 응답에 'items' 섹션이 없습니다. 응답 내용: " + response.text)

    items = items_section.get("item")
    if items is None:
        raise Exception("API 응답에 'item' 데이터가 없습니다. 응답 내용: " + response.text)

    # item이 단일 객체인 경우 리스트로 변환
    if not isinstance(items, list):
        items = [items]

    # 예보일자(informData)를 기준으로 항목을 그룹화 (최대 3일치 데이터)
    forecast_data = {}
    for item in items:
        forecast_date_str = item.get("informData")
        if forecast_date_str is None:
            continue
        try:
            forecast_date = datetime.strptime(forecast_date_str, "%Y-%m-%d").date()
        except Exception as e:
            logging.error(f"날짜 변환 실패 (forecast_date): {e}")
            continue

        # 이미 3일치 그룹이 형성되어 있고, 새로운 날짜라면 무시
        if len(forecast_data) >= 3 and forecast_date not in forecast_data:
            continue

        forecast_data.setdefault(forecast_date, []).append(item)

    # 오름차순 정렬 후 최대 3일치 데이터 선택
    forecast_dates = sorted(forecast_data.keys())[:3]

    for f_date in forecast_dates:
        # 기존 데이터 삭제 (해당 forecast_date 파티션의 데이터)
        delete_stmt = SimpleStatement("DELETE FROM airinform WHERE forecast_date = %s")
        connector.session.execute(delete_stmt, (f_date,))
        print(f"Deleted existing records for forecast_date: {f_date}")

        # 새 데이터를 삽입
        for item in forecast_data[f_date]:
            aq_no = uuid4()
            try:
                dt_str = item.get("dataTime", "").replace("시 발표", "").strip()
                data_time = datetime.strptime(dt_str, "%Y-%m-%d %H")
            except Exception as e:
                logging.error(f"날짜 변환 실패 (data_time): {e}")
                data_time = datetime.now()

            cause = item.get("informCause") or ""
            code = item.get("informCode") or ""
            overall = item.get("informOverall") or ""
            grade = item.get("informGrade") or ""

            insert_stmt = SimpleStatement("""
                INSERT INTO airinform (
                    aq_no, cause, code, data_time, forecast_date, grade, overall, search_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """)
            connector.session.execute(insert_stmt, (
                aq_no,
                cause,
                code,
                data_time,
                f_date,
                grade,
                overall,
                datetime.now().date()
            ))
            print(f"Inserted new record for forecast_date {f_date}, aq_no: {aq_no}")


def get_air_grade():
    """
    Air Grade API를 호출하여 데이터를 받아오고, Cassandra DB에 저장하는 함수입니다.
    """
    params = {
        "sidoName": "전국",
        "returnType": "xml",
        "serviceKey": API_KEY,
        "ver": "1.3"
    }
    try:
        response = requests.get(AIR_GRADE_API, params=params, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Air Grade API 호출 실패: {e}")
        raise Exception("Air Grade API 호출 실패")

    data_dict = xmltodict.parse(response.text)
    items = data_dict.get("response", {}).get("body", {}).get("items", {}).get("item")
    if items is None:
        raise Exception("API 응답에 'item' 데이터가 없습니다. 응답 내용: " + response.text)
    if not isinstance(items, list):
        items = [items]

    filtered_data = []
    for item in items:
        extracted = {
            "pm25Grade1h": item.get("pm25Grade1h"),
            "pm10Grade1h": item.get("pm10Grade1h"),
            "sidoName": item.get("sidoName"),
            "dataTime": item.get("dataTime")
        }
        filtered_data.append(extracted)

        pm_no = uuid4()
        dt_grade = None
        try:
            korea_timezone = timezone(timedelta(hours=9))
            dt_grade = datetime.strptime(extracted["dataTime"], "%Y-%m-%d %H:%M:%S") \
                .replace(tzinfo=korea_timezone).astimezone(timezone.utc)
        except Exception as e:
            logging.error(f"날짜 변환 실패 (airgrade data_time, 포맷 1): {e}")
            try:
                korea_timezone = timezone(timedelta(hours=9))
                dt_grade = datetime.strptime(extracted["dataTime"], "%Y-%m-%d %H:%M") \
                    .replace(tzinfo=korea_timezone).astimezone(timezone.utc)
            except Exception as e2:
                logging.error(f"날짜 변환 실패 (airgrade data_time, 포맷 2): {e2}")
                dt_grade = datetime.utcnow()

        try:
            pm10_grade = int(extracted["pm10Grade1h"]) if extracted["pm10Grade1h"] not in (None, "") else 0
        except Exception as e:
            logging.error(f"pm10_grade 변환 실패: {e}")
            pm10_grade = 0

        try:
            pm25_grade = int(extracted["pm25Grade1h"]) if extracted["pm25Grade1h"] not in (None, "") else 0
        except Exception as e:
            logging.error(f"pm25_grade 변환 실패: {e}")
            pm25_grade = 0

        sido = extracted["sidoName"] or ""

        insert_stmt = SimpleStatement("""
            INSERT INTO airgrade (pm_no, data_time, pm10_grade, pm25_grade, sido)
            VALUES (%s, %s, %s, %s, %s)
        """)
        connector.session.execute(insert_stmt, (
            pm_no,
            dt_grade,
            pm10_grade,
            pm25_grade,
            sido
        ))
        print(f"Air Grade 데이터 저장 완료 - pm_no: {pm_no}")

    return filtered_data


def main():
    try:
        print("프로그램 시작")
        # 3일치 Air Inform 데이터 저장 (기존 데이터 삭제 후 새롭게 저장)
        store_air_inform_three_days()
        # Air Grade 데이터 호출 및 DB 저장
        air_grade_data = get_air_grade()
        unified_air_output = {
            "air_inform_data": "새로운 저장 방식 적용",
            "air_grade_data": air_grade_data
        }
        print("=== Air API 데이터 (JSON 형식) ===")
        print(json.dumps(unified_air_output, ensure_ascii=False, indent=2))
        print("=================================\n")
    except Exception as e:
        print(f"Air API 호출 또는 DB 저장 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

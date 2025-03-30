# push_service.py
import os
import time
import logging
from datetime import datetime, timedelta
from uuid import uuid5, NAMESPACE_DNS
from dotenv import load_dotenv

from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 환경 변수 로드
load_dotenv()


# API_KEY 등 필요 시 추가

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

# (필요 시) 통합 데이터 저장 함수 - 여기서는 주로 조회만 진행합니다.

app = FastAPI()


def send_push_notification(message: str):
    """
    실제 푸시 알림 전송 코드(예: FCM, APNs) 대신, 로그에 기록합니다.
    """
    logging.info("푸시 알림 전송: " + message)
    # 실제 전송 로직 추가 가능
@app.get("/")
def read_root():
    return {"message": "푸시 서비스가 실행 중입니다."}


@app.get("/push")
def push_notifications():
    """
    최근 5분간의 rtd_db 이벤트를 조회하여,
      - rtd_code 71 (미세먼지) 이벤트: rtd_details 내 pm10_grade 또는 pm25_grade가 3 이상이면 알림 전송
      - 그 외 재난 이벤트: 조건 없이 모두 알림 전송
    """
    recent_time = datetime.utcnow() - timedelta(minutes=5)
    query = "SELECT rtd_code, rtd_time, rtd_loc, rtd_details FROM rtd_db WHERE rtd_time > %s ALLOW FILTERING"
    statement = SimpleStatement(query)
    rows = connector.session.execute(statement, (recent_time,))

    notifications = []
    for row in rows:
        if row.rtd_code == 71:
            grade_ok = False
            for detail in row.rtd_details:
                if "pm10_grade:" in detail:
                    try:
                        grade = int(detail.split("pm10_grade:")[1].strip())
                        if grade >= 3:
                            grade_ok = True
                    except Exception:
                        pass
                elif "pm25_grade:" in detail:
                    try:
                        grade = int(detail.split("pm25_grade:")[1].strip())
                        if grade >= 3:
                            grade_ok = True
                    except Exception:
                        pass
            if grade_ok:
                message = f"[대기질 경보] 지역: {row.rtd_loc} / 상세: {', '.join(row.rtd_details)}"
                send_push_notification(message)
                notifications.append(message)
        else:
            message = f"[재난 알림 - 코드 {row.rtd_code}] 지역: {row.rtd_loc} / 상세: {', '.join(row.rtd_details)}"
            send_push_notification(message)
            notifications.append(message)

    return JSONResponse(content={"notifications_sent": notifications})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

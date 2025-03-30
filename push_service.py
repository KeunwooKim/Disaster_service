from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import logging
from datetime import datetime, timedelta, timezone
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import os
from dotenv import load_dotenv

# 환경 변수 로드 (.env 파일 활용)
load_dotenv()
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "andy013")
CASSANDRA_PASS = os.getenv("CASSANDRA_PASS", "1212")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "disaster_service")

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Cassandra 연결 설정
try:
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT,
                      auth_provider=PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS))
    session = cluster.connect(KEYSPACE)
    logging.info("Cassandra 연결 성공")
except Exception as e:
    logging.error(f"Cassandra 연결 실패: {e}")
    raise

app = FastAPI()

# 전송한 푸시 알림 데이터를 저장할 전역 리스트
push_log = []

# 루트 엔드포인트
@app.get("/")
def read_root():
    return {"message": "Push service is running."}

def send_push_notification(message: str):
    """
    실제 푸시 알림 전송 로직 대신, 로그에 기록하고 전역 리스트에 저장합니다.
    """
    try:
        # 푸시 전송 시각 및 메시지 기록
        timestamp = datetime.utcnow().isoformat()
        push_entry = {"timestamp": timestamp, "message": message}
        push_log.append(push_entry)
        logging.info("푸시 알림 전송: " + message)
    except Exception as e:
        logging.error(f"푸시 알림 전송 실패: {e}")

@app.get("/push")
def push_notifications(time_window_minutes: int = 5):
    """
    최근 time_window_minutes(기본 5분)간의 rtd_db 이벤트를 조회하여 조건에 맞는 푸시 알림을 전송합니다.
    여기서는 전송된 알림은 로그와 push_log 리스트에 기록됩니다.
    """
    try:
        recent_time = datetime.utcnow() - timedelta(minutes=time_window_minutes)
        query = "SELECT rtd_code, rtd_time, rtd_loc, rtd_details FROM rtd_db WHERE rtd_time > %s ALLOW FILTERING"
        statement = SimpleStatement(query)
        rows = session.execute(statement, (recent_time,))
    except Exception as e:
        logging.error(f"Cassandra 조회 에러: {e}")
        raise HTTPException(status_code=500, detail="Cassandra 조회 실패")

    notifications = []
    for row in rows:
        try:
            # rtd_code 71 (미세먼지) 이벤트: pm10_grade 혹은 pm25_grade가 3 이상이면 알림 전송
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
                # 그 외 재난 이벤트
                message = f"[재난 알림 - 코드 {row.rtd_code}] 지역: {row.rtd_loc} / 상세: {', '.join(row.rtd_details)}"
                send_push_notification(message)
                notifications.append(message)
        except Exception as e:
            logging.error(f"푸시 알림 처리 중 에러: {e}")
            continue

    return JSONResponse(content={"notifications_sent": notifications, "count": len(notifications)})

@app.get("/push/log")
def get_push_log():
    """
    지금까지 전송된 푸시 알림 데이터(메시지와 전송 시각)를 조회하는 엔드포인트입니다.
    """
    return {"push_log": push_log, "count": len(push_log)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

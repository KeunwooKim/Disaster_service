from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
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
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
    )
    session = cluster.connect(KEYSPACE)
    logging.info("Cassandra 연결 성공")
except Exception as e:
    logging.error(f"Cassandra 연결 실패: {e}")
    raise

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Test Events API is running."}

@app.get("/events")
def get_test_events():
    """
    test_events 테이블의 모든 데이터를 조회하여 JSON으로 반환하는 엔드포인트입니다.
    """
    try:
        query = """
            SELECT disaster_id, description, disaster_time, disaster_type, latitude, longitude 
            FROM test_events
        """
        rows = session.execute(query)
        events = []
        for row in rows:
            event = {
                "disaster_id": str(row.disaster_id),
                "description": row.description,
                "disaster_time": row.disaster_time.isoformat() if row.disaster_time else None,
                "disaster_type": row.disaster_type,
                "latitude": row.latitude,
                "longitude": row.longitude,
            }
            events.append(event)
        return JSONResponse(content={"events": events, "count": len(events)})
    except Exception as e:
        logging.error(f"Cassandra 조회 에러: {e}")
        raise HTTPException(status_code=500, detail="Cassandra 조회 실패")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

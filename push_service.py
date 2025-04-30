from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv
from typing import Optional

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

@app.get("/test")
def test(test_id: Optional[str], test_code: Optional[str] = None):
    print(test_id)
    if test_id is None and test_code is None:
        raise HTTPException(
            status_code=404,
            detail="값이 비었습니다"
        )
    if test_id is not None and test_code is not None:
        result = []
        result.append({"test_id": test_id, "test_code": test_code})
        print(result)
        return JSONResponse(content={"results": result, "count": len(result)})

@app.get("/userReport")
def createUserMsg(userId: Optional[str] = None, disasterType: Optional[int] = None, disasterTime: Optional[str] = None, reportContent: Optional[str] = None, disasterPos: Optional[str] = None):
    if userId is None and disasterType is None:
        raise HTTPException(
            status_code=404,
            detail="값이 비었습니다"
        )
    if userId is not None and disasterType is not None:
        result = []
        result.append({"userId": userId,
                       "disasterType": disasterType,
                       "disasterTime": disasterTime,
                       "reportContent": reportContent,
                       "disasterPos": disasterPos,
                       })
        print(result)
    return JSONResponse(content={"results": result})

@app.get("/rtd/search")
def search_rtd(rtd_code: Optional[int] = None, rtd_loc: Optional[str] = None):
    """
    rtd_db 테이블에서 재난 유형(rtd_code) 또는 재난 장소(rtd_loc)를 기준으로
    데이터를 검색하여 JSON으로 반환하는 엔드포인트입니다.

    두 조건 중 하나만 제공되어도 해당 조건에 맞는 데이터를 반환합니다.
    Cassandra에서는 문자열 검색 시 ALLOW FILTERING을 사용하므로,
    프로덕션 환경에서는 적절한 인덱싱이나 데이터 모델링이 필요합니다.
    """
    if rtd_code is None and rtd_loc is None:
        raise HTTPException(
            status_code=400,
            detail="검색 조건인 재난 유형(rtd_code) 또는 재난 장소(rtd_loc) 중 하나를 제공해주세요."
        )
    results = []
    seen_ids = set()
    # rtd_code 조건 검색
    if rtd_code is not None:
        try:
            query = """
                SELECT rtd_code, rtd_time, id, rtd_loc, rtd_details 
                FROM rtd_db 
                WHERE rtd_code = %s ALLOW FILTERING
            """
            rows = session.execute(query, (rtd_code,))
            for row in rows:
                row_id = str(row.id)
                if row_id not in seen_ids:
                    results.append({
                        "rtd_code": row.rtd_code,
                        "rtd_time": row.rtd_time.isoformat() if row.rtd_time else None,
                        "id": row_id,
                        "rtd_loc": row.rtd_loc,
                        "rtd_details": row.rtd_details
                    })
                    seen_ids.add(row_id)
        except Exception as e:
            logging.error(f"rtd_db 검색 에러 (rtd_code): {e}")
            raise HTTPException(status_code=500, detail="rtd_db 검색 실패 (rtd_code)")
    # rtd_loc 조건 검색
    if rtd_loc is not None:
        try:
            query = """
                SELECT rtd_code, rtd_time, id, rtd_loc, rtd_details 
                FROM rtd_db 
                WHERE rtd_loc = %s ALLOW FILTERING
            """
            rows = session.execute(query, (rtd_loc,))
            for row in rows:
                row_id = str(row.id)
                if row_id not in seen_ids:
                    results.append({
                        "rtd_code": row.rtd_code,
                        "rtd_time": row.rtd_time.isoformat() if row.rtd_time else None,
                        "id": row_id,
                        "rtd_loc": row.rtd_loc,
                        "rtd_details": row.rtd_details
                    })
                    seen_ids.add(row_id)
        except Exception as e:
            logging.error(f"rtd_db 검색 에러 (rtd_loc): {e}")
            raise HTTPException(status_code=500, detail="rtd_db 검색 실패 (rtd_loc)")
    return JSONResponse(content={"results": results, "count": len(results)})


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

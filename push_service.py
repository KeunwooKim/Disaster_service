from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime, timedelta
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
def search_rtd(
    rtd_loc: Optional[str] = None,
    regioncode: Optional[int] = None,
    rtd_code: Optional[int] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    days: Optional[int] = 1
):
    """
    재난 정보 검색 API
    - rtd_loc: 장소 기반 검색
    - regioncode: 지역코드 기반 검색
    - rtd_code: 재난코드 (선택)
    - from_time, to_time: ISO 8601 시간 필터
    - days: 시간 필터 기본값 1일
    """

    now = datetime.utcnow()

    try:
        if from_time and to_time:
            start_time = datetime.fromisoformat(from_time)
            end_time = datetime.fromisoformat(to_time)
        else:
            end_time = now
            start_time = now - timedelta(days=days)
    except ValueError:
        raise HTTPException(status_code=400, detail="시간 형식이 잘못되었습니다 (ISO 8601)")

    start_str = start_time.isoformat()
    end_str = end_time.isoformat()
    results = []

    try:
        # 1. 조건 없음 → 전체 시간 기준 최신 데이터 (rtd_by_time)
        if rtd_loc is None and regioncode is None:
            query = """
                SELECT * FROM rtd_by_time
                WHERE rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rows = session.execute(query, (start_time, end_time))

        # 2. rtd_loc 기반 조회 (rtd_by_loc_time)
        elif rtd_loc:
            if rtd_code is None:
                raise HTTPException(status_code=400, detail="rtd_loc 사용 시 rtd_code도 필요합니다.")
            query = """
                SELECT * FROM rtd_by_loc_time
                WHERE rtd_loc = %s AND rtd_code = %s
                AND rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rows = session.execute(query, (rtd_loc, rtd_code, start_time, end_time))

        # 3. regioncode 기반 조회 (rtd_by_region_time)
        elif regioncode:
            if rtd_code is None:
                raise HTTPException(status_code=400, detail="regioncode 사용 시 rtd_code도 필요합니다.")
            query = """
                SELECT * FROM rtd_by_region_time
                WHERE regioncode = %s AND rtd_code = %s
                AND rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rows = session.execute(query, (regioncode, rtd_code, start_time, end_time))

        else:
            raise HTTPException(status_code=400, detail="rtd_loc 또는 regioncode 중 하나는 필요합니다.")

        # 결과 정리
        for row in rows:
            results.append({
                "id": str(row.id),
                "rtd_time": row.rtd_time.isoformat() if row.rtd_time else None,
                "rtd_loc": row.rtd_loc,
                "rtd_details": row.rtd_details,
                "rtd_code": row.rtd_code,
                "regioncode": row.regioncode if hasattr(row, 'regioncode') else None,
                "latitude": row.latitude if hasattr(row, 'latitude') else None,
                "longitude": row.longitude if hasattr(row, 'longitude') else None,
            })

        return JSONResponse(content={"count": len(results), "results": results})

    except Exception as e:
        logging.error(f"검색 오류: {e}")
        raise HTTPException(status_code=500, detail="rtd 검색 실패")
    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

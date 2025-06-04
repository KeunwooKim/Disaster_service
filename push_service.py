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
        raise HTTPException(status_code=400, detail="시간 형식이 잘못되었습니다. ISO 8601 형식으로 입력해주세요.")

    results = []
    seen_ids = set()

    # 1. 지역명 + 재난코드
    if rtd_loc and rtd_code:
        try:
            query = """
                SELECT id, rtd_time, rtd_loc, rtd_details, rtd_code
                FROM rtd_by_loc_time
                WHERE rtd_loc = %s AND rtd_code = %s AND rtd_time >= %s AND rtd_time <= %s
            """
            rows = session.execute(query, (rtd_loc, rtd_code, start_time, end_time))
            for row in rows:
                row_id = str(row.id)
                if row_id not in seen_ids:
                    results.append({
                        "id": row_id,
                        "rtd_time": row.rtd_time.isoformat(),
                        "rtd_loc": row.rtd_loc,
                        "rtd_details": row.rtd_details,
                        "rtd_code": row.rtd_code
                    })
                    seen_ids.add(row_id)
        except Exception as e:
            logging.error(f"[rtd_by_loc_time] 조회 실패: {e}")
            raise HTTPException(status_code=500, detail="지역 기반 검색 실패")

    # 2. 지역코드 + 재난코드
    elif regioncode and rtd_code:
        try:
            query = """
                SELECT id, rtd_time, regioncode, rtd_loc, rtd_details, rtd_code
                FROM rtd_by_region_time
                WHERE regioncode = %s AND rtd_code = %s AND rtd_time >= %s AND rtd_time <= %s
            """
            rows = session.execute(query, (regioncode, rtd_code, start_time, end_time))
            for row in rows:
                row_id = str(row.id)
                if row_id not in seen_ids:
                    results.append({
                        "id": row_id,
                        "rtd_time": row.rtd_time.isoformat(),
                        "rtd_loc": row.rtd_loc,
                        "rtd_details": row.rtd_details,
                        "rtd_code": row.rtd_code
                    })
                    seen_ids.add(row_id)
        except Exception as e:
            logging.error(f"[rtd_by_region_time] 조회 실패: {e}")
            raise HTTPException(status_code=500, detail="지역코드 기반 검색 실패")

    # 3. 조건이 모두 없는 경우: 최근 하루치
    elif not rtd_loc and not regioncode and not rtd_code:
        try:
            query = """
                SELECT id, rtd_time, rtd_loc, rtd_details, rtd_code
                FROM rtd_by_time
                WHERE rtd_time >= %s AND rtd_time <= %s
            """
            rows = session.execute(query, (start_time, end_time))
            for row in rows:
                row_id = str(row.id)
                if row_id not in seen_ids:
                    results.append({
                        "id": row_id,
                        "rtd_time": row.rtd_time.isoformat(),
                        "rtd_loc": row.rtd_loc,
                        "rtd_details": row.rtd_details,
                        "rtd_code": row.rtd_code
                    })
                    seen_ids.add(row_id)
        except Exception as e:
            logging.error(f"[rtd_by_time] 기본 검색 실패: {e}")
            raise HTTPException(status_code=500, detail="기본 검색 실패")

    else:
        raise HTTPException(status_code=400, detail="rtd_loc + rtd_code 또는 regioncode + rtd_code 조합을 사용하거나 조건 없이 요청해주세요.")

    return JSONResponse(content={"results": results, "count": len(results)})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

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

from uuid import uuid4
from fastapi import Query

@app.get("/userReport/history")
def get_user_report_history(
    userId: str = Query(..., description="제보자 ID"),
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    days: Optional[int] = 7
):
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

    try:
        query = """
            SELECT * FROM user_report_by_user_time
            WHERE report_by_id = %s AND report_at >= %s AND report_at <= %s
            ALLOW FILTERING
        """
        rows = session.execute(query, (userId, start_time, end_time))

        reports = []
        for row in rows:
            reports.append({
                "report_id": str(row.report_id),
                "report_time": row.report_at.isoformat() if row.report_at else None,
                "middle_type": row.middle_type,
                "small_type": row.small_type,
                "report_location": row.report_location,
                "report_content": row.report_content,
                "latitude": row.report_lat,
                "longitude": row.report_lot,
                "visable": row.visable,
                "delete_vote": row.delete_vote
            })

        return JSONResponse(content={"count": len(reports), "results": reports})

    except Exception as e:
        logging.error(f"사용자 제보 내역 조회 실패: {e}")
        raise HTTPException(status_code=500, detail="사용자 제보 조회 실패")

@app.post("/userReport")
def create_user_report(
    userId: str,
    disasterType: str,
    disasterTime: Optional[str] = None,
    reportContent: Optional[str] = None,
    disasterPos: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None
):
    try:
        report_time = datetime.fromisoformat(disasterTime) if disasterTime else datetime.utcnow()
        report_id = uuid4()

        query = """
            INSERT INTO user_report (
                report_by_id, report_at, report_id, middle_type, small_type,
                report_location, report_content, report_lat, report_lot,
                visable, delete_vote, vote_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, true, 0, [])
        """

        session.execute(query, (
            userId,
            report_time,
            report_id,
            disasterType,
            disasterType,
            disasterPos,
            reportContent,
            latitude,
            longitude
        ))

        return {"message": "제보가 성공적으로 저장되었습니다.", "report_id": str(report_id)}

    except Exception as e:
        logging.error(f"제보 저장 실패: {e}")
        raise HTTPException(status_code=500, detail="제보 저장 실패")


@app.get("/rtd/search")
def search_rtd(
    rtd_loc: Optional[str] = None,
    regioncode: Optional[int] = None,
    rtd_code: Optional[int] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    days: Optional[int] = 1
):
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

    rtd_results = []
    report_results = []

    try:
        # === 1) RTD 데이터 조회 ===
        if rtd_loc:
            rtd_query = """
                SELECT * FROM rtd_by_loc_time
                WHERE rtd_loc = %s AND rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rtd_rows = session.execute(rtd_query, (rtd_loc, start_time, end_time))
        elif regioncode:
            rtd_query = """
                SELECT * FROM rtd_by_region_time
                WHERE regioncode = %s AND rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rtd_rows = session.execute(rtd_query, (regioncode, start_time, end_time))
        elif rtd_code is not None:
            rtd_query = """
                SELECT * FROM rtd_by_code_time
                WHERE rtd_code = %s AND rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rtd_rows = session.execute(rtd_query, (rtd_code, start_time, end_time))
        else:
            rtd_query = """
                SELECT * FROM rtd_by_code_time
                WHERE rtd_time >= %s AND rtd_time <= %s
                ALLOW FILTERING
            """
            rtd_rows = session.execute(rtd_query, (start_time, end_time))

        for row in rtd_rows:
            rtd_results.append({
                "type": "rtd",
                "id": str(row.id),
                "rtd_time": row.rtd_time.isoformat() if row.rtd_time else None,
                "rtd_loc": row.rtd_loc,
                "rtd_details": row.rtd_details,
                "rtd_code": row.rtd_code,
                "regioncode": getattr(row, 'regioncode', None),
                "latitude": getattr(row, 'latitude', None),
                "longitude": getattr(row, 'longitude', None),
            })

        # === 2) 제보 데이터 조회 ===
        if rtd_loc:
            report_query = """
                SELECT * FROM user_report_by_location_time
                WHERE report_location = %s AND report_at >= %s AND report_at <= %s
                ALLOW FILTERING
            """
            report_rows = session.execute(report_query, (rtd_loc, start_time, end_time))

            for row in report_rows:
                report_results.append({
                    "type": "report",
                    "report_id": str(row.report_id),
                    "report_time": row.report_at.isoformat() if row.report_at else None,
                    "report_location": row.report_location,
                    "middle_type": row.middle_type,
                    "small_type": row.small_type,
                    "content": row.report_content,
                    "report_by": row.report_by_id,
                    "latitude": row.report_lat,
                    "longitude": row.report_lot,
                    "delete_vote": row.delete_vote
                })

        return JSONResponse(content={
            "count": len(rtd_results) + len(report_results),
            "rtd_results": rtd_results,
            "report_results": report_results
        })

    except Exception as e:
        logging.error(f"검색 오류: {e}")
        raise HTTPException(status_code=500, detail="rtd/search 통합 검색 실패")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

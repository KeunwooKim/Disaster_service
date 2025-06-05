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
from uuid import UUID
from pydantic import BaseModel

from uuid import uuid4
from fastapi import Query

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
                "visible": row.visible,
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
        # 사용자 존재 여부 확인
        check_query = "SELECT user_id FROM user_device WHERE user_id = %s"
        result = session.execute(check_query, (userId,)).one()

        if result is None:
            raise HTTPException(status_code=404, detail="사용자 정보가 존재하지 않습니다.")

        report_time = datetime.fromisoformat(disasterTime) if disasterTime else datetime.utcnow()
        report_id = uuid4()

        insert_query = """
            INSERT INTO user_report (
                report_by_id, report_at, report_id, middle_type, small_type,
                report_location, report_content, report_lat, report_lot,
                visible, delete_vote, vote_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, true, 0, [])
        """

        session.execute(insert_query, (
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

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"제보 저장 실패: {e}")
        raise HTTPException(status_code=500, detail="제보 저장 실패")

class VoteByIDRequest(BaseModel):
    report_id: UUID
    user_id: str

@app.post("/report/vote_by_id")
def vote_to_delete_by_report_id(data: VoteByIDRequest):
    try:
        # 1. 기존 데이터 조회
        query = "SELECT delete_vote, vote_id, visible FROM user_report_by_id WHERE report_id = %s"
        row = session.execute(query, (data.report_id,)).one()

        if not row:
            raise HTTPException(status_code=404, detail="해당 제보를 찾을 수 없습니다.")

        vote_ids = row.vote_id or []
        if data.user_id in vote_ids:
            raise HTTPException(status_code=400, detail="이미 이 제보에 투표하셨습니다.")

        # 2. 투표 추가 및 비활성 여부 판단
        vote_ids.append(data.user_id)
        new_count = (row.delete_vote or 0) + 1
        visible_flag = False if new_count >= 10 else True

        # 3. 업데이트
        update_q = """
            UPDATE user_report_by_id
            SET vote_id = %s,
                delete_vote = %s,
                visible = %s
            WHERE report_id = %s
        """
        session.execute(update_q, (vote_ids, new_count, visible_flag, data.report_id))

        return JSONResponse(content={
            "message": "투표 완료",
            "delete_vote": new_count,
            "visible": visible_flag
        })

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/report/user_history")
def get_reports_by_user(
    user_id: str = Query(..., description="제보자 ID"),
    limit: int = 50
):
    try:
        query = """
            SELECT report_at, report_id, middle_type, small_type,
                   report_location, report_content,
                   report_lat, report_lot, visible, delete_vote, vote_id
            FROM user_report_by_user_time
            WHERE report_by_id = %s
            LIMIT %s
        """
        rows = session.execute(query, (user_id, limit))

        results = []
        for row in rows:
            results.append({
                "report_id": str(row.report_id),
                "report_at": row.report_at.isoformat() if row.report_at else None,
                "middle_type": row.middle_type,
                "small_type": row.small_type,
                "report_location": row.report_location,
                "report_content": row.report_content,
                "latitude": row.report_lat,
                "longitude": row.report_lot,
                "visible": row.visible,
                "delete_vote": row.delete_vote,
                "vote_id": row.vote_id,
            })

        return JSONResponse(content={"count": len(results), "results": results})

    except Exception as e:
        logging.error(f"사용자 제보 조회 실패: {e}")
        raise HTTPException(status_code=500, detail="사용자 제보 조회 실패")

class DeleteReportRequest(BaseModel):
    report_id: UUID
    user_id: str

@app.delete("/report/delete")
def delete_user_report(data: DeleteReportRequest):
    try:
        # 1. 해당 report_id가 존재하는지 확인
        query = "SELECT report_by_id FROM user_report_by_id WHERE report_id = %s"
        result = session.execute(query, (data.report_id,)).one()

        if not result:
            raise HTTPException(status_code=404, detail="해당 제보를 찾을 수 없습니다.")

        # 2. 작성자 일치 여부 확인
        if result.report_by_id != data.user_id:
            raise HTTPException(status_code=403, detail="해당 제보의 작성자가 아닙니다.")

        # 3. 삭제 (실제 삭제 대신 visible=False 처리할 수도 있음)
        delete_query = "DELETE FROM user_report_by_id WHERE report_id = %s"
        session.execute(delete_query, (data.report_id,))

        return {"message": "제보가 삭제되었습니다."}

    except Exception as e:
        logging.error(f"제보 삭제 실패: {e}")
        raise HTTPException(status_code=500, detail="제보 삭제 실패")

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

        # === 2) user_report 테이블에서 visible=True만 필터링 ===
        if rtd_loc:
            report_query = """
                SELECT * FROM user_report
                WHERE report_location = %s AND report_at >= %s AND report_at <= %s
                ALLOW FILTERING
            """
            report_rows = session.execute(report_query, (rtd_loc, start_time, end_time))

            for row in report_rows:
                if row.visible:  # ✅ visible = True 조건
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


# 사용자 디바이스 요청 모델
class UserDeviceRequest(BaseModel):
    user_id: str
    device_token: str

# 디바이스 등록 API
@app.post("/devices/register")
def register_device(data: UserDeviceRequest):
    try:
        # user_id 중복 확인
        check_user_query = "SELECT * FROM user_device WHERE user_id = %s"
        existing_user = session.execute(check_user_query, (data.user_id,)).one()
        if existing_user:
            return JSONResponse(
                status_code=400,
                content={"message": f"이미 등록된 user_id입니다: {data.user_id}"}
            )

        # device_token 중복 확인 (중복이면 실패 처리)
        check_token_query = "SELECT user_id FROM user_device WHERE device_token = %s ALLOW FILTERING"
        duplicate = session.execute(check_token_query, (data.device_token,)).one()
        if duplicate:
            return JSONResponse(
                status_code=400,
                content={"message": f"이미 다른 사용자에 등록된 device_token입니다: {data.device_token}"}
            )

        # 디바이스 등록
        insert_query = "INSERT INTO user_device (user_id, device_token) VALUES (%s, %s)"
        session.execute(insert_query, (data.user_id, data.device_token))

        return {"message": "사용자 디바이스 정보가 등록되었습니다."}
    except Exception as e:
        logging.error(f"디바이스 등록 실패: {e}")
        raise HTTPException(status_code=500, detail="디바이스 등록 실패")

# 디바이스 토큰 수정 API
@app.put("/devices/update")
def update_device_token(data: UserDeviceRequest):
    try:
        # user_id 존재 확인
        check_user_query = "SELECT * FROM user_device WHERE user_id = %s"
        user = session.execute(check_user_query, (data.user_id,)).one()
        if not user:
            return JSONResponse(
                status_code=404,
                content={"message": f"존재하지 않는 user_id입니다: {data.user_id}"}
            )

        # device_token 중복 검사
        check_token_query = "SELECT user_id FROM user_device WHERE device_token = %s ALLOW FILTERING"
        duplicate = session.execute(check_token_query, (data.device_token,)).one()
        if duplicate and duplicate.user_id != data.user_id:
            return JSONResponse(
                status_code=400,
                content={"message": f"이미 다른 사용자에 등록된 device_token입니다: {data.device_token}"}
            )

        # 토큰 업데이트
        update_query = "UPDATE user_device SET device_token = %s WHERE user_id = %s"
        session.execute(update_query, (data.device_token, data.user_id))

        return {"message": "디바이스 토큰이 성공적으로 수정되었습니다."}
    except Exception as e:
        logging.error(f"디바이스 토큰 수정 실패: {e}")
        raise HTTPException(status_code=500, detail="디바이스 토큰 수정 실패")

# 디바이스 조회 API (전체 또는 특정 사용자)
@app.get("/devices")
def get_devices(user_id: Optional[str] = Query(None, description="user_id로 필터링")):
    try:
        if user_id:
            query = "SELECT * FROM user_device WHERE user_id = %s"
            rows = session.execute(query, (user_id,))
        else:
            query = "SELECT * FROM user_device"
            rows = session.execute(query)

        results = []
        for row in rows:
            results.append({
                "user_id": row.user_id,
                "device_token": row.device_token
            })

        return {"count": len(results), "devices": results}
    except Exception as e:
        logging.error(f"디바이스 조회 실패: {e}")
        raise HTTPException(status_code=500, detail="디바이스 조회 실패")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

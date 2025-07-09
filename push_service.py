from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import uvicorn
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime, timedelta, timezone
from uuid import UUID
from pydantic import BaseModel

from uuid import uuid4
from fastapi import Query
from fastapi import Body

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


@app.get("/userReport/history")
def get_user_report_history(
    userId: Optional[str] = Query(None, description="제보자 ID (없으면 전체 조회)"),
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    days: Optional[int] = 7
):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    try:
        if from_time and to_time:
            start_time = datetime.fromisoformat(from_time).astimezone(timezone.utc)
            end_time = datetime.fromisoformat(to_time).astimezone(timezone.utc)
        else:
            end_time = now + timedelta(seconds=1)
            start_time = now - timedelta(days=days)

        logging.info(f"User Report History - Querying from {start_time} to {end_time}")

    except ValueError:
        raise HTTPException(status_code=400, detail="시간 형식이 잘못되었습니다 (ISO 8601)")

    try:
        if userId:
            query = """
                SELECT * FROM user_report_by_user_time
                WHERE report_by_id = %s AND report_at >= %s AND report_at <= %s
                ALLOW FILTERING
                ORDER BY report_at DESC
            """
            rows = session.execute(query, (userId, start_time, end_time))
        else:
            query = """
                SELECT * FROM user_report_by_time
                WHERE report_at >= %s AND report_at <= %s
                ALLOW FILTERING
            """
            rows = session.execute(query, (start_time, end_time))

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
                "delete_vote": row.delete_vote,
                "report_by_id": row.report_by_id
            })

        # Python에서 report_time을 기준으로 내림차순 정렬
        reports.sort(key=lambda x: x.get("report_time") or "", reverse=True)

        return JSONResponse(content={"events": reports, "count": len(reports)})

    except Exception as e:
        logging.error(f"제보 내역 조회 실패: {e}")
        raise HTTPException(status_code=500, detail="제보 내역 조회 실패")

SMALL_TYPE_TO_MIDDLE = {
    "31": ("풍수해", "30"),   # 태풍
    "32": ("풍수해", "30"),   # 호우
    "33": ("풍수해", "30"),   # 홍수
    "34": ("풍수해", "30"),   # 강풍
    "35": ("풍수해", "30"),   # 대설
    "41": ("기상재난", "40"), # 폭염
    "42": ("기상재난", "40"), # 한파
    "51": ("지질재난", "50"), # 지진
    "11": ("감염병", "11"),   # 감염병
    "61": ("화재/폭발", "60"), # 산불
    "62": ("화재/폭발", "60"), # 일일화재
    "71": ("미세먼지", "70"), # 미세먼지 시도별
    "72": ("미세먼지", "70"), # 대기질 예보
}

class UserReportRequest(BaseModel):
    userId: str
    disasterType: str
    disasterTime: Optional[str] = None
    reportContent: Optional[str] = None
    disasterPos: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

@app.post("/userReport")
def create_user_report(request: UserReportRequest):
    try:
        # 사용자 존재 여부 확인
        check_query = "SELECT user_id FROM user_device WHERE user_id = %s"
        result = session.execute(check_query, (request.userId,)).one()

        if result is None:
            raise HTTPException(status_code=404, detail="사용자 정보가 존재하지 않습니다.")

        if request.disasterTime:
            dt_obj = datetime.fromisoformat(request.disasterTime)
            if dt_obj.tzinfo is None: # Naive datetime, assume KST (UTC+9)
                report_time = dt_obj - timedelta(hours=9)
                report_time = report_time.replace(tzinfo=timezone.utc)
            else: # Timezone-aware datetime
                report_time = dt_obj.astimezone(timezone.utc)
        else:
            report_time = datetime.utcnow().replace(tzinfo=timezone.utc)

        report_id = uuid4()

        small_type = request.disasterType

        if small_type not in SMALL_TYPE_TO_MIDDLE:
            raise HTTPException(status_code=400, detail=f"알 수 없는 소분류 코드: {small_type}")

        _, middle_type = SMALL_TYPE_TO_MIDDLE[small_type]

        insert_query = """
            INSERT INTO user_report (
                report_by_id, report_at, report_id, middle_type, small_type,
                report_location, report_content, report_lat, report_lot,
                visible, delete_vote, vote_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, true, 0, [])
        """

        session.execute(insert_query, (
            request.userId,
            report_time,
            report_id,
            middle_type,
            small_type,
            request.disasterPos,
            request.reportContent,
            request.latitude,
            request.longitude
        ))

        logging.info(f"New user report created: report_id={report_id}, report_time={report_time}")

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
        query = "SELECT report_by_id, report_at, delete_vote, vote_user_ids, visible FROM user_report WHERE report_id = %s ALLOW FILTERING"
        row = session.execute(query, (data.report_id,)).one()

        if not row:
            raise HTTPException(status_code=404, detail="해당 제보를 찾을 수 없습니다.")

        voter_ids = row.vote_user_ids or []
        if data.user_id in voter_ids:
            raise HTTPException(status_code=400, detail="이미 이 제보에 투표하셨습니다.")

        # 2. 투표 추가 및 비활성 여부 판단
        voter_ids.append(data.user_id)
        new_count = (row.delete_vote or 0) + 1
        visible_flag = False if new_count >= 10 else True

        # 3. 업데이트
        update_q = """
            UPDATE user_report
            SET vote_user_ids = %s,
                delete_vote = %s,
                visible = %s
            WHERE report_by_id = %s AND report_at = %s
        """
        session.execute(update_q, (voter_ids, new_count, visible_flag, row.report_by_id, row.report_at))

        return JSONResponse(content={
            "message": "투표 완료",
            "delete_vote": new_count,
            "visible": visible_flag
        })

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"투표 처리 실패: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="투표 처리 실패")

class RtdVoteRequest(BaseModel):
    rtd_time: datetime
    rtd_id: UUID
    user_id: str

@app.post("/rtd/vote")
def vote_on_rtd(data: RtdVoteRequest):
    try:
        # 1. Fetch current vote status
        query = """
            SELECT vote_count, vote_user_ids, visible
            FROM rtd_db
            WHERE rtd_time = %s AND id = %s
        """
        row = session.execute(query, (data.rtd_time, data.rtd_id)).one()

        if not row:
            raise HTTPException(status_code=404, detail="해당 RTD 항목을 찾을 수 없습니다.")

        voter_ids = row.vote_user_ids or []
        if data.user_id in voter_ids:
            raise HTTPException(status_code=400, detail="이미 이 RTD 항목에 투표하셨습니다.")

        # 2. Update vote count and user IDs
        voter_ids.append(data.user_id)
        new_count = (row.vote_count or 0) + 1
        visible_flag = False if new_count >= 10 else True # 10표 이상이면 visible = False

        logging.info(f"RTD Vote - Before update: rtd_id={data.rtd_id}, current_vote_count={row.vote_count}, new_count={new_count}, visible_flag={visible_flag}")

        update_query = """
            UPDATE rtd_db
            SET vote_count = %s, vote_user_ids = %s, visible = %s
            WHERE rtd_time = %s AND id = %s
        """
        session.execute(update_query, (new_count, voter_ids, visible_flag, data.rtd_time, data.rtd_id))

        return JSONResponse(content={
            "message": "RTD 투표 완료",
            "vote_count": new_count,
            "visible": visible_flag
        })

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"RTD 투표 실패: {e}")
        raise HTTPException(status_code=500, detail="RTD 투표 실패")
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
def delete_user_report(data: DeleteReportRequest = Body(...)):
    from cassandra.query import BatchStatement, SimpleStatement
    try:
        # 1. 삭제할 제보의 모든 필드 조회
        query = """
            SELECT report_id, report_by_id, report_at, delete_vote, middle_type, small_type,
                   report_content, report_lat, report_location, report_lot, visible, vote_user_ids
            FROM user_report
            WHERE report_id = %s ALLOW FILTERING
        """
        report_to_delete = session.execute(query, (data.report_id,)).one()

        if not report_to_delete:
            raise HTTPException(status_code=404, detail="해당 제보를 찾을 수 없습니다.")

        # 2. 작성자 확인
        if report_to_delete.report_by_id != data.user_id:
            raise HTTPException(status_code=403, detail="해당 제보의 작성자가 아닙니다.")

        # 3. 배치 생성
        batch = BatchStatement()
        deleted_at = datetime.utcnow()

        # 4. deleted_user_report 테이블에 삽입
        insert_deleted_query = """
            INSERT INTO deleted_user_report (
                report_id, deleted_at, report_by_id, report_at, delete_vote, middle_type,
                report_content, report_lat, report_location, report_lot, small_type,
                visible, vote_user_ids, deleted_by_user_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        batch.add(SimpleStatement(insert_deleted_query), (
            report_to_delete.report_id,
            deleted_at,
            report_to_delete.report_by_id,
            report_to_delete.report_at,
            report_to_delete.delete_vote,
            report_to_delete.middle_type,
            report_to_delete.report_content,
            report_to_delete.report_lat,
            report_to_delete.report_location,
            report_to_delete.report_lot,
            report_to_delete.small_type,
            report_to_delete.visible,
            report_to_delete.vote_user_ids,
            data.user_id # deleted_by_user_id
        ))

        # 5. user_report 테이블에서 삭제
        delete_original_query = "DELETE FROM user_report WHERE report_by_id = %s AND report_at = %s"
        batch.add(SimpleStatement(delete_original_query), (report_to_delete.report_by_id, report_to_delete.report_at))

        # 6. 배치 실행
        session.execute(batch)

        return {"message": "제보가 성공적으로 삭제(아카이빙)되었습니다."}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"제보 삭제(아카이빙) 실패: {e}")
        raise HTTPException(status_code=500, detail="제보 삭제(아카이빙) 실패")
@app.get("/rtd/search")
def search_rtd(
    rtd_loc: Optional[str] = None,
    regioncode: Optional[int] = None,
    rtd_code: Optional[int] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    days: Optional[int] = 1,
    sort: Optional[str] = Query("desc", description="정렬 순서: asc 또는 desc")
):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    try:
        if from_time and to_time:
            start_time = datetime.fromisoformat(from_time).astimezone(timezone.utc)
            end_time = datetime.fromisoformat(to_time).astimezone(timezone.utc)
        else:
            end_time = now + timedelta(seconds=1)
            start_time = now - timedelta(days=days)

        logging.info(f"RTD Search - Querying from {start_time} to {end_time}")

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
            # rtd_db에서 최신 visible 상태 조회
            rtd_db_query = "SELECT visible FROM rtd_db WHERE rtd_time = %s AND id = %s"
            rtd_db_row = session.execute(rtd_db_query, (row.rtd_time, row.id)).one()

            current_visible = True # 기본값은 True
            if rtd_db_row and hasattr(rtd_db_row, 'visible'):
                current_visible = rtd_db_row.visible

            rtd_results.append({
                "type": "rtd",
                "id": str(row.id),
                "time": row.rtd_time.isoformat() if row.rtd_time else None,
                "rtd_loc": row.rtd_loc,
                "rtd_details": row.rtd_details,
                "rtd_code": row.rtd_code,
                "regioncode": getattr(row, 'regioncode', None),
                "latitude": getattr(row, 'latitude', None),
                "longitude": getattr(row, 'longitude', None),
                "vote_count": getattr(row, 'vote_count', 0),
                "visible": current_visible, # rtd_db에서 조회한 visible 값 사용
            })

        # === 2) user_report 테이블 조회 ===
        report_query = """
            SELECT * FROM user_report_by_time
            WHERE report_at >= %s AND report_at <= %s
            ALLOW FILTERING
        """
        report_rows = session.execute(report_query, (start_time, end_time))

        for row in report_rows:
            report_results.append({
                "type": "report",
                "id": str(row.report_id),
                "time": row.report_at.isoformat() if row.report_at else None,
                "report_location": row.report_location,
                "middle_type": row.middle_type,
                "small_type": row.small_type,
                "content": row.report_content,
                "report_by": row.report_by_id,
                "latitude": row.report_lat,
                "longitude": row.report_lot,
                "visible": row.visible,
                "delete_vote": row.delete_vote
            })

        # === 3) 통합 정렬 ===
        merged_results = rtd_results + report_results

        sorted_results = sorted(
            merged_results,
            key=lambda x: x.get("time"),
            reverse=(sort != "asc")  # asc일 때만 오름차순, 나머지는 최신순
        )

        return JSONResponse(content={
            "count": len(sorted_results),
            "results": sorted_results
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
    logging.info(f"device register data: {data}")
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

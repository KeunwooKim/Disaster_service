import logging
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from ner_utils import extract_locations
from main import geocoding, get_regioncode  # 실제 경로로

logging.basicConfig(level=logging.INFO)

# Cassandra 접속
auth    = PlainTextAuthProvider(username="andy013", password="1212")
cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth)
session = cluster.connect("disaster_service")

select_q = SimpleStatement(
    "SELECT rtd_time, id, rtd_loc, rtd_details FROM rtd_db "
    "WHERE rtd_code = 21 ALLOW FILTERING"
)
rows = session.execute(select_q)

update_q = session.prepare("""
    UPDATE rtd_db
       SET rtd_loc    = ?,
           regioncode = ?,
           latitude   = ?,
           longitude  = ?
     WHERE rtd_time  = ?
       AND id        = ?
""")

for row in rows:
    record_id = row.id
    rtd_time  = row.rtd_time
    orig_loc  = row.rtd_loc
    details   = row.rtd_details

    # 기본값 할당
    loc = orig_loc

    # content 파싱
    content = next((d.split("content:",1)[1].strip()
                    for d in details if d.startswith("content:")), None)
    if not content:
        logging.warning(f"[{record_id}] content 필드 누락, 스킵")
        continue

    # 지역 추출
    regions = extract_locations(content)
    if regions:
        loc = regions[0]
        logging.info(f"[{record_id}] 새로 뽑은 지역: {loc}")
    else:
        logging.warning(f"[{record_id}] 재추출 실패 → 기존 loc 사용: {loc}")

    # geocode & regioncode
    geo       = geocoding(loc)
    lat       = float(geo.get("lat")) if geo.get("lat") else None
    lon       = float(geo.get("lng")) if geo.get("lng") else None
    region_cd = get_regioncode(loc)

    # 업데이트
    try:
        session.execute(
            update_q,
            (loc, region_cd, lat, lon, rtd_time, record_id)
        )
        logging.info(f"[{record_id}] 업데이트 성공")
    except Exception as e:
        logging.error(f"[{record_id}] UPDATE 중 오류: {e}")


logging.info("업데이트 작업 전체 완료")

session.shutdown()
cluster.shutdown()

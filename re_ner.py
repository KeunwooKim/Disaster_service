import logging
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from ner_utils import extracted_regions
from main import geocoding, get_regioncode  # 실제 모듈 경로로 조정하세요

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cassandra 접속 설정
auth = PlainTextAuthProvider(username="andy013", password="1212")
cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth)
session = cluster.connect("disaster_service")

# 모든 메시지 레코드 조회
select_q = SimpleStatement(
    "SELECT rtd_time, id, rtd_details FROM rtd_db "
    "WHERE rtd_code = 21 ALLOW FILTERING"
)
rows = session.execute(select_q)

# 업데이트 쿼리 준비
update_q = session.prepare(
    """
    UPDATE rtd_db
       SET rtd_loc = ?, regioncode = ?, latitude = ?, longitude = ?
     WHERE rtd_time = ? AND id = ?
    """
)

count = 0
for row in rows:
    record_id = row.id
    rtd_time = row.rtd_time
    details = row.rtd_details

    # content 파싱
    content = next(
        (d.split("content:",1)[1].strip() for d in details if d.startswith("content:")),
        None
    )
    if not content:
        logging.warning(f"[{record_id}] content 누락, 스킵")
        continue

    # 모든 메시지에 대해 지명 재추출
    regions = extracted_regions(content)
    if not regions:
        logging.warning(f"[{record_id}] 추출된 지명 없음, 스킵")
        continue

    # rtd_loc 필드에 모든 지명 쉼표로 연결
    combined_loc = ", ".join(regions)
    # 대표 지명
    primary_loc = regions[0]

    # geocode & regioncode
    geo = geocoding(primary_loc)
    lat = float(geo.get("lat")) if geo.get("lat") else None
    lon = float(geo.get("lng")) if geo.get("lng") else None
    region_cd = get_regioncode(primary_loc)

    # DB 업데이트
    try:
        session.execute(
            update_q,
            (combined_loc, region_cd, lat, lon, rtd_time, record_id)
        )
        count += 1
        logging.info(f"[{record_id}] 업데이트 완료: {combined_loc}")
        print(f"[{record_id}] 저장 완료: {combined_loc}")  # ← 추가된 출력문
    except Exception as e:
        logging.error(f"[{record_id}] 업데이트 오류: {e}")

# 최종 업데이트 개수 출력
logging.info(f"총 {count}건 업데이트 완료.")
print(f"총 {count}건 업데이트 완료.")  # ← 추가된 출력문

session.shutdown()
cluster.shutdown()

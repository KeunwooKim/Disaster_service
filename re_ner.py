import logging
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from ner_utils import extracted_regions
from main import geocoding, get_regioncode  # 실제 모듈 경로로 조정하세요

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Cassandra 접속
auth = PlainTextAuthProvider(username="andy013", password="1212")
cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth)
session = cluster.connect("disaster_service")

# 전체 레코드 조회
select_q = SimpleStatement(
    "SELECT rtd_time, id, rtd_details FROM rtd_db "
    "WHERE rtd_code = 21 ALLOW FILTERING"
)
rows = session.execute(select_q)

# UPDATE 문 준비
# 1) 지명 있을 때: rtd_loc 포함 모두 업데이트
update_full = session.prepare("""
    UPDATE rtd_db
       SET rtd_loc    = ?,
           regioncode = ?,
           latitude   = ?,
           longitude  = ?
     WHERE rtd_time   = ?
       AND id         = ?
""")
# 2) 지명 없을 때: regioncode, latitude, longitude만 None 으로 덮어쓰기
update_null = session.prepare("""
    UPDATE rtd_db
       SET regioncode = ?,
           latitude   = ?,
           longitude  = ?
     WHERE rtd_time   = ?
       AND id         = ?
""")

count = 0
for row in rows:
    record_id = row.id
    rtd_time  = row.rtd_time
    # details 에서 content 추출
    content = next((d.split("content:",1)[1].strip()
                    for d in row.rtd_details
                    if d.startswith("content:")),
                   None)

    if not content:
        logging.warning(f"[{record_id}] content 누락, regioncode 등만 None으로 업데이트")
        # content 자체가 없어도, 지명 칼럼들만 None 처리
        session.execute(update_null, (None, None, None, rtd_time, record_id))
        count += 1
        continue

    regions = extracted_regions(content)
    if regions:
        # 지명 추출 성공
        combined_loc = ", ".join(regions)
        primary_loc  = regions[0]

        # 위경도 / 행정코드 조회
        geo = geocoding(primary_loc)
        lat = float(geo.get("lat")) if geo.get("lat") else None
        lon = float(geo.get("lng")) if geo.get("lng") else None
        region_cd = get_regioncode(primary_loc)

        # 모든 칼럼 업데이트
        session.execute(
            update_full,
            (combined_loc, region_cd, lat, lon, rtd_time, record_id)
        )
        logging.info(f"[{record_id}] 업데이트 완료: {combined_loc}")
    else:
        # 지명 미추출 → regioncode, 위경도만 None
        logging.warning(f"[{record_id}] 지명 미추출, regioncode 등만 None으로 업데이트")
        session.execute(
            update_null,
            (None, None, None, rtd_time, record_id)
        )

    count += 1

logging.info(f"총 {count}건 업데이트 완료.")
print(f"총 {count}건 업데이트 완료.")

session.shutdown()
cluster.shutdown()

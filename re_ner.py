import logging
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from ner_utils import extracted_regions
from main import geocoding, get_regioncode
from address_utils import extract_best_address

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

auth = PlainTextAuthProvider(username="andy013", password="1212")
cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth)
session = cluster.connect("disaster_service")

select_q = SimpleStatement(
    "SELECT rtd_time, id, rtd_details FROM rtd_db "
    "WHERE rtd_code = 21 ALLOW FILTERING"
)
rows = session.execute(select_q)

update_full = session.prepare("""
    UPDATE rtd_db
       SET rtd_loc    = ?,
           regioncode = ?,
           latitude   = ?,
           longitude  = ?
     WHERE rtd_time   = ?
       AND id         = ?
""")
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

    # 1) content 추출
    content = next((d.split("content:",1)[1].strip()
                    for d in row.rtd_details
                    if d.startswith("content:")),
                   None)

    if not content:
        logging.warning(f"[{record_id}] content 누락 → 좌표만 None 업데이트")
        session.execute(update_null, (None, None, None, rtd_time, record_id))
        count += 1
        continue

    # 2) 상세주소 우선 추출 시도
    best_addr = extract_best_address(content)
    if best_addr:
        logging.info(f"[{record_id}] 문장: {content!r}")
        logging.info(f"[{record_id}] 상세주소 추출됨: {best_addr!r}")

        geo = geocoding(best_addr)
        lat = float(geo.get("lat")) if geo.get("lat") else None
        lon = float(geo.get("lng")) if geo.get("lng") else None
        region_cd = get_regioncode(best_addr)
        rtd_loc = best_addr

        session.execute(
            update_full,
            (rtd_loc, region_cd, lat, lon, rtd_time, record_id)
        )
        logging.info(f"[{record_id}] 업데이트 완료: {rtd_loc}")

    else:
        # 3) fallback: ner_utils.extracted_regions
        regions = extracted_regions(content)
        if regions:
            logging.info(f"[{record_id}] 문장: {content!r}")
            logging.info(f"[{record_id}] extracted_regions: {regions}")

            primary_loc = regions[0]
            geo = geocoding(primary_loc)
            lat = float(geo.get("lat")) if geo.get("lat") else None
            lon = float(geo.get("lng")) if geo.get("lng") else None
            region_cd = get_regioncode(primary_loc)

            session.execute(
                update_full,
                (primary_loc, region_cd, lat, lon, rtd_time, record_id)
            )
            logging.info(f"[{record_id}] fallback 업데이트: {primary_loc}")

        else:
            logging.warning(f"[{record_id}] 지명 미추출 → 좌표만 None 업데이트")
            session.execute(
                update_null,
                (None, None, None, rtd_time, record_id)
            )

    count += 1

logging.info(f"총 {count}건 업데이트 완료.")
print(f"총 {count}건 업데이트 완료.")

session.shutdown()
cluster.shutdown()

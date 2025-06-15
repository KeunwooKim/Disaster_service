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

# 1) SELECT할 때 rtd_time 추가
select_q = SimpleStatement(
    "SELECT id, rtd_time, rtd_loc, rtd_details "
    "FROM rtd_db WHERE rtd_code = 21 ALLOW FILTERING"
)
rows = session.execute(select_q)

# 2) UPDATE 준비: WHERE에 rtd_time 포함
update_q = session.prepare("""
    UPDATE rtd_db
       SET rtd_loc    = ?,
           regioncode = ?,
           latitude   = ?,
           longitude  = ?
     WHERE id        = ?
       AND rtd_time  = ?
""")

for row in rows:
    record_id = row.id
    rtd_time  = row.rtd_time     # ← 여기서 가져온 rtd_time
    orig_loc  = row.rtd_loc
    details   = row.rtd_details

    # … (extract_locations, geocoding 등 생략) …

    # 3) UPDATE 실행 시 rtd_time 추가
    try:
        session.execute(
            update_q,
            (loc, region_cd, lat, lon, record_id, rtd_time)
        )
        logging.info(f"[{record_id}] 업데이트 성공")
    except Exception as e:
        logging.error(f"[{record_id}] UPDATE 중 오류: {e}")


logging.info("업데이트 작업 전체 완료")

session.shutdown()
cluster.shutdown()

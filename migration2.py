from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Cassandra 접속 정보 (직접 입력)
CASSANDRA_HOST = '127.0.0.1'
CASSANDRA_PORT = 9042
CASSANDRA_USER = 'andy013'
CASSANDRA_PASS = '1212'
KEYSPACE = 'disaster_service'

# Cassandra 연결
auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)

print("✅ Cassandra 연결 성공")

# rtd_db_new → rtd_db 복사 시작
rows = session.execute("SELECT * FROM rtd_db_new")
count = 0

for row in rows:
    try:
        session.execute("""
            INSERT INTO rtd_db (
                id, rtd_time, rtd_loc, rtd_details,
                rtd_code, regioncode, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row.id, row.rtd_time, row.rtd_loc, row.rtd_details,
            row.rtd_code, row.regioncode, row.latitude, row.longitude
        ))
        count += 1
    except Exception as e:
        print(f"❌ INSERT 실패 (ID: {row.id}): {e}")

print(f"✅ 총 {count}건 마이그레이션 완료 (rtd_db_new → rtd_db)")

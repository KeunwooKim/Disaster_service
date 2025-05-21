from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

KEYSPACE = 'disaster_service'
TABLE = 'rtd_db'

# Cassandra 연결 정보
auth_provider = PlainTextAuthProvider(username='andy013', password='1212')
cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
session = cluster.connect(KEYSPACE)

# rtd_code가 33인 레코드 삭제
def delete_rtd_code_33():
    # 먼저 삭제할 레코드의 id와 rtd_time을 조회
    select_query = f"""
    SELECT rtd_time, id FROM {TABLE} WHERE rtd_code = 33 ALLOW FILTERING;
    """
    rows = session.execute(select_query)

    delete_stmt = session.prepare(f"""
    DELETE FROM {TABLE} WHERE rtd_code = ? AND rtd_time = ? AND id = ?
    """)

    for row in rows:
        try:
            session.execute(delete_stmt, (33, row.rtd_time, row.id))
            print(f"Deleted record with rtd_time: {row.rtd_time} and id: {row.id}")
        except Exception as e:
            print(f"Error deleting record: {e}")


if __name__ == "__main__":
    delete_rtd_code_33()
    session.shutdown()
    cluster.shutdown()

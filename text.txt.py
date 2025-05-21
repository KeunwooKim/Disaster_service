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

    # 각 레코드에 대해 삭제 쿼리 실행
    for row in rows:
        rtd_time = row.rtd_time.strftime('%Y-%m-%d %H:%M:%S')
        record_id = row.id
        delete_query = f"""
        DELETE FROM {TABLE} WHERE rtd_code = 33 AND rtd_time = '{rtd_time}' AND id = {record_id};
        """
        try:
            session.execute(delete_query)
            print(f"Deleted record with rtd_time: {rtd_time} and id: {record_id}")
        except Exception as e:
            print(f"Error deleting record: {e}")

    print("Deletion process completed.")

if __name__ == "__main__":
    delete_rtd_code_33()
    session.shutdown()
    cluster.shutdown()

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import json
import os

# Cassandra 접속 정보
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']  # 노드 IP 리스트
CASSANDRA_PORT = 9042
KEYSPACE = 'disaster_service'
TABLE = 'rtd_db'

# 환경변수에서 사용자/비밀번호 읽기
CASSANDRA_USER = os.getenv('CASSANDRA_USER', 'andy013')
CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD', '1212')

# 백업 파일 경로
BACKUP_DIR = './backup'
BACKUP_FILE = os.path.join(BACKUP_DIR, f'{TABLE}.csv')

def backup_table_to_csv():
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # 인증 제공자 설정
    auth_provider = PlainTextAuthProvider(
        username=CASSANDRA_USER,
        password=CASSANDRA_PASSWORD
    )
    cluster = Cluster(
        contact_points=CASSANDRA_CONTACT_POINTS,
        port=CASSANDRA_PORT,
        auth_provider=auth_provider
    )
    session = cluster.connect(KEYSPACE)

    # 전체 데이터 조회
    rows = session.execute(f"SELECT * FROM {TABLE}")

    # CSV로 쓰기
    with open(BACKUP_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'rtd_code', 'rtd_time', 'id',
            'latitude', 'longitude', 'regioncode',
            'rtd_loc', 'rtd_details'
        ])
        for row in rows:
            writer.writerow([
                row.rtd_code,
                row.rtd_time,
                row.id,
                row.latitude,
                row.longitude,
                row.regioncode,
                row.rtd_loc,
                json.dumps(row.rtd_details, ensure_ascii=False)
            ])

    print(f'백업 완료: {BACKUP_FILE}')

    session.shutdown()
    cluster.shutdown()

if __name__ == '__main__':
    backup_table_to_csv()

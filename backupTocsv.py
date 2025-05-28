from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
import json
import os

# Cassandra 접속 정보
CASSANDRA_CONTACT_POINTS = ['127.0.0.1']  # Cassandra 노드 IP 리스트
CASSANDRA_PORT = 9042
KEYSPACE = 'disaster_service'
TABLE = 'rtd_db'

# 백업 파일 경로
BACKUP_DIR = './backup'
BACKUP_FILE = os.path.join(BACKUP_DIR, f'{TABLE}.csv')

def backup_table_to_csv():
    # 백업 디렉토리 생성
    os.makedirs(BACKUP_DIR, exist_ok=True)

    # Cluster 및 Session 생성 (인증이 필요한 경우, PlainTextAuthProvider 활용)
    # auth_provider = PlainTextAuthProvider(username='YOUR_USER', password='YOUR_PASS')
    # cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT, auth_provider=auth_provider)
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    # SELECT 쿼리 실행
    query = f"SELECT * FROM {TABLE}"
    rows = session.execute(query)

    # CSV 파일 쓰기
    with open(BACKUP_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # 헤더 작성
        writer.writerow([
            'rtd_code', 'rtd_time', 'id',
            'latitude', 'longitude', 'regioncode',
            'rtd_loc', 'rtd_details'
        ])

        # 각 행을 순회하며 CSV에 기록
        for row in rows:
            writer.writerow([
                row.rtd_code,
                row.rtd_time,               # pandas로 읽을 때 자동 파싱됨
                row.id,
                row.latitude,
                row.longitude,
                row.regioncode,
                row.rtd_loc,
                json.dumps(row.rtd_details, ensure_ascii=False)  # 리스트를 JSON 문자열로 저장
            ])

    print(f'백업 완료: {BACKUP_FILE}')

    # 테이블 초기화
    session.execute(f"TRUNCATE {TABLE}")
    print(f'테이블 초기화 완료: {KEYSPACE}.{TABLE}')

    # 리소스 해제
    session.shutdown()
    cluster.shutdown()

if __name__ == '__main__':
    backup_table_to_csv()

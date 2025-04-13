import os
import sys
import logging
import itertools
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CassandraConnector:
    """
    Cassandra 데이터베이스에 연결하는 클래스입니다.
    keyspace 및 접속정보(username, password, host, port)를 환경에 맞게 수정하세요.
    """
    def __init__(self, keyspace="disaster_service"):
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.connect()

    def connect(self):
        try:
            auth_provider = PlainTextAuthProvider(username="andy013", password="1212")
            self.cluster = Cluster(["127.0.0.1"], port=9042, auth_provider=auth_provider)
            self.session = self.cluster.connect(self.keyspace)
            logging.info("Cassandra DB에 연결되었습니다.")
        except Exception as e:
            logging.error(f"Cassandra 접속 오류: {e}")
            sys.exit(1)

def get_all_tables(connector):
    """
    system_schema를 통해 현재 keyspace 내의 모든 테이블 이름을 조회합니다.
    """
    try:
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s;"
        rows = connector.session.execute(query, (connector.keyspace,))
        tables = [row.table_name for row in rows]
        return tables
    except Exception as e:
        logging.error(f"테이블 목록 조회 오류: {e}")
        return []

def interactive_view_table(connector, table_name, page_size=50):
    """
    지정된 테이블의 데이터를 페이지 단위로 조회하는 함수입니다.
    - page_size: 한 페이지에 보여줄 행 수 (기본값 50)
    사용자가:
      - 'n'을 입력하면 다음 페이지,
      - 'b'를 입력하면 이전 페이지,
      - '?'를 입력하면 현재 keyspace 내의 테이블 목록을 출력,
      - 'q'를 입력하면 페이지 조회를 종료합니다.
    """
    query = f"SELECT * FROM {table_name};"
    stmt = SimpleStatement(query, fetch_size=page_size)
    result_set = connector.session.execute(stmt)
    # iterator를 이용해 페이지 단위로 가져옴
    iterator = iter(result_set)
    pages = []
    first_page = list(itertools.islice(iterator, page_size))
    if not first_page:
        print("해당 테이블에 데이터가 없습니다.")
        return
    pages.append(first_page)
    current_page_index = 0

    while True:
        print("\n" + "=" * 50)
        print(f"테이블: {table_name} - 페이지 {current_page_index + 1} (행 {len(pages[current_page_index])}개)")
        print("=" * 50)
        for row in pages[current_page_index]:
            print(row)
        print("\n명령: n (다음 페이지), b (이전 페이지), ? (테이블 목록 보기), q (종료)")
        cmd = input("명령을 입력하세요: ").strip().lower()

        if cmd == "n":
            next_page = list(itertools.islice(iterator, page_size))
            if next_page:
                pages.append(next_page)
                current_page_index += 1
            else:
                print("다음 페이지가 없습니다.")
        elif cmd == "b":
            if current_page_index > 0:
                current_page_index -= 1
            else:
                print("이전 페이지가 없습니다.")
        elif cmd == "q":
            break
        elif cmd == "?":
            tables = get_all_tables(connector)
            if tables:
                print("\n현재 keyspace에 있는 테이블 목록:")
                for tbl in tables:
                    print(" -", tbl)
            else:
                print("테이블 목록을 가져올 수 없습니다.")
        else:
            print("알 수 없는 명령입니다. 다시 입력하세요.")

def main():
    connector = CassandraConnector()
    tables = get_all_tables(connector)

    if not tables:
        print("현재 keyspace에 조회 가능한 테이블이 없습니다.")
        return

    # 메인 메뉴: 테이블 목록 출력
    print("현재 keyspace('{}')에 존재하는 테이블 목록:".format(connector.keyspace))
    for table in tables:
        print(" - " + table)

    print("\n조회할 테이블명을 입력하세요. (종료하려면 'q' 입력)")
    while True:
        table_name = input("테이블명: ").strip()
        if table_name.lower() == 'q':
            print("프로그램을 종료합니다.")
            break
        if table_name not in tables:
            print(f"'{table_name}' 테이블은 존재하지 않습니다. 다시 입력해주세요.")
            continue
        # 해당 테이블을 인터랙티브하게 페이지 단위로 조회
        interactive_view_table(connector, table_name)
        print("\n다른 테이블을 조회하려면 테이블명을 입력하세요. (종료하려면 'q' 입력)")

if __name__ == "__main__":
    main()

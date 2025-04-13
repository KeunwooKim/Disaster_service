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


def get_next_page(iterator, page_size):
    """
    지정된 iterator에서 page_size 만큼의 row를 가져옵니다.
    """
    next_page = []
    for _ in range(page_size):
        try:
            next_page.append(next(iterator))
        except StopIteration:
            break
    return next_page


def interactive_view_table(connector, table_name, page_size=50):
    """
    지정된 테이블의 데이터를 페이지 단위로 조회하는 함수입니다.
    - page_size: 한 페이지에 보여줄 행 수 (기본값 50)
    사용자가:
      - 'n'을 입력하면 다음 페이지,
      - 'b'를 입력하면 이전 페이지,
      - 'f'를 입력하면 현재 페이지 내에서 검색,
      - '?'를 입력하면 현재 keyspace 내의 테이블 목록을 출력,
      - 'q'를 입력하면 페이지 조회를 종료합니다.
    """
    query = f"SELECT * FROM {table_name};"
    stmt = SimpleStatement(query, fetch_size=page_size)
    result_set = connector.session.execute(stmt)
    iterator = iter(result_set)

    pages = []
    first_page = get_next_page(iterator, page_size)
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
        print("\n명령: n (다음 페이지), b (이전 페이지), f (현재 페이지 검색), ? (테이블 목록 보기), q (종료)")
        cmd = input("명령을 입력하세요: ").strip().lower()

        if cmd == "n":
            next_page = get_next_page(iterator, page_size)
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
        elif cmd == "f":
            search_term = input("검색어: ").strip().lower()
            # 현재 페이지의 각 행을 문자열로 변환하여 검색어 포함 여부를 확인합니다.
            filtered = [row for row in pages[current_page_index] if search_term in str(row).lower()]
            if filtered:
                print("\n=== 검색 결과 ===")
                for row in filtered:
                    print(row)
                print("================\n")
            else:
                print("현재 페이지에서 검색 결과가 없습니다.")
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


def display_tables(tables):
    """
    테이블 목록을 번호와 함께 출력합니다.
    """
    print("\n현재 keyspace에 존재하는 테이블 목록:")
    for idx, table in enumerate(tables):
        print(f"{idx + 1}. {table}")


def main():
    connector = CassandraConnector()
    all_tables = get_all_tables(connector)

    if not all_tables:
        print("현재 keyspace에 조회 가능한 테이블이 없습니다.")
        return

    current_tables = all_tables.copy()
    display_tables(current_tables)

    print("\n조회할 테이블을 선택하세요.")
    print(" - 테이블명 또는 번호를 입력")
    print(" - 검색하려면 's' 입력")
    print(" - 테이블 목록 보려면 '?' 입력")
    print(" - 종료하려면 'q' 입력")

    while True:
        user_input = input("선택: ").strip()
        if user_input.lower() == 'q':
            print("프로그램을 종료합니다.")
            break
        elif user_input.lower() == '?':
            display_tables(current_tables)
            continue
        elif user_input.lower() == 's':
            search_query = input("검색어: ").strip().lower()
            filtered_tables = [tbl for tbl in all_tables if search_query in tbl.lower()]
            if not filtered_tables:
                print("검색 결과가 없습니다.")
                continue
            current_tables = filtered_tables
            display_tables(current_tables)
            continue

        # 번호 입력인 경우
        if user_input.isdigit():
            index = int(user_input) - 1
            if index < 0 or index >= len(current_tables):
                print("유효한 번호를 입력하세요.")
                continue
            table_name = current_tables[index]
        else:
            table_name = user_input
            if table_name not in all_tables:
                print(f"'{table_name}' 테이블은 존재하지 않습니다. 다시 입력해주세요.")
                continue

        # 지정된 테이블을 페이지 단위로 조회
        interactive_view_table(connector, table_name)
        # 조회 후 전체 테이블 목록으로 복구
        current_tables = all_tables.copy()
        display_tables(current_tables)
        print("\n다른 테이블을 조회하려면 테이블명 또는 번호를, 검색하려면 's', 테이블 목록 보려면 '?' , 종료하려면 'q'를 입력하세요.")


if __name__ == "__main__":
    main()

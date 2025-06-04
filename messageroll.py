# migrate_disaster_messages.py

import logging
from cassandra.query import SimpleStatement
from ner_utils import extract_location
from main import connector, get_regioncode, geocoding, insert_rtd_data
from tqdm import tqdm  # 진행 바를 위해 tqdm 사용

def migrate_disaster_messages_to_rtd():
    """
    disaster_message 테이블의 모든 레코드를 읽어서
    extract_location → get_regioncode → geocoding → insert_rtd_data(21, …) 로 rtd_db에 저장
    진행 상황을 tqdm 진행 바를 사용해 표시합니다.
    """
    session = connector.session

    # 전체 행 수를 먼저 조회하여 tqdm에 전달할 total로 사용
    count_query = SimpleStatement("SELECT COUNT(*) FROM disaster_service.disaster_message")
    count_result = session.execute(count_query).one()
    total_rows = count_result.count if count_result and hasattr(count_result, 'count') else None

    # 모든 레코드 조회
    query = SimpleStatement(
        "SELECT message_id, emergency_level, DM_ntype, issued_at, issuing_agency, message_content "
        "FROM disaster_service.disaster_message"
    )
    rows = session.execute(query)

    count_total = 0
    count_inserted = 0

    # tqdm 으로 진행 바 적용 (total_rows가 None인 경우, total 인자를 생략하여 무한 진행 바로 동작)
    iterator = tqdm(rows, total=total_rows, desc="Migrating messages", unit="msg")

    for row in iterator:
        count_total += 1
        try:
            # (A) 행에서 필요한 필드 꺼내기
            message_id      = row.message_id
            emergency_level = row.emergency_level
            dm_ntype        = row.DM_ntype
            issued_at       = row.issued_at       # Cassandra timestamp → datetime
            issuing_agency  = row.issuing_agency
            content         = row.message_content

            # (B) NER 모델로 메시지 내용에서 지역 추출
            extracted_region = extract_location(content)
            logging.info(f"[메시지 ID {message_id}] 추출된 지역: '{extracted_region}'")

            # (C) 지역이 있으면 코드/좌표 조회, 없으면 None/빈 문자열 처리
            if extracted_region:
                region_cd = get_regioncode(extracted_region)
                coords    = geocoding(extracted_region)
                lat = float(coords["lat"]) if coords.get("lat") else None
                lng = float(coords["lng"]) if coords.get("lng") else None
                rtd_loc = extracted_region
            else:
                region_cd = None
                lat = None
                lng = None
                rtd_loc = ""  # 빈 문자열

            # (D) rtd_details 구성: 기존 상세 정보와 동일하게 level, type, content
            rtd_details = [
                f"level: {emergency_level}",
                f"type: {dm_ntype}",
                f"content: {content}"
            ]

            # (E) insert_rtd_data 호출 (문자코드 = 21)
            insert_rtd_data(
                21,          # rtd_code: 재난문자 전용
                issued_at,   # 발송 시각(datetime)
                rtd_loc,     # 추출된 지역명(없으면 "")
                rtd_details, # 상세 정보 리스트
                region_cd,   # 행정구역 코드 or None
                lat,         # 위도 or None
                lng          # 경도 or None
            )
            count_inserted += 1

        except Exception as e:
            logging.error(f"[메시지 ID {message_id}] RTD 저장 실패: {e}")

    logging.info(f"[migration 완료] 전체 메시지 수: {count_total}, RTD에 저장된 수: {count_inserted}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    migrate_disaster_messages_to_rtd()

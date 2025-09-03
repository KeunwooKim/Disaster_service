import firebase_admin
from firebase_admin import credentials, messaging
import os
import logging
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Cassandra Connection ---
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "andy013")
CASSANDRA_PASS = os.getenv("CASSANDRA_PASS", "1212")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "disaster_service")

session = None
try:
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
    )
    session = cluster.connect(KEYSPACE)
    logging.info("FCM Sender: Cassandra 연결 성공")
except Exception as e:
    logging.error(f"FCM Sender: Cassandra 연결 실패: {e}")

# --- Firebase Initialization ---
try:
    cred_path = os.getenv("FIREBASE_CRED_PATH")
    if not cred_path or not os.path.exists(cred_path):
        raise ValueError(f"Firebase 인증 파일을 찾을 수 없거나 FIREBASE_CRED_PATH 환경 변수가 설정되지 않았습니다: {cred_path}")

    cred = credentials.Certificate(cred_path)
    # 이미 초기화된 앱이 있는지 확인
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
        logging.info("FCM Sender: Firebase 앱 초기화 성공")
    else:
        logging.info("FCM Sender: Firebase 앱이 이미 초기화되어 있습니다.")
except Exception as e:
    logging.critical(f"FCM Sender: Firebase 앱 초기화 실패: {e}")
    raise e

def send_broadcast_data_message(data_payload: dict):
    """
    Sends a data message to all registered devices.
    """
    if not session:
        logging.error("Cassandra 세션이 없어 FCM을 보낼 수 없습니다.")
        return

    try:
        # Get all device tokens from the database
        device_tokens_query = "SELECT device_token FROM user_device"
        device_tokens_rows = session.execute(device_tokens_query)
        tokens = [row.device_token for row in device_tokens_rows if row.device_token]

        if not tokens:
            logging.warning("알림을 보낼 등록된 디바이스 토큰이 없습니다.")
            return

        logging.info(f"총 {len(tokens)}개의 모든 디바이스에 데이터 메시지를 전송합니다.")
        logging.info(f"전송할 데이터 페이로드: {data_payload}")

        # Send the data message to all tokens in chunks
        for i in range(0, len(tokens), 500):
            chunk = tokens[i:i + 500]
            message = messaging.MulticastMessage(
                data=data_payload,
                tokens=chunk,
            )
            response = messaging.send_each_for_multicast(message)
            logging.info(f"FCM 데이터 메시지 전송 ({i+1}-{i+len(chunk)}): {response.success_count} 성공, {response.failure_count} 실패")

            if response.failure_count > 0:
                failed_tokens = []
                for idx, resp in enumerate(response.responses):
                    if not resp.success:
                        failed_tokens.append(chunk[idx])
                logging.warning(f"실패한 토큰: {failed_tokens}")

    except Exception as e:
        logging.error(f"데이터 메시지 전송 중 오류 발생: {e}")

def send_data_message_to_tokens(tokens: list, data_payload: dict):
    """
    Sends a data message to a specific list of device tokens.
    """
    if not tokens:
        logging.warning("전송할 대상 토큰이 없습니다.")
        return

    try:
        logging.info(f"총 {len(tokens)}개의 특정 디바이스에 데이터 메시지를 전송합니다.")
        logging.info(f"전송할 데이터 페이로드: {data_payload}")

        # Send the data message to the specified tokens in chunks
        for i in range(0, len(tokens), 500):
            chunk = tokens[i:i + 500]
            message = messaging.MulticastMessage(
                data=data_payload,
                tokens=chunk,
            )
            response = messaging.send_each_for_multicast(message)
            logging.info(f"FCM 데이터 메시지 전송 ({i+1}-{i+len(chunk)}): {response.success_count} 성공, {response.failure_count} 실패")

            if response.failure_count > 0:
                failed_tokens = []
                for idx, resp in enumerate(response.responses):
                    if not resp.success:
                        failed_tokens.append(chunk[idx])
                logging.warning(f"실패한 토큰: {failed_tokens}")

    except Exception as e:
        logging.error(f"특정 토큰에 데이터 메시지 전송 중 오류 발생: {e}")

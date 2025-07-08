import firebase_admin
from firebase_admin import credentials
from firebase_admin import messaging
import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 환경 변수 로드 (.env 파일 활용)
load_dotenv()
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "andy013")
CASSANDRA_PASS = os.getenv("CASSANDRA_PASS", "1212")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "disaster_service")

# Cassandra 연결 설정
try:
    cluster = Cluster(
        [CASSANDRA_HOST],
        port=CASSANDRA_PORT,
        auth_provider=PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
    )
    session = cluster.connect(KEYSPACE)
    logging.info("Cassandra 연결 성공")
except Exception as e:
    logging.error(f"Cassandra 연결 실패: {e}")
    exit()

# Firebase 서비스 계정 키 파일 경로
# 이 경로는 Firebase 프로젝트에서 다운로드한 JSON 파일의 경로여야 합니다.
cred_path = "/root/Project_disaster/disasteralert-bc125-firebase-adminsdk-fbsvc-808e3fc9d3.json"

# 파일 존재 여부 확인
if not os.path.exists(cred_path):
    logging.error(f"오류: Firebase 서비스 계정 키 파일이 존재하지 않습니다: {cred_path}")
    logging.error("경로를 확인하거나 올바른 파일을 다운로드하여 해당 위치에 놓으세요.")
    exit()

try:
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    logging.info("Firebase 앱 초기화 성공")
except Exception as e:
    logging.error(f"Firebase 앱 초기화 실패: {e}")
    exit()

def send_fcm_notification(token: str, title: str, body: str):
    try:
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            token=token,
        )
        response = messaging.send(message)
        logging.info(f"FCM 메시지 전송 성공 (token: {token[:10]}..., response: {response})")
    except Exception as e:
        logging.error(f"FCM 메시지 전송 실패 (token: {token[:10]}...): {e}")

def get_all_users_with_tokens():
    try:
        query = "SELECT user_id, device_token FROM user_device"
        rows = session.execute(query)
        users = []
        for row in rows:
            users.append({"user_id": row.user_id, "device_token": row.device_token})
        return users
    except Exception as e:
        logging.error(f"사용자 디바이스 토큰 조회 실패: {e}")
        return []

if __name__ == "__main__":
    while True:
        print("\n--- FCM 테스트 메뉴 ---")
        print("1. 등록된 사용자 목록 보기")
        print("2. 특정 사용자에게 테스트 메시지 보내기")
        print("3. 모든 사용자에게 테스트 메시지 보내기")
        print("4. 여러 사용자에게 테스트 메시지 보내기 (쉼표 구분)")
        print("q. 종료")

        choice = input("선택: ").strip().lower()

        if choice == '1':
            users = get_all_users_with_tokens()
            if users:
                print("\n--- 등록된 사용자 목록 ---")
                for user in users:
                    print(f"User ID: {user['user_id']}, Device Token: {user['device_token'][:10]}...")
            else:
                print("등록된 사용자가 없습니다.")
        elif choice == '2':
            user_id = input("메시지를 보낼 사용자 ID를 입력하세요: ").strip()
            users = get_all_users_with_tokens()
            target_token = None
            for user in users:
                if user['user_id'] == user_id:
                    target_token = user['device_token']
                    break
            
            if target_token:
                title = input("알림 제목을 입력하세요: ").strip()
                body = input("알림 내용을 입력하세요: ").strip()
                send_fcm_notification(target_token, title, body)
            else:
                print(f"사용자 ID '{user_id}'를 찾을 수 없습니다.")
        elif choice == '3':
            confirm = input("모든 사용자에게 메시지를 보내시겠습니까? (y/n): ").strip().lower()
            if confirm == 'y':
                users = get_all_users_with_tokens()
                if users:
                    title = input("알림 제목을 입력하세요: ").strip()
                    body = input("알림 내용을 입력하세요: ").strip()
                    for user in users:
                        send_fcm_notification(user['device_token'], title, body)
                    print(f"총 {len(users)}명에게 메시지를 보냈습니다.")
                else:
                    print("등록된 사용자가 없습니다.")
            else:
                print("취소되었습니다.")
        elif choice == '4':
            user_ids_str = input("메시지를 보낼 사용자 ID들을 쉼표로 구분하여 입력하세요 (예: user1,user2): ").strip()
            target_user_ids = [uid.strip() for uid in user_ids_str.split(',') if uid.strip()]
            
            if not target_user_ids:
                print("유효한 사용자 ID를 입력해주세요.")
                continue

            users = get_all_users_with_tokens()
            found_tokens = []
            for target_uid in target_user_ids:
                for user in users:
                    if user['user_id'] == target_uid:
                        found_tokens.append(user['device_token'])
                        break
            
            if found_tokens:
                title = input("알림 제목을 입력하세요: ").strip()
                body = input("알림 내용을 입력하세요: ").strip()
                for token in found_tokens:
                    send_fcm_notification(token, title, body)
                print(f"총 {len(found_tokens)}명에게 메시지를 보냈습니다.")
            else:
                print("지정된 사용자 ID를 찾을 수 없습니다.")
        elif choice == 'q':
            print("테스트를 종료합니다.")
            break
        else:
            print("잘못된 선택입니다. 다시 시도해주세요.")

    session.shutdown()
    cluster.shutdown()
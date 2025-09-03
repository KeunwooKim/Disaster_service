import logging
from fcm_sender import send_data_message_to_tokens, session, cluster

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_all_users_with_tokens():
    """
    Fetches all users and their device tokens from Cassandra.
    Uses the session from the fcm_sender module.
    """
    if not session:
        logging.error("Cassandra 세션이 초기화되지 않았습니다.")
        return []
    try:
        query = "SELECT user_id, device_token FROM user_device"
        rows = session.execute(query)
        users = [{"user_id": row.user_id, "device_token": row.device_token} for row in rows]
        return users
    except Exception as e:
        logging.error(f"사용자 디바이스 토큰 조회 실패: {e}")
        return []

def main():
    """
    Main function to run the FCM test menu.
    """
    while True:
        print("\n--- FCM 테스트 메뉴 (리팩토링 버전) ---")
        print("1. 등록된 사용자 목록 보기")
        print("2. 특정 사용자에게 테스트 메시지 보내기")
        print("3. 모든 사용자에게 테스트 메시지 보내기")
        print("4. 여러 사용자에게 테스트 메시지 보내기 (쉼표 구분)")
        print("q. 종료")

        choice = input("선택: ").strip().lower()

        if choice == 'q':
            break

        if choice == '1':
            users = get_all_users_with_tokens()
            if users:
                print("\n--- 등록된 사용자 목록 ---")
                for user in users:
                    print(f"User ID: {user['user_id']}, Device Token: {user.get('device_token', 'N/A')[:10]}...")
            else:
                print("등록된 사용자가 없습니다.")
            continue

        # Options 2, 3, 4 require sending a message
        if choice in ['2', '3', '4']:
            users = get_all_users_with_tokens()
            if not users:
                print("메시지를 보낼 사용자가 없습니다.")
                continue

            target_tokens = []
            if choice == '2':
                user_id = input("메시지를 보낼 사용자 ID를 입력하세요: ").strip()
                user_found = False
                for user in users:
                    if user['user_id'] == user_id:
                        if user.get('device_token'):
                            target_tokens.append(user['device_token'])
                        user_found = True
                        break
                if not user_found:
                    print(f"사용자 ID '{user_id}'를 찾을 수 없습니다.")
                    continue
            
            elif choice == '3':
                confirm = input("모든 사용자에게 메시지를 보내시겠습니까? (y/n): ").strip().lower()
                if confirm == 'y':
                    target_tokens = [user['device_token'] for user in users if user.get('device_token')]
                else:
                    print("취소되었습니다.")
                    continue

            elif choice == '4':
                user_ids_str = input("메시지를 보낼 사용자 ID들을 쉼표로 구분하여 입력하세요 (예: user1,user2): ").strip()
                target_user_ids = {uid.strip() for uid in user_ids_str.split(',') if uid.strip()}
                target_tokens = [user['device_token'] for user in users if user['user_id'] in target_user_ids and user.get('device_token')]

            if not target_tokens:
                print("알림을 보낼 대상이 없습니다.")
                continue

            # Get payload and send
            title = input("알림 제목을 입력하세요: ").strip()
            body = input("알림 내용을 입력하세요: ").strip()
            
            data_payload = {
                'title': title,
                'body': body,
                'source': 'test_fcm.py'
            }
            
            send_data_message_to_tokens(target_tokens, data_payload)
        
        else:
            print("잘못된 선택입니다. 다시 시도해주세요.")

    # Shutdown Cassandra connection
    if cluster:
        cluster.shutdown()
    print("테스트를 종료합니다.")

if __name__ == "__main__":
    main()

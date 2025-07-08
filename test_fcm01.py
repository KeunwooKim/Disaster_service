import firebase_admin
from firebase_admin import credentials
from firebase_admin import messaging

#cred_path = "/Users/keunwookim/Documents/Python/DisasterAPI/disaster-9dbd5-firebase-adminsdk-fbsvc-c4498ef23c.json"
cred_path = "/root/Project_disaster/disasteralert-bc125-firebase-adminsdk-fbsvc-808e3fc9d3.json"
cred = credentials.Certificate(cred_path)
firebase_admin.initialize_app(cred)

registration_token = 'eM5EwZdzQVWCHLEts-sEwd:APA91bFIeuh5SyZ4FOdyWDeE6xP-BEFdEk_zsImXP1bbHSnC-UKDg1t2U6P22w-mD7yNV7UrU-0hMPhIm7nhSrnFdVmrDHB1zrpsjsxwXNHJiWJKTsWZK60'
message = messaging.Message(
    notification = messaging.Notification(
        title='지진 재난 알림',
        body='원광대학교 프라임관 반경 10km 강도 2.0의 지진 발생',
    ),
    token=registration_token,
)

response = messaging.send(message)
print('Successfully sent message:', response)
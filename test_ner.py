# test_location_extraction.py

from ner_utils import extract_location  # ner_utils.py에 위 함수가 정의돼 있다고 가정
import sys

def main():
    print("📍 테스트할 텍스트를 입력하세요 (종료하려면 Enter 없이 엔터):")
    while True:
        text = input(">>> ")
        if not text.strip():
            print("종료합니다.")
            break
        region = extract_location(text)
        print(f"→ 예측된 지역: '{region}'\n")

if __name__ == "__main__":
    main()

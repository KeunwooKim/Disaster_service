# test_location_extraction.py

from ner_utils import extract_locations  # ner_utils.py에 위 함수가 정의돼 있다고 가정
import sys

if __name__ == "__main__":
    # 테스트할 문장 목록
    test_sentences = [
        "21:48 남구 선암동(두왕사거리, 감나무진사거리 방면)화재 발생. 인근 주민은 주의 바랍니다.",
        "오늘 13:48 온양읍 운화리 산119-1 산불 발생. 마을 주민은 대피하세요.",
        "서울특별시 성동구 왕십리로 123, 한양대 앞 횡단보도 인근에서 사고 발생.",
        "기상청 예보: 경기도 수원시 권선구 호매실동에 호우주의보 발효 중."
    ]

    for sent in test_sentences:
        regions = extract_locations(sent)
        print(f"문장: {sent}")
        print(f"추출된 지역: {regions}\n{'-'*60}")

    # 대화형 테스트 (비워 두고 엔터하면 종료)
    print("🔍 직접 입력 테스트 (종료하려면 빈 줄에서 엔터)")
    while True:
        text = input("문장 입력> ").strip()
        if not text:
            break
        print("추출된 지역:", extract_locations(text), end="\n\n")

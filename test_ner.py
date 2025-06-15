from ner_utils import extract_locations, extracted_regions

"""
이 스크립트는 ner_utils.py에서 정의한 extract_locations와 extracted_regions 함수를 불러와
샘플 문장에 대해 지역명을 출력합니다.
"""

def main():
    samples = [
        "21:48 남구 선암동 화재 발생. 인근 주민은 주의 바랍니다.",
        "온양읍 운화리 산119-1 산불 발생. 주민 대피 요망.",
        "기상청 예보: 경기도 수원시 권선구 호매실동 호우주의보 발효 중.",
        "서울특별시 성동구 왕십리로 사고 발생. 차량 우회 바랍니다."
    ]

    for text in samples:
        print(f"문장: {text}")
        # 단순 스팬 추출 (리스트 반환)
        locs = extract_locations(text)
        print("extract_locations →", locs)
        # 콘솔 출력 포함 추출 함수 호출
        regs = extracted_regions(text)
        print("extracted_regions →", regs)
        print('-' * 50)

if __name__ == "__main__":
    main()

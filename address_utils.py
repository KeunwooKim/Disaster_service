# address_utils.py

import re
from ner_utils import extracted_regions

# 1) 시·군·구 + 읍·면·동 + 산번호(예: 하남시 하산곡동 산51-2)
pattern_hill_full = re.compile(
    r'([가-힣]+(?:시|군|구)\s+[가-힣]+(?:읍|면|동)\s*산\d+(?:-\d+)?)'
)
# 2) 읍·면·동 + 산번호 (예: 하산곡동 산51-2)
pattern_hill_partial = re.compile(
    r'([가-힣]+(?:읍|면|동)\s*산\d+(?:-\d+)?)'
)
# 3) 시·군·구 + 읍·면·동 (예: 하남시 하산곡동)
pattern_partial = re.compile(
    r'([가-힣]+(?:시|군|구)\s+[가-힣]+(?:읍|면|동))'
)

def extract_best_address(text: str) -> str | None:
    """
    본문에서 가능한 한 가장 세부적인 주소(산번지 포함)를 추출.
    1) 시·군·구 + 읍·면·동 + 산번호
    2) 읍·면·동 + 산번호
    3) 시·군·구 + 읍·면·동
    4) 위 패턴이 없으면 기존 extracted_regions() 첫 번째 값
    """
    if not text:
        return None

    # 1) 시·군·구 + 읍·면·동 + 산번호
    m = pattern_hill_full.search(text)
    if m:
        return m.group(1)

    # 2) 읍·면·동 + 산번호
    m = pattern_hill_partial.search(text)
    if m:
        return m.group(1)

    # 3) 시·군·구 + 읍·면·동
    m = pattern_partial.search(text)
    if m:
        return m.group(1)

    # 4) fallback: ner_utils.extracted_regions() 첫 번째 결과
    regions = extracted_regions(text)
    return regions[0] if regions else None

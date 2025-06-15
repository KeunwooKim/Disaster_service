import os
import torch
from transformers import BertTokenizerFast, BertForTokenClassification

# ──────────────────────────────────────────────────────────────────────────────
# 1) 모델 로드
BASE_DIR = os.path.dirname(__file__)
MODEL_PATH = os.path.join(BASE_DIR, "ner_model")

tokenizer_loc = BertTokenizerFast.from_pretrained(MODEL_PATH)
model_loc = BertForTokenClassification.from_pretrained(MODEL_PATH)
model_loc.config.id2label = {0: "O", 1: "B-ORG", 2: "B-LOC", 3: "I-LOC", 4: "I-ORG"}
model_loc.config.label2id = {v: k for k, v in model_loc.config.id2label.items()}
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model_loc.to(device)
model_loc.eval()

# ──────────────────────────────────────────────────────────────────────────────
def extract_locations(text: str) -> list:
    """
    재난 문자 내에서 B-LOC/I-LOC로 예측된 지역명을 모두 추출하여 리스트로 반환합니다.
    특수 토큰(offset span이 0인 토큰)을 무시하며, 중복 제거 및 최소 길이 2자 이상의 지역만 반환합니다.
    """
    if not text:
        return []

    encoding = tokenizer_loc(
        text,
        return_offsets_mapping=True,
        truncation=True,
        max_length=512,
        return_tensors="pt"
    )
    input_ids = encoding["input_ids"].to(device)
    attention_mask = encoding["attention_mask"].to(device)
    offsets = encoding["offset_mapping"][0].tolist()

    with torch.no_grad():
        logits = model_loc(input_ids, attention_mask=attention_mask).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()

    regions = []
    current_span = None

    for idx, (label_id, (start, end)) in enumerate(zip(preds, offsets)):
        # special token or no span
        if start == end:
            continue
        label = model_loc.config.id2label[label_id]

        if label == "B-LOC":
            if current_span:
                regions.append(tuple(current_span))
            current_span = [start, end]
        elif label == "I-LOC" and current_span:
            current_span[1] = end
        else:
            if current_span:
                regions.append(tuple(current_span))
                current_span = None

    if current_span:
        regions.append(tuple(current_span))

    extracted = []
    for s, e in regions:
        segment = text[s:e].strip()
        if len(segment) >= 2 and segment not in extracted:
            extracted.append(segment)
    return extracted

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    examples = [
        "21:48 남구 선암동(두왕사거리, 감나무진사거리 방면)화재 발생. 인근 주민은 주의 바랍니다.",
        "오늘 13:48 온양읍 운화리 산119-1 산불 발생. 마을 주민은 대피하세요.",
        "서울특별시 성동구 왕십리로 123, 한양대 앞 횡단보도 인근에서 사고 발생.",
        "기상청 예보: 경기도 수원시 권선구 호매실동에 호우주의보 발효 중."
    ]

    for sent in examples:
        print(f"문장: {sent}")
        print(f"추출된 지역: {extract_locations(sent)}")
        print("─" * 40)

    print("🔍 직접 입력 테스트 (종료하려면 빈 줄)")
    while True:
        text = input("문장 입력> ").strip()
        if not text:
            break
        print(f"추출된 지역: {extract_locations(text)}\n")

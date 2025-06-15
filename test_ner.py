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
    전체 텍스트에서 B-LOC/I-LOC로 인식된 스팬(문자열)을 반환합니다.
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

    spans = []
    current = None
    for idx, (label_id, (start, end)) in enumerate(zip(preds, offsets)):
        if start == end:
            continue
        label = model_loc.config.id2label[label_id]
        if label == "B-LOC":
            if current:
                spans.append(tuple(current))
            current = [start, end]
        elif label == "I-LOC" and current:
            current[1] = end
        else:
            if current:
                spans.append(tuple(current))
                current = None
    if current:
        spans.append(tuple(current))

    regions = []
    for s, e in spans:
        seg = text[s:e].strip()
        if len(seg) >= 2 and seg not in regions:
            regions.append(seg)
    return regions


def extract_location_tokens(text: str) -> list:
    """
    입력 문장을 토큰 단위로 분해한 후, B-LOC/I-LOC로 라벨된 모든 토큰을 반환합니다.
    """
    if not text:
        return []
    encoding = tokenizer_loc(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=512
    )
    input_ids = encoding["input_ids"][0]
    tokens = tokenizer_loc.convert_ids_to_tokens(input_ids)
    encoding = {k: v.to(device) for k, v in encoding.items()}
    with torch.no_grad():
        logits = model_loc(**encoding).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()

    loc_tokens = []
    for tok, pid in zip(tokens, preds):
        label = model_loc.config.id2label.get(pid, "O")
        if label in {"B-LOC", "I-LOC"}:
            loc_tokens.append(tok)
    return loc_tokens

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    samples = [
        "21:48 남구 선암동(두왕사거리, 감나무진사거리 방면)화재 발생. 인근 주민은 주의 바랍니다.",
        "오늘 13:48 온양읍 운화리 산119-1 산불 발생. 마을 주민은 대피하세요.",
        "서울특별시 성동구 왕십리로 123, 한양대 앞 횡단보도 인근에서 사고 발생.",
        "기상청 예보: 경기도 수원시 권선구 호매실동에 호우주의보 발효 중."
    ]

    for sent in samples:
        print(f"문장: {sent}")
        print(f"추출된 스팬: {extract_locations(sent)}")
        print(f"추출된 토큰: {extract_location_tokens(sent)}")
        print("─" * 40)

    print("🔍 직접 입력 테스트 (종료하려면 빈 줄 입력)")
    while True:
        text = input("문장 입력> ").strip()
        if not text:
            break
        print(f"스팬: {extract_locations(text)}")
        print(f"토큰: {extract_location_tokens(text)}\n")

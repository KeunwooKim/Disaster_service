from transformers import BertTokenizerFast, BertForTokenClassification
import torch
import os

# ──────────────────────────────────────────────────────────────────────────────
# 1) 모델 경로 설정
BASE_DIR = os.path.dirname(__file__)
MODEL_PATH = os.path.join(BASE_DIR, "ner_model")

# 2) 토크나이저·모델 로드
tokenizer_loc = BertTokenizerFast.from_pretrained(MODEL_PATH)
model_loc = BertForTokenClassification.from_pretrained(MODEL_PATH)

# 3) id2label/label2id 세팅 (학습 시 사용한 라벨과 동일하게)
model_loc.config.id2label = {
    0: "O",
    1: "B-ORG",
    2: "B-LOC",
    3: "I-LOC",
    4: "I-ORG"
}
model_loc.config.label2id = {v: k for k, v in model_loc.config.id2label.items()}

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model_loc.to(device)
model_loc.eval()

# ──────────────────────────────────────────────────────────────────────────────
def extract_locations(text: str) -> list:
    """
    재난 문자 내에서 B-LOC/I-LOC로 예측된 지역명을 모두 추출하여 리스트로 반환합니다.
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
    offset_mapping = encoding["offset_mapping"][0].tolist()

    with torch.no_grad():
        outputs = model_loc(input_ids, attention_mask=attention_mask)
    logits = outputs.logits.squeeze(0)
    preds = torch.argmax(logits, dim=-1).tolist()

    locations = []
    current_start = None
    current_end = None

    for idx, label_id in enumerate(preds):
        label = model_loc.config.id2label.get(label_id, "O")
        start_char, end_char = offset_mapping[idx]

        if label == "B-LOC":
            if current_start is not None and current_end is not None:
                locations.append((current_start, current_end))
            current_start, current_end = start_char, end_char

        elif label == "I-LOC":
            if current_start is not None:
                current_end = end_char
            else:
                # B-LOC 없이 시작한 I-LOC → 예외 처리로 시작점으로 간주
                current_start, current_end = start_char, end_char

        else:
            if current_start is not None and current_end is not None:
                locations.append((current_start, current_end))
                current_start, current_end = None, None

    if current_start is not None and current_end is not None:
        locations.append((current_start, current_end))

    # 중복 제거 및 정제
    extracted_regions = []
    for s, e in locations:
        if e - s >= 2:
            region = text[s:e].strip()
            if region and region not in extracted_regions:
                extracted_regions.append(region)

    return extracted_regions


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
def extract_location(text: str) -> str:
    """
    입력된 문자열(text)을 NER 모델로 예측한 뒤,
    B-LOC, I-LOC 태그가 붙은 토큰들의 원문 상 오프셋(offset) 구간을 합쳐서
    “지역명”을 뽑아 반환합니다. (없으면 빈 문자열)

    (1) tokenizer에 `return_offsets_mapping=True` 옵션을 주면,
        출력물(inputs) 안에 `offset_mapping`이 있다.
        offset_mapping[i] = (start_char, end_char) → 토큰 i가 원문에서 차지하는 구간 정보.

    (2) 모델이 예측한 logits을 argmax로 변환해서 “각 토큰별 라벨”을 얻고,
        만약 라벨이 B-LOC이면, 현재 토큰의 (start, end)를 시작으로 잡고,
        뒤따르는 I-LOC이 있을 때마다 end를 늘려가다가 “O 또는 B-**”가 나오면 끊는다.

    이 과정을 통해 “원본 텍스트(text)”에서 정확한 substring을 뽑아낼 수 있습니다.
    """
    if not text:
        return ""

    # (1) 토큰화 + 오프셋 매핑 요청
    encoding = tokenizer_loc(
        text,
        return_offsets_mapping=True,
        truncation=True,
        max_length=512,   # 문장이 아주 길 경우 512 토큰까지만 처리
        return_tensors="pt"
    )
    input_ids = encoding["input_ids"].to(device)            # (1, seq_len)
    attention_mask = encoding["attention_mask"].to(device)  # (1, seq_len)
    offset_mapping = encoding["offset_mapping"][0].tolist() # [(start, end), ...] 길이 = seq_len

    # (2) 모델 예측
    with torch.no_grad():
        outputs = model_loc(input_ids, attention_mask=attention_mask)
    logits = outputs.logits.squeeze(0)       # (seq_len, num_labels)
    preds = torch.argmax(logits, dim=-1).tolist()  # [label_id, label_id, ...]

    # (3) B-LOC/I-LOC 연속 구간을 찾아 원문 텍스트에서 자르기
    locations = []
    current_start = None
    current_end = None

    for idx, label_id in enumerate(preds):
        label = model_loc.config.id2label[label_id]
        if label == "B-LOC":
            # 이전에 달리던 구간이 있으면 저장
            if current_start is not None and current_end is not None:
                locations.append((current_start, current_end))
            # 새 구간 시작
            start_char, end_char = offset_mapping[idx]
            current_start, current_end = start_char, end_char

        elif label == "I-LOC" and current_start is not None:
            # 이어지는 토큰이면 end를 갱신
            _, end_char = offset_mapping[idx]
            current_end = end_char

        else:
            # B/I-LOC 연속이 끊긴 경우
            if current_start is not None and current_end is not None:
                locations.append((current_start, current_end))
                current_start, current_end = None, None

    # 마지막으로 남은 구간 저장
    if current_start is not None and current_end is not None:
        locations.append((current_start, current_end))

    # (4) 여러 개의 위치가 예측될 수 있지만, 보통 첫 번째만 사용
    if locations:
        # locations[0]이 (start_char, end_char)
        s, e = locations[0]
        return text[s:e]
    return ""

from transformers import BertTokenizerFast, BertForTokenClassification
import torch
import os
# 모델 경로
BASE_DIR = os.path.dirname(__file__)
MODEL_PATH = os.path.join(BASE_DIR, "ner_model")
# 토크나이저·모델 로드
tokenizer_loc = BertTokenizerFast.from_pretrained(MODEL_PATH)
model_loc = BertForTokenClassification.from_pretrained(MODEL_PATH)

# id2label, label2id 세팅 (학습 시 사용한 것과 동일하게)
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

def extract_location(text: str) -> str:
    """
    메시지 한 줄을 받아서 NER 모델로 토큰별 예측을 수행한 뒤,
    'B-LOC' + 그 뒤의 'I-LOC' 토큰들을 합쳐서 지역명으로 반환.
    없으면 빈 문자열("") 리턴.
    """
    if not text:
        return ""

    # 1) 토큰화
    inputs = tokenizer_loc(text, return_tensors="pt", truncation=True)
    inputs = {k: v.to(device) for k, v in inputs.items()}

    # 2) 예측
    with torch.no_grad():
        outputs = model_loc(**inputs)
    preds = torch.argmax(outputs.logits, dim=-1).squeeze().tolist()

    tokens = tokenizer_loc.convert_ids_to_tokens(inputs["input_ids"].squeeze().tolist())

    # 3) B-LOC/I-LOC 연속 토큰 모으기
    locations = []
    current_loc = ""
    for token, pred_id in zip(tokens, preds):
        label = model_loc.config.id2label[pred_id]
        if label == "B-LOC":
            if current_loc:
                locations.append(current_loc)
            current_loc = token.replace("##", "")
        elif label == "I-LOC" and current_loc:
            if token.startswith("##"):
                current_loc += token[2:]
            else:
                current_loc += token
        else:
            if current_loc:
                locations.append(current_loc)
                current_loc = ""
    if current_loc:
        locations.append(current_loc)

    # 4) 있으면 첫 번째, 없으면 ""
    return locations[0] if locations else ""

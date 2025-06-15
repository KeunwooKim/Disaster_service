import os
import torch
from transformers import BertTokenizerFast, BertForTokenClassification

# 모델 로드
BASE_DIR = os.path.dirname(__file__)
MODEL_PATH = os.path.join(BASE_DIR, "ner_model")

tokenizer_loc = BertTokenizerFast.from_pretrained(MODEL_PATH)
model_loc = BertForTokenClassification.from_pretrained(MODEL_PATH)
model_loc.config.id2label = {0: "O", 1: "B-LOC", 2: "I-LOC"}
model_loc.config.label2id = {v: k for k, v in model_loc.config.id2label.items()}
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model_loc.to(device)
model_loc.eval()


def extract_locations(text: str) -> list:
    """
    입력 텍스트에서 B-LOC/I-LOC로 예측된 단어 단위 지명을 스팬으로 추출하여 리스트로 반환합니다.
    불용어(stopwords)에 해당하는 단어가 포함된 스팬은 제외합니다.
    """
    STOPWORDS = {"미리", "관리"}

    if not text:
        return []

    words = text.split()
    encoding = tokenizer_loc(
        words,
        is_split_into_words=True,
        return_tensors="pt",
        truncation=True,
        max_length=512
    )
    input_ids = encoding["input_ids"].to(device)
    attention_mask = encoding["attention_mask"].to(device)
    word_ids = encoding.word_ids()

    with torch.no_grad():
        logits = model_loc(input_ids=input_ids, attention_mask=attention_mask).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()

    spans = []
    current = []
    prev_wid = None

    for idx, wid in enumerate(word_ids):
        label = "O" if wid is None else model_loc.config.id2label[preds[idx]]
        if wid is not None and label in ("B-LOC", "I-LOC"):
            if wid != prev_wid:
                current.append(words[wid])
        else:
            if current:
                span = " ".join(current)
                # 불용어 포함 시 제외
                if not any(sw in span for sw in STOPWORDS):
                    spans.append(span)
                current = []
        prev_wid = wid

    # 마지막 스팬 처리
    if current:
        span = " ".join(current)
        if not any(sw in span for sw in STOPWORDS):
            spans.append(span)

    return spans


def extracted_regions(text: str) -> list:
    """
    extract_locations를 호출해 모든 지명 스팬을 추출하고, 콘솔에 출력한 뒤 리스트로 반환합니다.
    """
    regions = extract_locations(text)
    print(f"[extracted_regions] 추출된 지명: {regions}")
    return regions

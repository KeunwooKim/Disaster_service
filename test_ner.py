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
    B-LOC/I-LOC로 예측된 단어 단위 지명을 연속 스팬으로 반환합니다.
    """
    if not text:
        return []
    # 단어 단위로 간단 분할
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

    # 예측
    with torch.no_grad():
        logits = model_loc(input_ids=input_ids, attention_mask=attention_mask).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()

    # word_id 단위 LOC 판단
    span_list = []
    current_span = []
    prev_wid = None
    for idx, wid in enumerate(word_ids):
        if wid is None:
            label = "O"
        else:
            label = model_loc.config.id2label[preds[idx]]
        if wid is not None and label in ("B-LOC", "I-LOC"):
            # LOC 단어
            if wid != prev_wid:
                # 새로운 단어
                current_span.append(words[wid])
            else:
                # 같은 단어 id: 추가하지 않음
                pass
        else:
            # LOC 종료
            if current_span:
                span_list.append(" ".join(current_span))
                current_span = []
        prev_wid = wid
    # 마지막 span
    if current_span:
        span_list.append(" ".join(current_span))
    return span_list


def extract_location_tokens(text: str) -> list:
    """
    B-LOC/I-LOC 서브워드 토큰 리스트를 반환합니다.
    """
    if not text:
        return []
    enc = tokenizer_loc(text, return_tensors="pt", truncation=True, max_length=512)
    input_ids = enc["input_ids"][0]
    tokens = tokenizer_loc.convert_ids_to_tokens(input_ids)
    enc = {k: v.to(device) for k, v in enc.items()}
    with torch.no_grad():
        logits = model_loc(**enc).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()
    return [tok for tok, pid in zip(tokens, preds) if model_loc.config.id2label[pid] in ('B-LOC','I-LOC')]


def debug_token_labels(text: str) -> list:
    """
    모든 토큰과 예측 라벨을 튜플로 반환합니다.
    """
    enc = tokenizer_loc(text, return_tensors="pt", truncation=True, max_length=512)
    input_ids = enc["input_ids"][0]
    tokens = tokenizer_loc.convert_ids_to_tokens(input_ids)
    enc = {k: v.to(device) for k, v in enc.items()}
    with torch.no_grad():
        logits = model_loc(**enc).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()
    return [(tok, model_loc.config.id2label[pid]) for tok, pid in zip(tokens, preds)]

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
        print(f"원문 단어 분할: {sent.split()}")
        print(f"토큰-라벨: {debug_token_labels(sent)}")
        print(f"추출된 스팬: {extract_locations(sent)}")
        print(f"추출된 토큰: {extract_location_tokens(sent)}")
        print("─" * 40)
    print("🔍 직접 입력 테스트 (종료하려면 빈 줄 입력)")
    while True:
        text = input("문장 입력> ").strip()
        if not text:
            break
        print(f"단어 분할: {text.split()}")
        print(f"토큰-라벨: {debug_token_labels(text)}")
        print(f"스팬: {extract_locations(text)}")
        print(f"토큰: {extract_location_tokens(text)}\n")

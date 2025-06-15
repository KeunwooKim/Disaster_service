import os
import torch
from transformers import BertTokenizerFast, BertForTokenClassification

# ──────────────────────────────────────────────────────────────────────────────
# 모델 로드
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
    B-LOC/I-LOC로 예측된 서브워드를 단어 단위로 결합해, 연속된 지명 단어를 스팬 문자열로 반환합니다.
    """
    if not text:
        return []
    # 토큰화: word level 인덱스 포함
    encoding = tokenizer_loc(
        text,
        return_offsets_mapping=True,
        return_tensors="pt",
        truncation=True,
        max_length=512,
        is_split_into_words=True
    )
    input_ids = encoding["input_ids"].to(device)
    attention_mask = encoding["attention_mask"].to(device)
    word_ids = encoding.word_ids()
    tokens = tokenizer_loc.convert_ids_to_tokens(input_ids[0])

    # 모델 예측
    with torch.no_grad():
        logits = model_loc(input_ids=input_ids, attention_mask=attention_mask).logits[0]
    preds = torch.argmax(logits, dim=-1).tolist()

    # word_id 단위로 subword 결합 및 word-level LOC 판정
    word_texts = []
    word_is_loc = []
    current_id = None
    pieces = []
    labels = []
    for tok, pid, wid in zip(tokens, preds, word_ids):
        if wid is None:
            continue
        label = model_loc.config.id2label[pid]
        if wid != current_id:
            if current_id is not None:
                # finalize previous word
                word = ''.join([p[2:] if p.startswith('##') else p for p in pieces])
                is_loc = any(l in ('B-LOC','I-LOC') for l in labels)
                word_texts.append(word)
                word_is_loc.append(is_loc)
            # reset for new word
            current_id = wid
            pieces = [tok]
            labels = [label]
        else:
            pieces.append(tok)
            labels.append(label)
    # 마지막 단어 처리
    if current_id is not None:
        word = ''.join([p[2:] if p.startswith('##') else p for p in pieces])
        is_loc = any(l in ('B-LOC','I-LOC') for l in labels)
        word_texts.append(word)
        word_is_loc.append(is_loc)

    # 연속 LOC 단어를 묶어 스팬 생성
    spans = []
    current_span = []
    for word, is_loc in zip(word_texts, word_is_loc):
        if is_loc:
            current_span.append(word)
        else:
            if current_span:
                spans.append(' '.join(current_span))
                current_span = []
    if current_span:
        spans.append(' '.join(current_span))

    return spans


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
        print(f"전처리된 단어: {tokenizer_loc.convert_ids_to_tokens(tokenizer_loc(sent, return_tensors='pt', truncation=True, max_length=512)['input_ids'][0])}")
        print(f"토큰-라벨: {debug_token_labels(sent)}")
        print(f"추출된 스팬: {extract_locations(sent)}")
        print(f"추출된 토큰: {extract_location_tokens(sent)}")
        print("─" * 40)
    print("🔍 직접 입력 테스트 (종료하려면 빈 줄 입력)")
    while True:
        text = input("문장 입력> ").strip()
        if not text:
            break
        print(f"단어 단위: {tokenizer_loc.convert_ids_to_tokens(tokenizer_loc(text, return_tensors='pt', truncation=True, max_length=512)['input_ids'][0])}")
        print(f"토큰-라벨: {debug_token_labels(text)}")
        print(f"스팬: {extract_locations(text)}")
        print(f"토큰: {extract_location_tokens(text)}\n")

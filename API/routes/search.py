import os
import json
import re
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from firebase_admin import credentials, firestore, initialize_app
from firebase_admin import _apps as firebase_apps
from rapidfuzz import fuzz
from jamo import h2j, hangul_to_jamo
from pympler import asizeof
from app.utils.cache import product_cache
from app.utils.brandlabel import brand_label_map_kor_to_eng, brand_label_map_eng_to_kor

# Firebase 초기화
if not firebase_apps:
    firebase_key = os.environ.get("FIREBASE_KEY")
    key_dict = json.loads(firebase_key)
    cred = credentials.Certificate(key_dict)
    initialize_app(cred)
    print("✅ Firebase 초기화 완료")

db = firestore.client()
router = APIRouter()

# 문자열 정제 (리스트 및 기타 타입 대응)
def get_clean_text(text) -> str:
    if isinstance(text, list):
        text = " ".join(map(str, text))
    elif not isinstance(text, str):
        text = str(text)
    return re.sub(r"[^\w\s\uAC00-\uD7A3]", "", text).lower().strip()

# 브랜드 확장 키워드
def expand_brand_keywords(keyword: str) -> list[str]:
    keyword = get_clean_text(keyword)
    expanded = {keyword}

    if keyword in brand_label_map_kor_to_eng:
        eng = brand_label_map_kor_to_eng[keyword]
        if isinstance(eng, list):
            expanded.update(get_clean_text(e) for e in eng)
        else:
            expanded.add(get_clean_text(eng))

    if keyword in brand_label_map_eng_to_kor:
        expanded.add(get_clean_text(brand_label_map_eng_to_kor[keyword]))

    return list(expanded)

# 캐시 적재
def load_products():
    global product_cache
    print("📍 load_products() 함수 진입")

    page_size = 1000
    last_doc = None
    total_loaded = 0

    try:
        metadata_doc = db.collection("metadata").document("products_metadata").get()
        if metadata_doc.exists:
            total_count = metadata_doc.to_dict().get("total_count", 25000)
            print(f"ℹ️ 메타데이터 기반 MAX_DOCS: {total_count}")
        else:
            print("⚠️ 메타데이터 문서 없음 → fallback 사용")
            total_count = 25000
    except Exception as e:
        print(f"❌ 메타데이터 조회 실패 → fallback 사용: {e}")
        total_count = 25000

    try:
        while total_loaded < total_count:
            query = db.collection("products").order_by("product_name").limit(page_size)
            if last_doc:
                query = query.start_after(last_doc)

            docs = query.get()
            if not docs:
                break

            for doc in docs:
                data = doc.to_dict()
                product_name = data.get("product_name", "")

                brand_kor = next((b for b in brand_label_map_kor_to_eng if b in product_name), "")
                brand_eng_raw = brand_label_map_kor_to_eng.get(brand_kor, "")

                if isinstance(brand_eng_raw, list):
                    brand_eng = brand_eng_raw[0]
                else:
                    brand_eng = brand_eng_raw

                product_cache.append({
                    **data,
                    "product_id": doc.id,
                    "brand_name_kor": brand_kor,
                    "brand_name_eng": brand_eng
                })

                total_loaded += 1

            last_doc = docs[-1]

        print(f"✅ [제품 캐시] 총 {len(product_cache)}개 적재 완료")

    except Exception as e:
        print(f"❌ [제품 캐시 오류] {e}")

def jamo_text(text: str) -> str:
    # 자모 단위로 변환 후 공백 제거
    return ''.join(list(hangul_to_jamo(text))).replace(' ', '')

def is_jamo_similar(keyword: str, target: str, threshold: int = 80) -> bool:
    keyword_jamo = jamo_text(keyword)
    target_jamo = jamo_text(target)
    return fuzz.ratio(keyword_jamo, target_jamo) >= threshold

@router.get("/search")
def search_products(keyword: str = Query(..., min_length=1)):
    try:
        keyword_clean = get_clean_text(keyword)
        keywords = expand_brand_keywords(keyword_clean)
        results = []

        for product in product_cache:
            product_name = get_clean_text(product.get("product_name", ""))
            brand_kor = get_clean_text(product.get("brand_name_kor", ""))
            brand_eng = get_clean_text(product.get("brand_name_eng", ""))
            manufacturer = get_clean_text(product.get("manufacturer", ""))

            # 1단계: 정확 일치 (브랜드 한글/영어, 제조사 포함)
            if any(k == brand_kor or k == brand_eng or k == manufacturer for k in keywords):
                results.append(product)
                continue

            # 2단계: 제품명 or 제조사에 키워드 포함
            if any(k in product_name or k in manufacturer for k in keywords):
                results.append(product)
                continue

            # 3단계: fuzzy match (제품명 or 제조사) 기준 85 이상
            if any(
                fuzz.partial_ratio(k, product_name) >= 85 or
                fuzz.partial_ratio(k, manufacturer) >= 85
                for k in keywords
            ):
                results.append(product)

        if results:
            print(f"[검색] '{keyword}' → 결과 {len(results)}개")
            return JSONResponse(content=results, media_type="application/json; charset=utf-8")

        # 🔁 Fallback 단계: 완화된 조건 (fuzzy 70 + manufacturer/brand도 포함 + 자모 유사도)
        print(f"[검색-FALLBACK] '{keyword}' 포함 조건으로 재검색")
        fallback_matches = []
        for product in product_cache:
            product_name_clean = get_clean_text(product.get("product_name", ""))
            manufacturer_clean = get_clean_text(product.get("manufacturer", ""))
            brand_kor_clean = get_clean_text(product.get("brand_name_kor", ""))

            # manufacturer 매핑 확장
            manufacturer_expanded = {manufacturer_clean}
            if manufacturer_clean in brand_label_map_kor_to_eng:
                eng = brand_label_map_kor_to_eng[manufacturer_clean]
                if isinstance(eng, list):
                    manufacturer_expanded.update([get_clean_text(e) for e in eng])
                else:
                    manufacturer_expanded.add(get_clean_text(eng))
            if manufacturer_clean in brand_label_map_eng_to_kor:
                manufacturer_expanded.add(get_clean_text(brand_label_map_eng_to_kor[manufacturer_clean]))

            for k in keywords:
                if (
                    fuzz.partial_ratio(k, product_name_clean) >= 70
                    or any(fuzz.partial_ratio(k, m) >= 70 for m in manufacturer_expanded)
                    or fuzz.partial_ratio(k, brand_kor_clean) >= 70
                    or is_jamo_similar(k, product_name_clean)
                    or any(is_jamo_similar(k, m) for m in manufacturer_expanded)
                    or is_jamo_similar(k, brand_kor_clean)
                ):
                    fallback_matches.append(product)
                    break  # ✅ 중복 방지

        print(f"[검색] '{keyword}' → 결과 {len(fallback_matches)}개 (Fallback)")
        return JSONResponse(content=fallback_matches, media_type="application/json; charset=utf-8")

    except Exception as e:
        import traceback
        print(f"❌ [검색 중 오류 발생] {e}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"error": str(e)})

# 캐시 용량 정보 API
@router.get("/cache-info")
def get_cache_info():
    try:
        est_avg_kb = 10
        estimated_total_mb = round((len(product_cache) * est_avg_kb) / 1024, 2)

        try:
            sample = product_cache[:50]
            sample_size = asizeof.asizeof(sample)
            per_item_kb = sample_size / 50 / 1024
            refined_estimate_mb = round((per_item_kb * len(product_cache)) / 1024, 2)
        except:
            refined_estimate_mb = "Not available"

        return JSONResponse(content={
            "cached_products": len(product_cache),
            "estimated_memory_mb_simple": estimated_total_mb,
            "estimated_memory_mb_refined": refined_estimate_mb
        })

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
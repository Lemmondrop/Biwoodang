import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from rapidfuzz import fuzz

from app.utils.cache import product_cache

router = APIRouter()

@router.get("/recommend/{product_id}")
def recommend(product_id: str, limit: int = Query(default=4, ge=1, le=10)):
    if not product_cache:
        return JSONResponse(
            status_code=503,
            content={"error": "추천 캐시가 비어 있습니다"},
            media_type="application/json; charset=utf-8"
        )

    df = pd.DataFrame(product_cache)
    df = df[df["product_id"].notna()]
    df["product_id"] = df["product_id"].astype(str)

    if product_id not in df["product_id"].values:
        return JSONResponse(
            status_code=404,
            content={"error": "Invalid product ID"},
            media_type="application/json; charset=utf-8"
        )

    base_idx = df.index[df["product_id"] == product_id][0]
    base_category = df.iloc[base_idx]["category"]
    base_big_category = df.iloc[base_idx]["big_category"]
    base_name = df.iloc[base_idx]["product_name"]

    df_filtered = df[df["category"] == base_category].reset_index(drop=True)
    if len(df_filtered) < 5:
        df_filtered = df[df["big_category"] == base_big_category].reset_index(drop=True)

    def get_combined(row):
        rm = row.get("raw_materials", {})
        return " ".join([
            row.get("product_name", ""),
            row.get("category", ""),
            rm.get("safe", ""),
            rm.get("caution", ""),
            rm.get("warning", ""),
            rm.get("etc", "")
        ])

    df_filtered["combined"] = df_filtered.apply(get_combined, axis=1)

    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(df_filtered["combined"])

    if product_id not in df_filtered["product_id"].values:
        return JSONResponse(
            status_code=404,
            content={"error": "Product not found in filtered set"},
            media_type="application/json; charset=utf-8"
        )

    idx = df_filtered.index[df_filtered["product_id"] == product_id][0]
    cosine_scores = cosine_similarity(tfidf_matrix[idx], tfidf_matrix).flatten()

    def position_penalty(row):
        rm = row.get("raw_materials", {})
        ingredients = rm.get("ingredients_raw", "")
        warning_list = [w.strip() for w in rm.get("warning", "").split(",") if w.strip()]
        ingredients_list = [i.strip() for i in ingredients.split(",") if i.strip()]
        total_len = len(ingredients_list)
        if total_len == 0:
            return 0
        score = 0
        for warning in warning_list:
            if warning in ingredients_list:
                i = ingredients_list.index(warning)
                ratio = i / total_len
                if ratio <= 0.5:
                    score -= 0.5
                elif ratio >= 0.9:
                    score += 0.5
        return score

    top_indices = cosine_scores.argsort()[::-1]
    top_indices = [i for i in top_indices if i != idx]

    scored_candidates = [
        (i, cosine_scores[i] + position_penalty(df_filtered.iloc[i]))
        for i in top_indices
    ]

    # 1차 필터링: 제품명 유사도
    primary = []
    for i, score in scored_candidates[:200]:
        name = df_filtered.iloc[i]["product_name"]
        if fuzz.partial_ratio(base_name, name) >= 40:
            primary.append((i, score))

    # 부족 시 보충
    selected = set(i for i, _ in primary)
    if len(primary) < limit:
        additional = [(i, score) for i, score in scored_candidates if i not in selected][:limit - len(primary)]
        primary += additional

    final_top = [i for i, _ in sorted(primary, key=lambda x: x[1], reverse=True)[:limit]]
    recommended = df_filtered.iloc[final_top][["product_id", "manufacturer", "product_name", "image_url"]]

    return JSONResponse(
        content=recommended.to_dict(orient="records"),
        media_type="application/json; charset=utf-8"
    )

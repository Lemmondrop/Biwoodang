from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from app.utils.cache import product_cache

# ---------------------------------------------------------------
# 📌 카테고리별 제품 리스트 API
# 이 API는 지정된 대분류(big_category)에 해당하는 제품을 필터링하여 반환합니다.
#
# ✅ 반환 개수 제한에 대한 설명:
# Render 무료 플랜(Free Tier)에서는 서버 리소스(RAM, 응답 바디 크기 등)에 제약이 있으므로,
# 한 번에 반환하는 제품 수를 최대 1,000개로 제한하고 있습니다.
#
# - FastAPI 자체에는 기본 응답 개수 제한은 없지만,
#   실제 운영 환경에서는 메모리 초과(OOM)나 응답 지연, 클라이언트 측 JSON 렌더링 실패 가능성이 있습니다.
#
# - 따라서 안정성을 고려하여 API 레벨에서 하드코딩으로 제한(limit=1000)을 설정한 것입니다.
#
# 🚀 추후 유료 플랜 도입 시 페이지네이션 기반 구조로 확장 가능하도록 설계되어 있습니다.
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# 📌 Category-based Product Listing API
# This endpoint returns a list of products filtered by the given `big_category`.

# ✅ Why is there a limit on the number of returned products?
# Since this service is currently deployed on Render's free tier,
# there are limitations in server resources such as memory and response body size.
# To ensure stability and prevent issues like OOM (Out of Memory) or client-side rendering lag,
# the number of returned products is explicitly limited to a maximum of 1,000.

# - FastAPI itself does not impose any default response size limit.
# - However, returning too many items at once may lead to browser crashes or timeouts.

# 🚀 In the future, this can be expanded to support full pagination or increased limits
# if deployed on a higher-tier server plan.
# ---------------------------------------------------------------

router = APIRouter(tags=["Category"])

@router.get("/category")
def get_products_by_category(
    big_category: str = Query(..., description="예: 음료류, 과자류 등"),
    limit: int = Query(500, ge=1, le=1000, description="최대 반환 개수 (기본: 500)")
):
    keyword = big_category.lower()
    results = [
        p for p in product_cache
        if keyword in p.get("big_category", "").lower()
    ]
    return JSONResponse(
        content=results[:limit],
        media_type="application/json; charset=utf-8"
    )
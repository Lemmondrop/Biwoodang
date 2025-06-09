from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every

from app.utils.cache import product_cache
from app.routes.products import router as product_router
from app.routes.recommend import router as recommend_router
from app.routes.search import router as search_router, load_products  # 🔥 여기에 load_products 가져오기
from app.routes.categorical import router as categorical_router

import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(product_router)
app.include_router(recommend_router)
app.include_router(search_router)
app.include_router(categorical_router)

# 🔥 startup 이벤트에 캐시 적재 연결
@app.on_event("startup")
def startup_event():
    print("🚀 [Startup] load_products() 호출 시작")
    load_products()
    print("🚀 [Startup] load_products() 호출 완료")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
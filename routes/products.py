from fastapi import APIRouter
import firebase_admin
import os
import json
from firebase_admin import credentials, firestore, storage
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

# Firebase 환경변수 기반 초기화
if not firebase_admin._apps:
    firebase_key = os.environ.get("FIREBASE_KEY")
    key_dict = json.loads(firebase_key)
    cred = credentials.Certificate(key_dict)
    firebase_admin.initialize_app(cred)

db = firestore.client()

@router.get("/products/{product_id}")
def get_product_detail(product_id: str):
    doc = db.collection("products").document(product_id).get()
    if not doc.exists:
        return JSONResponse(
            content={"error": "Product not found"},
            media_type="application/json; charset=utf-8"
        )

    return JSONResponse(
        content=doc.to_dict(),
        media_type="application/json; charset=utf-8"  # ✅ 인코딩 명시
    )
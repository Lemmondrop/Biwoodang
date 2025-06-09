import pandas as pd
import numpy as np
from tqdm import tqdm
from io import BytesIO
import firebase_admin
from firebase_admin import credentials, storage, firestore

# Firebase 초기화
cred = credentials.Certificate(
    "FIREBASE JSON FILE PATH"
)
firebase_admin.initialize_app(
    cred, {"storageBucket": "YOUR_FIREBASE_STORAGE_BUCKET_ADDRESS"} # Replace with your actual Firebase Storage bucket URL
)
db = firestore.client()
bucket = storage.bucket()

# CSV 불러오기
df = pd.read_csv('YOUR .csv FILE PATH', encoding="utf-8-sig") # Replace with your actual .csv file path address

# 컬럼 매핑 딕셔너리
column_map = {
    "제품명": "product_name",
    "업체명": "manufacturer",
    "식품대분류명": "big_category",
    "식품소분류명": "category",
    "에너지(kcal)": "energy_kcal",
    "단백질(g)": "protein_g",
    "지방(g)": "fat_g",
    "탄수화물(g)": "carbs_g",
    "당류(g)": "sugar_g",
    "식이섬유(g)": "fiber_g",
    "칼슘(mg)": "calcium_mg",
    "철(mg)": "iron_mg",
    "인(mg)": "phosphorus_mg",
    "칼륨(mg)": "potassium_mg",
    "나트륨(mg)": "sodium_mg",
    "비타민A(μg RAE)": "vitamin_a",
    "베타카로틴(μg)": "beta_carotene",
    "티아민(mg)": "thiamine",
    "리보플라빈(mg)": "riboflavin",
    "니아신(mg)": "niacin",
    "비타민 C(mg)": "vitamin_c",
    "비타민 D(μg)": "vitamin_d",
    "비오틴(μg)": "biotin",
    "비타민 B6 / 피리독신(mg)": "vitamin_b6",
    "비타민 B12(μg)": "vitamin_b12",
    "엽산(μg DFE)": "folate",
    "판토텐산(mg)": "pantothenic_acid",
    "비타민 D3(μg)": "vitamin_d3",
    "콜레스테롤(mg)": "cholesterol",
    "포화지방산(g)": "saturated_fatty_acid",
    "트랜스지방산(g)": "trans_fat",
    "비타민 E(mg α-TE)": "vitamin_e",
    "비타민 K(μg)": "vitamin_k",
    "비타민 K1(μg)": "vitamin_k1",
    "당알콜(g)": "sugar_alcohol",
    "알룰로오스(g)": "allulose",
    "에리스리톨(g)": "erythritol",
    "불포화지방산(g)": "unsaturated_fat",
    "EPA와 DHA의 합(mg)": "epa_dha",
    "리놀레산(18:2(n-6))(g)": "linoleic_acid",
    "알파 리놀렌산(18:3(n-3))(g)": "alpha_linolenic_acid",
    "오메가3 지방산(g)": "omega3",
    "오메가6 지방산(g)": "omega6",
    "올레산(18:1(n-9))(mg)": "oleic_acid",
    "구리(μg)": "copper",
    "마그네슘(mg)": "magnesium",
    "망간(mg)": "manganese",
    "몰리브덴(μg)": "molybdenum",
    "셀레늄(μg)": "selenium",
    "아연(mg)": "zinc",
    "염소(mg)": "chloride",
    "요오드(μg)": "iodine",
    "크롬(μg)": "chromium",
    "라이신(mg)": "lysine",
    "류신(mg)": "leucine",
    "메티오닌(mg)": "methionine",
    "발린(mg)": "valine",
    "아르기닌(mg)": "arginine",
    "이소류신(mg)": "isoleucine",
    "타우린(mg)": "taurine",
    "트레오닌(mg)": "threonine",
    "트립토판(mg)": "tryptophan",
    "페닐알라닌(mg)": "phenylalanine",
    "히스티딘(mg)": "histidine",
    "원재료명": "ingredients_raw",
    "안전": "safe",
    "유의": "caution",
    "주의": "warning",
    "그외": "etc",
    "유통업체명": "distributor",
    "허가번호": "license_no",
    "품목보고일자": "report_date",
    "당지수(GI)": "gi_point",
    "안전성": "safety_message",
    "인증" : "zero_certification"
}

# 숫자형 필드 변환 함수
def convert_fields(doc):
    numeric_fields = {
        "energy_kcal", "protein_g", "fat_g", "carbs_g", "sugar_g", "fiber_g",
        "calcium_mg", "iron_mg", "phosphorus_mg", "potassium_mg", "sodium_mg",
        "vitamin_a", "beta_carotene", "thiamine", "riboflavin", "niacin",
        "vitamin_c", "vitamin_d", "biotin", "vitamin_b6", "vitamin_b12", "folate",
        "pantothenic_acid", "vitamin_d3", "cholesterol", "saturated_fatty_acid",
        "trans_fat", "vitamin_e", "vitamin_k", "vitamin_k1", "sugar_alcohol",
        "allulose", "erythritol", "unsaturated_fat", "epa_dha", "linoleic_acid",
        "alpha_linolenic_acid", "omega3", "omega6", "oleic_acid", "copper",
        "magnesium", "manganese", "molybdenum", "selenium", "zinc", "chloride",
        "iodine", "chromium", "lysine", "leucine", "methionine", "valine",
        "arginine", "isoleucine", "taurine", "threonine", "tryptophan",
        "phenylalanine", "histidine", "zero_certification"
    }
    
    for field, value in doc.items():
        if field in numeric_fields:
            try:
                # 빈 문자열 또는 NaN만 None 처리하고, 0은 유지
                if value == "" or pd.isna(value):
                    doc[field] = None
                else:
                    doc[field] = float(value)
            except (ValueError, TypeError):
                doc[field] = None
    return doc
# Firestore 기존 문서 전체 불러오기
product_docs = db.collection("products").stream()
firestore_products = {doc.id: doc.to_dict() for doc in product_docs}

# image_url 빠른 매핑용 dict 생성 (제품명 → image_url)
product_name_to_image_url = {
    data.get("product_name", ""): data.get("image_url", "")
    for data in firestore_products.values()
    if "image_url" in data
}

# CSV 기준 남길 제품명 목록
csv_product_names = df["제품명"].dropna().tolist()

# Firestore 삭제 대상 판별 및 삭제
product_ids_to_delete = [
    doc_id for doc_id, data in firestore_products.items()
    if data.get("product_name", "") not in csv_product_names
]
for doc_id in product_ids_to_delete:
    try:
        db.collection("products").document(doc_id).delete()
        print(f"{doc_id} 삭제됨")
    except Exception as e:
        print(f"[삭제 실패] {doc_id} - {e}")

# 재업로드 (product_0부터 넘버링)
for new_index, (_, row) in enumerate(tqdm(df.iterrows(), total=len(df), desc="업데이트 진행")):
    try:
        product_name = row.get("제품명", "")
        product_id = f"product_{new_index}"
        doc_ref = db.collection("products").document(product_id)

        # 컬럼명 매핑 + 결측치 처리
        doc = {column_map.get(k, k): (v if pd.notna(v) else "") for k, v in row.items()}

        # 필드 타입 변환
        doc = convert_fields(doc)

        # raw_materials 필드 구조화
        doc["raw_materials"] = {
            "ingredients_raw": doc.pop("ingredients_raw", ""),
            "safe": doc.pop("safe", ""),
            "caution": doc.pop("caution", ""),
            "warning": doc.pop("warning", ""),
            "etc": doc.pop("etc", "")
        }

        # image_url 유지
        if product_name in product_name_to_image_url:
            doc["image_url"] = product_name_to_image_url[product_name]

        # 병합 업로드
        doc_ref.set(doc, merge=True)

        if new_index % 100 == 0:
            print(f"{new_index}개 등록 완료")

    except Exception as e:
        print(f"[오류] index={new_index}, 제품명={product_name} : {e}")
        break

from firebase_admin import firestore, initialize_app

try:
    initialize_app()
except ValueError:
    pass  # 이미 초기화된 경우 무시

db = firestore.client()

def update_products_metadata(total_count):
    try:
        metadata_ref = db.collection("metadata").document("products_metadata")
        metadata_ref.set({
            "total_count": total_count,
            "last_updated": firestore.SERVER_TIMESTAMP
        })
        print(f"✅ metadata 문서 업데이트 완료: 총 {total_count}개")
    except Exception as e:
        print(f"❌ metadata 문서 생성 실패: {e}")
        
# ✅ metadata 문서 갱신 실행 (new_index는 마지막 성공 인덱스, 0부터 시작이므로 +1)
update_products_metadata(new_index + 1)

print("삭제 + 필드 병합 + 재업로드 + 메타데이터 갱신 완료!")
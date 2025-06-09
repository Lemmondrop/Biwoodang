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
    cred, {"storageBucket": "YOUR_FIREBASE_STORAGE_BUCKET_ADDRESS"}  # Replace with your actual Firebase Storage bucket URL
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

    # 필드 값 변환
    for field, value in doc.items():
        if field in numeric_fields:
            try:
                doc[field] = float(value) if value else None
            except (ValueError, TypeError):
                doc[field] = None
        elif field == "gi_point" and isinstance(value, str) and "~" in value:
            try:
                min_val, max_val = value.split("~")
                doc[field] = {
                    "min": float(min_val.strip()),
                    "max": float(max_val.strip())
                }
            except:
                doc[field] = value  # 변환 실패 시 원래 문자열 유지

    return doc

# 시작 인덱스 설정
start_index = 0  # 0-based index

# 업로드 루프
for index, row in tqdm(df.iloc[:start_index].iterrows(), total=len(df) - start_index, desc="업로드 진행"):
    try:
        product_id = f"product_{index}"
        doc_ref = db.collection("products").document(product_id)

        # 컬럼명 매핑 + 결측치 처리
        doc = {column_map.get(k, k): (v if pd.notna(v) else "") for k, v in row.items()}

        # 필드 타입 변환
        doc = convert_fields(doc)

        # raw_materials 필드 구조화
        doc["raw_materials"] = {
            "safe": doc.pop("safe", ""),
            "caution": doc.pop("caution", ""),
            "warning": doc.pop("warning", ""),
            "etc": doc.pop("etc", "")
        }

        # Firestore에 병합 방식으로 저장
        # doc_ref.set(doc, merge=True) # merge=True, 덮어쓰기 방지 옵션
        doc_ref.set(doc) # 덮어쓰기


        if index % 100 == 0:
            print(f"{index}개 등록 완료")
    
    except Exception as e:
        print(f"작업 중단! 현재 인덱스: {index} - {str(e)}")
        break

    except KeyboardInterrupt:
        print(f"\n[사용자 중단] {index}번째 인덱스에서 작업 중단됨")
        break

print("작업 완료!")

# 기본 이미지로 image_url 필드 채우기기
# 설정
placeholder_url = "FIREBASE STORAGE PATH PUT IN HERE" # Replace with your actual firebase storage folder path address
collection_name = "products"
batch_size = 500
page_size = 1000  # 한 번에 가져올 문서 수 (권장: 500~1000)

def update_image_urls_in_pages():
    last_doc = None
    total_updated = 0

    while True:
        query = db.collection(collection_name).limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)

        docs = list(query.stream())
        if not docs:
            break

        batch = db.batch()
        updated_in_batch = 0

        for doc in docs:
            data = doc.to_dict()
            image_url = data.get("image_url", None)

            if image_url is None or image_url.strip() == "":
                batch.update(doc.reference, {"image_url": placeholder_url})
                updated_in_batch += 1

        if updated_in_batch > 0:
            batch.commit()
            total_updated += updated_in_batch
            print(f"✅ {total_updated}개 문서 업데이트 중...")

        last_doc = docs[-1]  # 다음 페이지 기준점

    print(f"🎉 완료: 총 {total_updated}개 문서에 placeholder 이미지 추가됨")

# 실행
update_image_urls_in_pages()
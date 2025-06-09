# 한글 → 영어 브랜드명 매핑
brand_label_map_kor_to_eng = {
    "비비고": "bibigo",
    "비비드": "vivid",
    "비비드키친": "vividkitchen",
    "코카콜라": ["cocacola",'cola', 'coke'],
    "펩시": "pepsi",
    "삼양": "samyang",
    "오뚜기": "ottogi",
    "농심": "nongshim",
    "롯데": "lotte",
    "노브랜드":"nobrand",
    "마이노멀":"mynormal",
    "라라스윗":"lalasweet",
    "씨유" : "비지에프",
    "시유" : "비지에프"
}

# 영어 → 한글 역방향 매핑 (리스트도 지원)
brand_label_map_eng_to_kor = {}

for kor, eng in brand_label_map_kor_to_eng.items():
    if isinstance(eng, list):
        for e in eng:
            brand_label_map_eng_to_kor[e.lower()] = kor  # 소문자 처리
    else:
        brand_label_map_eng_to_kor[eng.lower()] = kor
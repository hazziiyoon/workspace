import polars as pl
import os

# 1. 경로 설정
B2_PATH = "/Users/hajiyoon/dataset_seveneleven/B2_POS_SALE.parquet"
B4_PATH = "/Users/hajiyoon/dataset_seveneleven/B4_food_item_data.parquet"
B5_PATH = "/Users/hajiyoon/dataset_seveneleven/B5_MNM_DATA.xlsx"
OUTPUT_PATH = "block1_co_purchase_graph.parquet"

def build_graph():
    print("🚀 Block 1: 장바구니 공동 구매 그래프 구축 시작...")

    # [Step 1] 데이터 로드 (Lazy Mode)
    print("STEP 1: 데이터 스캔 중...")
    
    # POS 데이터 (B2)
    b2_lazy = pl.scan_parquet(B2_PATH)
    
    # 식품 마스터 (B4) - 상품코드 ITEM_CD를 String으로 보장
    b4_lazy = pl.scan_parquet(B4_PATH).select([
        pl.col("ITEM_CD").cast(pl.String).alias("상품코드"),
        pl.col("ITEM_NM")
    ])

    # 행사 마스터 (B5) - Excel이므로 즉시 로드 후 처리
    print("B5 행사 마스터 로드 중...")
    b5_df = pl.read_excel(B5_PATH)
    b5_lazy = b5_df.lazy().select([
        pl.col("상품코드").cast(pl.String),
        pl.col("행사개시일").alias("start_dt"),
        pl.col("행사종료일").alias("end_dt"),
        pl.lit(True).alias("is_promo")
    ])

    # [Step 2] 데이터 정제 및 영수증 ID 생성
    print("STEP 2: 정제 및 영수증 ID 생성...")
    # B2 전처리: 날짜 변환 및 receipt_id 생성
    b2_refined = b2_lazy.with_columns([
        pl.col("상품코드").cast(pl.String),
        pl.col("판매일자").str.to_date("%Y%m%d").alias("sale_dt"),
        (pl.col("점포코드") + "_" + 
         pl.col("POS번호") + "_" + 
         pl.col("판매일자") + "_" + 
         pl.col("거래번호")).alias("receipt_id")
    ])

    # 식품 데이터만 필터링 (Inner Join)
    b2_food = b2_refined.join(b4_lazy, on="상품코드", how="inner")

    # [Step 3] 가중치(Weight) 조정 (Promotion Bias 제거)
    print("STEP 3: 가중치 조정 로직 적용...")
    # 행사 데이터와 Join (상품코드 기준)
    # 특정 기간 내에 있는지 확인하기 위해 날짜 조건부 Join 대신 Join 후 Filter 방식 활용 (Polars 최적화)
    pos_promo = b2_food.join(b5_lazy, on="상품코드", how="left")
    
    # 행사 기간 내 판매 여부 판단 및 가중치 계산
    pos_weighted = pos_promo.with_columns([
        pl.when(
            (pl.col("is_promo") == True) & 
            (pl.col("sale_dt") >= pl.col("start_dt")) & 
            (pl.col("sale_dt") <= pl.col("end_dt"))
        )
        .then(pl.col("판매수량").cast(pl.Float64) * 0.5)
        .otherwise(pl.col("판매수량").cast(pl.Float64) * 1.0)
        .alias("adj_weight")
    ])

    # 영수증 내 동일 상품이 여러 줄 있을 경우 가중치 합산
    basket_items = pos_weighted.group_by(["receipt_id", "상품코드"]).agg(
        pl.col("adj_weight").sum().alias("item_weight")
    )

    # [Step 4] 상품-상품 투영 (Self-join for Co-purchase)
    print("STEP 4: 상품-상품 투영(Projection) 및 연결 강도 계산...")
    # 동일 영수증 내 상품 쌍 생성
    # 메모리 폭발 방지를 위해 필요한 컬럼만 선택
    pairs = basket_items.join(
        basket_items, 
        on="receipt_id", 
        suffix="_target"
    ).filter(
        pl.col("상품코드") < pl.col("상품코드_target") # 중복 쌍 및 Self-loop 방지
    )

    # 연결 강도 계산 (Weight A * Weight B)
    # 이후 전체 영수증에 대해 Edge별로 합산
    graph_edges = pairs.group_by(["상품코드", "상품코드_target"]).agg(
        (pl.col("item_weight") * pl.col("item_weight_target")).sum().alias("weight")
    ).rename({
        "상품코드": "source",
        "상품코드_target": "target"
    })

    # [Step 5] 결과 저장
    print(f"STEP 5: 최종 결과 저장 중... ({OUTPUT_PATH})")
    graph_edges.sink_parquet(OUTPUT_PATH)
    print("✅ 모든 작업이 완료되었습니다!")

if __name__ == "__main__":
    try:
        # StringCache 사용으로 조인 성능 및 메모리 효율 향상
        with pl.StringCache():
            build_graph()
    except Exception as e:
        print(f"❌ 에러 발생: {e}")

"""
ダミーデータ生成スクリプト
商品マスタ・売上実績・在庫・予約データをCSVで生成する
"""

import random
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker("ja_JP")
fake.seed_instance(SEED)

OUTPUT_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 商品定義
CATEGORIES = ["トップス", "ボトムス", "アウター", "小物"]

PRODUCT_NAMES = {
    "トップス": [
        "ベーシックTシャツ", "ロングスリーブTシャツ", "ボーダーカットソー",
        "リネンシャツ", "オックスフォードシャツ", "フランネルシャツ",
        "ニットプルオーバー", "クルーネックスウェット", "フーディースウェット",
        "タートルネックニット", "ケーブルニット", "モックネックTシャツ",
        "ヘンリーネックシャツ", "ポロシャツ",
    ],
    "ボトムス": [
        "スリムチノパン", "ワイドデニムパンツ", "テーパードスラックス",
        "カーゴパンツ", "ジョガーパンツ", "プリーツスカート",
        "フレアスカート", "タイトスカート", "ミニスカート",
        "ショートパンツ", "バミューダパンツ",
    ],
    "アウター": [
        "スタンダードダウンジャケット", "チェスターコート", "トレンチコート",
        "マウンテンパーカー", "デニムジャケット", "ブルゾン",
        "ピーコート", "フリースジャケット",
    ],
    "小物": [
        "レザートートバッグ", "ナイロンショルダーバッグ", "キャンバスエコバッグ",
        "ニット帽", "キャップ", "マフラー", "レザーベルト",
        "コットンソックス（3足セット）",
    ],
}

COLORS = {
    "トップス": ["ホワイト", "ブラック", "ネイビー", "グレー", "ベージュ", "ライトブルー"],
    "ボトムス": ["ブラック", "ネイビー", "カーキ", "グレー", "ベージュ"],
    "アウター": ["ブラック", "ネイビー", "グレー", "カーキ", "ブラウン"],
    "小物": ["ブラック", "ブラウン", "ベージュ", "ネイビー"],
}

# 価格帯（原価・売価）
PRICE_RANGE = {
    "トップス": {"cost": (1500, 4000), "price": (3990, 9990)},
    "ボトムス": {"cost": (2000, 6000), "price": (5990, 14990)},
    "アウター": {"cost": (5000, 20000), "price": (14990, 49990)},
    "小物": {"cost": (500, 3000), "price": (1990, 7990)},
}

# リードタイム（日数）
LEAD_TIME = {
    "トップス": (14, 30),
    "ボトムス": (21, 45),
    "アウター": (30, 60),
    "小物": (14, 30),
}

# 季節性: 月ごとのカテゴリ販売倍率
SEASONAL_FACTOR = {
    "トップス": [1.2, 1.0, 1.3, 1.5, 1.8, 2.0, 2.2, 2.0, 1.5, 1.0, 0.8, 0.9],
    "ボトムス": [1.0, 1.0, 1.3, 1.5, 1.5, 1.2, 1.0, 1.0, 1.3, 1.2, 1.0, 0.9],
    "アウター": [2.5, 2.0, 1.2, 0.5, 0.2, 0.1, 0.1, 0.1, 0.5, 1.5, 2.5, 2.8],
    "小物": [1.5, 1.2, 1.0, 0.8, 0.8, 0.7, 0.7, 0.8, 1.0, 1.2, 1.5, 2.0],
}


def generate_products() -> pd.DataFrame:
    """商品マスタを生成する（50商品）"""
    products = []
    product_id = 1

    all_names = []
    for cat, names in PRODUCT_NAMES.items():
        for name in names:
            all_names.append((cat, name))

    # 50商品になるよう調整
    target = 50
    while len(all_names) < target:
        cat = random.choice(CATEGORIES)
        base = random.choice(PRODUCT_NAMES[cat])
        all_names.append((cat, f"{base}（別モデル）"))
    all_names = all_names[:target]
    random.shuffle(all_names)

    for cat, name in all_names:
        cat_colors = COLORS[cat]
        n_colors = random.randint(2, min(4, len(cat_colors)))
        selected_colors = random.sample(cat_colors, n_colors)

        cost_range = PRICE_RANGE[cat]["cost"]
        price_range = PRICE_RANGE[cat]["price"]
        unit_cost = random.randint(*cost_range)
        selling_price = random.randint(
            max(price_range[0], int(unit_cost * 2)),
            price_range[1]
        )

        lead_range = LEAD_TIME[cat]
        lead_time = random.randint(*lead_range)

        products.append({
            "product_id": f"P{product_id:04d}",
            "product_name": name,
            "category": cat,
            "colors": ",".join(selected_colors),
            "unit_cost": unit_cost,
            "selling_price": selling_price,
            "lead_time_days": lead_time,
        })
        product_id += 1

    return pd.DataFrame(products)


def generate_sales(products: pd.DataFrame) -> pd.DataFrame:
    """売上実績を生成する（過去12ヶ月、約5万レコード）"""
    today = datetime(2026, 4, 2)
    start_date = today - timedelta(days=365)

    # 売れ筋商品（上位20%）
    n_products = len(products)
    hot_products = set(
        products.sample(frac=0.2, random_state=SEED)["product_id"].tolist()
    )

    sales = []
    sale_id = 1

    date_range = [start_date + timedelta(days=i) for i in range(365)]

    for _, product in products.iterrows():
        pid = product["product_id"]
        cat = product["category"]
        colors = product["colors"].split(",")
        is_hot = pid in hot_products

        # 日あたりの基本販売数
        base_daily = random.uniform(3, 8) if is_hot else random.uniform(0.5, 3)

        for date in date_range:
            month_idx = date.month - 1
            season_mult = SEASONAL_FACTOR[cat][month_idx]

            # 日ごとの実際の販売数（ポアソン分布）
            expected = base_daily * season_mult
            n_sales = np.random.poisson(expected)

            if n_sales == 0:
                continue

            # 色別に分配
            color_weights = np.random.dirichlet(np.ones(len(colors)))
            for color, weight in zip(colors, color_weights):
                qty = max(0, int(round(n_sales * weight)))
                if qty == 0:
                    continue
                sales.append({
                    "sale_id": f"S{sale_id:07d}",
                    "product_id": pid,
                    "color": color,
                    "quantity": qty,
                    "sale_date": date.strftime("%Y-%m-%d"),
                    "sale_amount": qty * product["selling_price"],
                })
                sale_id += 1

    df = pd.DataFrame(sales)
    print(f"  売上レコード数: {len(df):,}")
    return df


def generate_inventory(products: pd.DataFrame) -> pd.DataFrame:
    """現在庫を生成する（商品×色ごとに1レコード）"""
    inventory = []
    warehouses = ["東京倉庫A", "東京倉庫B", "大阪倉庫", "名古屋倉庫"]

    for _, product in products.iterrows():
        colors = product["colors"].split(",")
        for color in colors:
            # 一部の商品・色は在庫が少ない（在庫切れリスク演出）
            if random.random() < 0.15:
                stock = random.randint(0, 10)
            elif random.random() < 0.2:
                stock = random.randint(100, 300)  # 過剰在庫
            else:
                stock = random.randint(15, 80)

            inventory.append({
                "product_id": product["product_id"],
                "color": color,
                "current_stock": stock,
                "warehouse_location": random.choice(warehouses),
            })

    return pd.DataFrame(inventory)


def generate_reservations(products: pd.DataFrame) -> pd.DataFrame:
    """予約情報を生成する（約200レコード）"""
    reservations = []
    statuses = ["発注済", "生産中", "入荷待ち"]
    today = datetime(2026, 4, 2)

    sampled = products.sample(n=min(60, len(products)), random_state=SEED)

    res_id = 1
    for _, product in sampled.iterrows():
        colors = product["colors"].split(",")
        n_res = random.randint(1, 5)

        for _ in range(n_res):
            color = random.choice(colors)
            lead_time = product["lead_time_days"]
            delivery_offset = random.randint(int(lead_time * 0.5), int(lead_time * 1.5))
            delivery_date = today + timedelta(days=delivery_offset)
            status = random.choice(statuses)

            reservations.append({
                "reservation_id": f"R{res_id:05d}",
                "product_id": product["product_id"],
                "color": color,
                "reserved_quantity": random.randint(10, 100),
                "expected_delivery_date": delivery_date.strftime("%Y-%m-%d"),
                "status": status,
            })
            res_id += 1

            if res_id > 201:
                break
        if res_id > 201:
            break

    return pd.DataFrame(reservations)


def main():
    print("ダミーデータ生成を開始します...")

    print("1/4 商品マスタを生成中...")
    products = generate_products()
    out = OUTPUT_DIR / "products.csv"
    products.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"  -> {out} ({len(products)} 件)")

    print("2/4 売上実績を生成中（約1分かかります）...")
    sales = generate_sales(products)
    out = OUTPUT_DIR / "sales.csv"
    sales.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"  -> {out} ({len(sales):,} 件)")

    print("3/4 在庫情報を生成中...")
    inventory = generate_inventory(products)
    out = OUTPUT_DIR / "inventory.csv"
    inventory.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"  -> {out} ({len(inventory)} 件)")

    print("4/4 予約情報を生成中...")
    reservations = generate_reservations(products)
    out = OUTPUT_DIR / "reservations.csv"
    reservations.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"  -> {out} ({len(reservations)} 件)")

    print("\nダミーデータ生成が完了しました。")
    print(f"出力先: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

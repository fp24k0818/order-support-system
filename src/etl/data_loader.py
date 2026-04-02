"""
ETL処理モジュール
CSVデータをSQLiteデータベースに取り込み、分析用ビューを作成する
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
DB_PATH = Path(__file__).parent.parent.parent / "data" / "db" / "order_support.db"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def load_csv(filename: str) -> pd.DataFrame:
    path = RAW_DIR / filename
    df = pd.read_csv(path, encoding="utf-8-sig")
    logger.info(f"{filename} を読み込みました ({len(df):,} 件)")
    return df


def create_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.executescript("""
        DROP TABLE IF EXISTS products;
        CREATE TABLE products (
            product_id      TEXT PRIMARY KEY,
            product_name    TEXT NOT NULL,
            category        TEXT NOT NULL,
            colors          TEXT NOT NULL,
            unit_cost       INTEGER NOT NULL,
            selling_price   INTEGER NOT NULL,
            lead_time_days  INTEGER NOT NULL
        );

        DROP TABLE IF EXISTS sales;
        CREATE TABLE sales (
            sale_id     TEXT PRIMARY KEY,
            product_id  TEXT NOT NULL,
            color       TEXT NOT NULL,
            quantity    INTEGER NOT NULL,
            sale_date   TEXT NOT NULL,
            sale_amount INTEGER NOT NULL
        );

        DROP TABLE IF EXISTS inventory;
        CREATE TABLE inventory (
            product_id          TEXT NOT NULL,
            color               TEXT NOT NULL,
            current_stock       INTEGER NOT NULL,
            warehouse_location  TEXT,
            PRIMARY KEY (product_id, color)
        );

        DROP TABLE IF EXISTS reservations;
        CREATE TABLE reservations (
            reservation_id          TEXT PRIMARY KEY,
            product_id              TEXT NOT NULL,
            color                   TEXT NOT NULL,
            reserved_quantity       INTEGER NOT NULL,
            expected_delivery_date  TEXT NOT NULL,
            status                  TEXT NOT NULL
        );
    """)
    conn.commit()
    logger.info("テーブルを作成しました")


def insert_data(conn: sqlite3.Connection) -> None:
    products = load_csv("products.csv")
    products.to_sql("products", conn, if_exists="replace", index=False)
    logger.info(f"products: {len(products):,} 件を挿入しました")

    sales = load_csv("sales.csv")
    sales.to_sql("sales", conn, if_exists="replace", index=False)
    logger.info(f"sales: {len(sales):,} 件を挿入しました")

    inventory = load_csv("inventory.csv")
    inventory.to_sql("inventory", conn, if_exists="replace", index=False)
    logger.info(f"inventory: {len(inventory):,} 件を挿入しました")

    reservations = load_csv("reservations.csv")
    reservations.to_sql("reservations", conn, if_exists="replace", index=False)
    logger.info(f"reservations: {len(reservations):,} 件を挿入しました")


def create_views(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    cursor.executescript("""
        DROP VIEW IF EXISTS v_product_summary;
        CREATE VIEW v_product_summary AS
        WITH
        today AS (SELECT DATE('now') AS dt),

        sales_30 AS (
            SELECT product_id,
                   SUM(quantity)    AS qty_30d,
                   SUM(sale_amount) AS amount_30d
            FROM sales, today
            WHERE sale_date >= DATE(today.dt, '-30 days')
            GROUP BY product_id
        ),
        sales_60 AS (
            SELECT product_id,
                   SUM(quantity)    AS qty_60d,
                   SUM(sale_amount) AS amount_60d
            FROM sales, today
            WHERE sale_date >= DATE(today.dt, '-60 days')
            GROUP BY product_id
        ),
        sales_90 AS (
            SELECT product_id,
                   SUM(quantity)    AS qty_90d,
                   SUM(sale_amount) AS amount_90d
            FROM sales, today
            WHERE sale_date >= DATE(today.dt, '-90 days')
            GROUP BY product_id
        ),

        inv_total AS (
            SELECT product_id, SUM(current_stock) AS total_stock
            FROM inventory
            GROUP BY product_id
        ),

        res_ordered AS (
            SELECT product_id, SUM(reserved_quantity) AS qty_ordered
            FROM reservations WHERE status = '発注済'
            GROUP BY product_id
        ),
        res_production AS (
            SELECT product_id, SUM(reserved_quantity) AS qty_production
            FROM reservations WHERE status = '生産中'
            GROUP BY product_id
        ),
        res_waiting AS (
            SELECT product_id, SUM(reserved_quantity) AS qty_waiting
            FROM reservations WHERE status = '入荷待ち'
            GROUP BY product_id
        ),
        res_total AS (
            SELECT product_id, SUM(reserved_quantity) AS qty_reserved
            FROM reservations
            GROUP BY product_id
        )

        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.colors,
            p.unit_cost,
            p.selling_price,
            p.lead_time_days,

            -- 粗利率
            ROUND(CAST(p.selling_price - p.unit_cost AS REAL) / p.selling_price, 4)
                AS gross_margin_rate,

            -- 販売実績
            COALESCE(s30.qty_30d,    0) AS qty_30d,
            COALESCE(s60.qty_60d,    0) AS qty_60d,
            COALESCE(s90.qty_90d,    0) AS qty_90d,
            COALESCE(s30.amount_30d, 0) AS amount_30d,
            COALESCE(s60.amount_60d, 0) AS amount_60d,
            COALESCE(s90.amount_90d, 0) AS amount_90d,

            -- 日平均販売数
            ROUND(COALESCE(s30.qty_30d, 0) / 30.0, 2) AS avg_daily_30d,
            ROUND(COALESCE(s60.qty_60d, 0) / 60.0, 2) AS avg_daily_60d,
            ROUND(COALESCE(s90.qty_90d, 0) / 90.0, 2) AS avg_daily_90d,

            -- 在庫
            COALESCE(inv.total_stock, 0) AS total_stock,

            -- 予約数
            COALESCE(ro.qty_ordered,    0) AS qty_ordered,
            COALESCE(rp.qty_production, 0) AS qty_production,
            COALESCE(rw.qty_waiting,    0) AS qty_waiting,
            COALESCE(rt.qty_reserved,   0) AS qty_reserved,

            -- 在庫日数（日平均30日ベース、ゼロ除算防止）
            CASE
                WHEN COALESCE(s30.qty_30d, 0) = 0 THEN 9999
                ELSE ROUND(COALESCE(inv.total_stock, 0) / (COALESCE(s30.qty_30d, 0) / 30.0), 1)
            END AS stock_days

        FROM products p
        LEFT JOIN sales_30 s30 ON p.product_id = s30.product_id
        LEFT JOIN sales_60 s60 ON p.product_id = s60.product_id
        LEFT JOIN sales_90 s90 ON p.product_id = s90.product_id
        LEFT JOIN inv_total inv ON p.product_id = inv.product_id
        LEFT JOIN res_ordered   ro ON p.product_id = ro.product_id
        LEFT JOIN res_production rp ON p.product_id = rp.product_id
        LEFT JOIN res_waiting   rw ON p.product_id = rw.product_id
        LEFT JOIN res_total     rt ON p.product_id = rt.product_id;
    """)

    cursor.executescript("""
        DROP VIEW IF EXISTS v_color_breakdown;
        CREATE VIEW v_color_breakdown AS
        WITH
        today AS (SELECT DATE('now') AS dt),

        color_sales AS (
            SELECT product_id, color, SUM(quantity) AS qty_total
            FROM sales, today
            WHERE sale_date >= DATE(today.dt, '-90 days')
            GROUP BY product_id, color
        ),
        product_sales_total AS (
            SELECT product_id, SUM(qty_total) AS product_total
            FROM color_sales
            GROUP BY product_id
        )

        SELECT
            cs.product_id,
            cs.color,
            cs.qty_total,
            ROUND(CAST(cs.qty_total AS REAL) / NULLIF(pt.product_total, 0), 4)
                AS sales_ratio,
            COALESCE(inv.current_stock, 0) AS current_stock,
            COALESCE(
                (SELECT SUM(reserved_quantity) FROM reservations r
                 WHERE r.product_id = cs.product_id AND r.color = cs.color),
                0
            ) AS reserved_qty

        FROM color_sales cs
        JOIN product_sales_total pt ON cs.product_id = pt.product_id
        LEFT JOIN inventory inv
            ON cs.product_id = inv.product_id AND cs.color = inv.color;
    """)

    conn.commit()
    logger.info("ビュー v_product_summary, v_color_breakdown を作成しました")


def run_etl() -> None:
    logger.info("ETL処理を開始します")
    conn = get_connection()
    try:
        create_tables(conn)
        insert_data(conn)
        create_views(conn)

        # 件数確認
        for table in ["products", "sales", "inventory", "reservations"]:
            cnt = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table}: {cnt:,} 件")

        logger.info(f"ETL処理が完了しました。DB: {DB_PATH}")
    finally:
        conn.close()


def main():
    run_etl()


if __name__ == "__main__":
    main()

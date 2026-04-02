"""
ETL処理のテスト
"""

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.etl.data_loader import DB_PATH, run_etl


@pytest.fixture(scope="module")
def db_conn():
    run_etl()
    conn = sqlite3.connect(DB_PATH)
    yield conn
    conn.close()


def test_tables_exist(db_conn):
    tables = {
        row[0]
        for row in db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert "products" in tables
    assert "sales" in tables
    assert "inventory" in tables
    assert "reservations" in tables


def test_products_count(db_conn):
    cnt = db_conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    assert cnt == 50, f"商品マスタの件数が想定と異なります: {cnt}"


def test_sales_count(db_conn):
    cnt = db_conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    assert cnt > 10_000, f"売上レコード数が少なすぎます: {cnt}"


def test_inventory_count(db_conn):
    cnt = db_conn.execute("SELECT COUNT(*) FROM inventory").fetchone()[0]
    assert cnt > 0, "在庫データが空です"


def test_reservations_count(db_conn):
    cnt = db_conn.execute("SELECT COUNT(*) FROM reservations").fetchone()[0]
    assert cnt > 0, "予約データが空です"


def test_views_exist(db_conn):
    views = {
        row[0]
        for row in db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        )
    }
    assert "v_product_summary" in views
    assert "v_color_breakdown" in views


def test_product_summary_rows(db_conn):
    cnt = db_conn.execute("SELECT COUNT(*) FROM v_product_summary").fetchone()[0]
    assert cnt == 50


def test_no_negative_stock_days(db_conn):
    cnt = db_conn.execute(
        "SELECT COUNT(*) FROM v_product_summary WHERE stock_days < 0"
    ).fetchone()[0]
    assert cnt == 0, f"在庫日数が負の商品があります: {cnt} 件"

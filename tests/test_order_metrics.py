"""
発注指標計算のテスト
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.analysis.order_metrics import calc_order_metrics, calc_color_order_breakdown


@pytest.fixture(scope="module")
def metrics():
    return calc_order_metrics()


def test_metrics_columns(metrics):
    required = [
        "product_id", "product_name", "category",
        "trend_coef", "safety_stock", "recommended_order",
        "urgency", "order_amount",
    ]
    for col in required:
        assert col in metrics.columns, f"列 '{col}' が存在しません"


def test_no_negative_recommended_order(metrics):
    neg = (metrics["recommended_order"] < 0).sum()
    assert neg == 0, f"推奨発注数が負の商品があります: {neg} 件"


def test_no_negative_safety_stock(metrics):
    neg = (metrics["safety_stock"] < 0).sum()
    assert neg == 0, f"安全在庫数が負の商品があります: {neg} 件"


def test_urgency_values(metrics):
    valid = {"緊急", "注意", "通常"}
    actual = set(metrics["urgency"].unique())
    assert actual.issubset(valid), f"不正な緊急度値: {actual - valid}"


def test_trend_coef_range(metrics):
    assert (metrics["trend_coef"] >= 0.1).all(), "トレンド係数が最小値を下回っています"
    assert (metrics["trend_coef"] <= 5.0).all(), "トレンド係数が最大値を超えています"


def test_zero_division_protection(metrics):
    # 日平均0の商品で在庫日数が9999になること
    zero_sales = metrics[metrics["avg_daily_30d"] == 0]
    if not zero_sales.empty:
        assert (zero_sales["stock_days"] == 9999).all()


def test_order_amount_consistency(metrics):
    # 発注金額 = 推奨発注数 × 原価
    expected = metrics["recommended_order"] * metrics["unit_cost"]
    assert (metrics["order_amount"] == expected).all()


def test_custom_cycle_days():
    df_30 = calc_order_metrics(order_cycle_days=30)
    df_60 = calc_order_metrics(order_cycle_days=60)
    # 発注サイクルが長いほど推奨発注数の合計が多くなる傾向
    assert df_60["recommended_order"].sum() >= df_30["recommended_order"].sum() * 0.9


def test_color_breakdown(metrics):
    color_df = calc_color_order_breakdown(metrics)
    assert "product_id" in color_df.columns
    assert "color" in color_df.columns
    assert "recommended_order_color" in color_df.columns
    assert (color_df["recommended_order_color"] >= 0).all()

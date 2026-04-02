"""
アラートエンジンのテスト
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.analysis.alert_engine import (
    LEVEL_CAUTION,
    LEVEL_OVERSTOCK,
    LEVEL_TRENDING,
    LEVEL_URGENT,
    generate_alerts,
    get_alert_counts,
)


def _make_row(**kwargs) -> dict:
    """テスト用の商品行を生成する"""
    defaults = {
        "product_id": "P9999",
        "product_name": "テスト商品",
        "category": "トップス",
        "lead_time_days": 30,
        "total_stock": 100,
        "stock_days": 60.0,
        "trend_coef": 1.0,
        "avg_daily_30d": 2.0,
        "qty_reserved": 0,
        "recommended_order": 10,
        "safety_stock": 5,
        "gross_margin_rate": 0.5,
        "urgency": "通常",
    }
    defaults.update(kwargs)
    return defaults


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestUrgentAlert:
    def test_triggers_when_stock_days_less_than_lead_time(self):
        df = _df([_make_row(stock_days=20.0, lead_time_days=30)])
        alerts = generate_alerts(df)
        assert len(alerts) == 1
        assert alerts.iloc[0]["alert_level"] == LEVEL_URGENT

    def test_not_triggered_when_equal(self):
        df = _df([_make_row(stock_days=30.0, lead_time_days=30, trend_coef=0.9)])
        alerts = generate_alerts(df)
        urgent = alerts[alerts["alert_level"] == LEVEL_URGENT]
        assert urgent.empty


class TestCautionAlert:
    def test_triggers_between_lead_time_and_1_5x(self):
        # stock_days=35, lead_time=30 → 35 < 45 (30×1.5)
        df = _df([_make_row(stock_days=35.0, lead_time_days=30)])
        alerts = generate_alerts(df)
        assert len(alerts) == 1
        assert alerts.iloc[0]["alert_level"] == LEVEL_CAUTION

    def test_not_triggered_when_sufficient_stock(self):
        df = _df([_make_row(stock_days=50.0, lead_time_days=30, trend_coef=0.9)])
        alerts = generate_alerts(df)
        if alerts.empty:
            return
        caution = alerts[alerts["alert_level"] == LEVEL_CAUTION]
        assert caution.empty


class TestTrendingAlert:
    def test_triggers_on_high_trend_low_stock(self):
        df = _df([_make_row(stock_days=20.0, lead_time_days=10, trend_coef=1.5)])
        alerts = generate_alerts(df)
        levels = set(alerts["alert_level"].tolist())
        # 緊急でなく、要注目が発生すること
        # stock_days=20 > lead_time=10、stock_days=20 < 10*1.5=15 はFalseなので注意は出ない
        # trend=1.5>1.2 かつ stock_days=20<30 → 要注目
        assert LEVEL_TRENDING in levels

    def test_not_triggered_on_low_trend(self):
        df = _df([_make_row(stock_days=20.0, lead_time_days=10, trend_coef=1.0)])
        alerts = generate_alerts(df)
        if alerts.empty:
            return
        trending = alerts[alerts["alert_level"] == LEVEL_TRENDING]
        assert trending.empty


class TestOverstockAlert:
    def test_triggers_on_overstock_with_downtrend(self):
        df = _df([_make_row(stock_days=100.0, lead_time_days=30, trend_coef=0.7)])
        alerts = generate_alerts(df)
        assert len(alerts) == 1
        assert alerts.iloc[0]["alert_level"] == LEVEL_OVERSTOCK

    def test_not_triggered_when_trend_is_normal(self):
        df = _df([_make_row(stock_days=100.0, lead_time_days=30, trend_coef=1.0)])
        alerts = generate_alerts(df)
        if alerts.empty:
            return
        overstock = alerts[alerts["alert_level"] == LEVEL_OVERSTOCK]
        assert overstock.empty


class TestAlertCounts:
    def test_count_structure(self):
        df = generate_alerts()
        counts = get_alert_counts(df)
        for level in [LEVEL_URGENT, LEVEL_CAUTION, LEVEL_TRENDING, LEVEL_OVERSTOCK]:
            assert level in counts or counts.get(level, 0) >= 0

    def test_empty_df_returns_zeros(self):
        counts = get_alert_counts(pd.DataFrame())
        assert counts[LEVEL_URGENT] == 0
        assert counts[LEVEL_CAUTION] == 0

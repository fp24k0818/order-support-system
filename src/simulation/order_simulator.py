"""
発注シミュレーションモジュール
複数シナリオでの推奨発注数・発注金額を比較する
"""

from typing import Optional

import numpy as np
import pandas as pd

from src.analysis.order_metrics import calc_order_metrics


SCENARIOS = {
    "売上重視": {
        "safety_factor": 1.5,
        "apply_trend": True,
        "order_cycle_days": 30,
    },
    "利益重視": {
        "safety_factor": 1.2,
        "apply_trend": False,
        "order_cycle_days": 30,
    },
    "バランス": {
        "safety_factor": 1.2,
        "apply_trend": True,
        "order_cycle_days": 30,
    },
}


def run_scenario(
    scenario_name: str,
    safety_factor: Optional[float] = None,
    apply_trend: Optional[bool] = None,
    order_cycle_days: Optional[int] = None,
    budget_limit: Optional[float] = None,
) -> pd.DataFrame:
    """
    指定シナリオで発注計算を実行する。

    Parameters
    ----------
    scenario_name : str
        シナリオ名（"売上重視" / "利益重視" / "バランス" / "カスタム"）
    safety_factor : float, optional
        安全係数（Noneの場合はシナリオ定義値を使用）
    apply_trend : bool, optional
        トレンド係数反映有無
    order_cycle_days : int, optional
        発注サイクル日数
    budget_limit : float, optional
        予算上限（円）。設定した場合は粗利率の高い商品から予算消化

    Returns
    -------
    pd.DataFrame
        商品ごとの発注数・発注金額
    """
    base = SCENARIOS.get(scenario_name, SCENARIOS["バランス"])
    sf = safety_factor if safety_factor is not None else base["safety_factor"]
    at = apply_trend if apply_trend is not None else base["apply_trend"]
    ocd = order_cycle_days if order_cycle_days is not None else base["order_cycle_days"]

    df = calc_order_metrics(
        order_cycle_days=ocd,
        safety_factor=sf,
        apply_trend=at,
    )

    # 利益重視: 粗利率上位30%は発注数を1.3倍
    if scenario_name == "利益重視":
        threshold = df["gross_margin_rate"].quantile(0.7)
        df.loc[df["gross_margin_rate"] >= threshold, "recommended_order"] = (
            df.loc[df["gross_margin_rate"] >= threshold, "recommended_order"] * 1.3
        ).round(0).astype(int)
        df["order_amount"] = df["recommended_order"] * df["unit_cost"]

    # 予算上限適用
    if budget_limit is not None and budget_limit > 0:
        df = _apply_budget_limit(df, budget_limit)

    df["scenario"] = scenario_name
    return df


def _apply_budget_limit(df: pd.DataFrame, budget: float) -> pd.DataFrame:
    """
    予算上限内に収まるよう粗利率の高い商品から発注数を調整する。

    Parameters
    ----------
    df : pd.DataFrame
    budget : float
        予算上限（円）

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy().sort_values("gross_margin_rate", ascending=False)
    remaining = budget
    adj_orders = []
    adj_amounts = []

    for _, row in df.iterrows():
        if row["recommended_order"] == 0 or row["unit_cost"] == 0:
            adj_orders.append(0)
            adj_amounts.append(0)
            continue

        max_qty = int(remaining // row["unit_cost"])
        qty = min(row["recommended_order"], max_qty)
        amount = qty * row["unit_cost"]
        remaining -= amount
        adj_orders.append(qty)
        adj_amounts.append(amount)

    df["recommended_order"] = adj_orders
    df["order_amount"] = adj_amounts
    return df.sort_values("product_id")


def compare_scenarios(
    safety_factor: Optional[float] = None,
    apply_trend: Optional[bool] = None,
    order_cycle_days: Optional[int] = None,
    budget_limit: Optional[float] = None,
) -> pd.DataFrame:
    """
    3シナリオの比較DataFrameを返す。

    Returns
    -------
    pd.DataFrame
        columns: product_id, product_name, category,
                 recommended_order_<シナリオ>, order_amount_<シナリオ>, ...
    """
    results = []
    for scenario_name in SCENARIOS:
        df = run_scenario(
            scenario_name,
            safety_factor=safety_factor,
            apply_trend=apply_trend,
            order_cycle_days=order_cycle_days,
            budget_limit=budget_limit,
        )
        df = df.rename(columns={
            "recommended_order": f"発注数_{scenario_name}",
            "order_amount": f"発注金額_{scenario_name}",
        })
        results.append(df)

    base = results[0][["product_id", "product_name", "category",
                        "urgency", "stock_days",
                        f"発注数_{list(SCENARIOS.keys())[0]}",
                        f"発注金額_{list(SCENARIOS.keys())[0]}"]].copy()

    for i, name in enumerate(list(SCENARIOS.keys())[1:], start=1):
        base = base.merge(
            results[i][["product_id",
                         f"発注数_{name}",
                         f"発注金額_{name}"]],
            on="product_id",
            how="left",
        )

    return base


def get_scenario_totals(
    safety_factor: Optional[float] = None,
    apply_trend: Optional[bool] = None,
    order_cycle_days: Optional[int] = None,
    budget_limit: Optional[float] = None,
) -> pd.DataFrame:
    """
    シナリオ別の合計発注金額・発注数サマリーを返す。

    Returns
    -------
    pd.DataFrame
        columns: scenario, total_order_qty, total_order_amount
    """
    rows = []
    for name in SCENARIOS:
        df = run_scenario(
            name,
            safety_factor=safety_factor,
            apply_trend=apply_trend,
            order_cycle_days=order_cycle_days,
            budget_limit=budget_limit,
        )
        rows.append({
            "シナリオ": name,
            "合計発注数": int(df["recommended_order"].sum()),
            "合計発注金額(円)": int(df["order_amount"].sum()),
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    print("=== シナリオ別合計 ===")
    totals = get_scenario_totals()
    print(totals.to_string(index=False))

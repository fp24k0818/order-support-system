"""
発注判断指標計算モジュール
SQLiteから集計データを取得し、発注に必要な指標を算出する
"""

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

DB_PATH = Path(__file__).parent.parent.parent / "data" / "db" / "order_support.db"


def _get_conn() -> sqlite3.Connection:
    """DB接続を返す"""
    return sqlite3.connect(DB_PATH)


def get_product_summary() -> pd.DataFrame:
    """
    v_product_summary ビューを取得する。

    Returns
    -------
    pd.DataFrame
        商品ごとの集計（販売数・在庫・予約・粗利率など）
    """
    with _get_conn() as conn:
        df = pd.read_sql("SELECT * FROM v_product_summary", conn)
    return df


def get_color_breakdown() -> pd.DataFrame:
    """
    v_color_breakdown ビューを取得する。

    Returns
    -------
    pd.DataFrame
        商品×色ごとの販売比率・在庫・予約数
    """
    with _get_conn() as conn:
        df = pd.read_sql("SELECT * FROM v_color_breakdown", conn)
    return df


def get_daily_sales(product_id: str, days: int = 90) -> pd.DataFrame:
    """
    指定商品の日別売上を取得する。

    Parameters
    ----------
    product_id : str
        商品ID
    days : int
        取得する過去日数

    Returns
    -------
    pd.DataFrame
        columns: sale_date, quantity, sale_amount
    """
    sql = """
        SELECT sale_date, SUM(quantity) AS quantity, SUM(sale_amount) AS sale_amount
        FROM sales
        WHERE product_id = ?
          AND sale_date >= DATE('now', ? || ' days')
        GROUP BY sale_date
        ORDER BY sale_date
    """
    with _get_conn() as conn:
        df = pd.read_sql(sql, conn, params=[product_id, -days])
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    return df


def calc_order_metrics(
    order_cycle_days: int = 30,
    safety_factor: float = 1.2,
    apply_trend: bool = True,
) -> pd.DataFrame:
    """
    発注判断指標を計算する。

    Parameters
    ----------
    order_cycle_days : int
        発注サイクル日数（デフォルト30日）
    safety_factor : float
        安全係数（デフォルト1.2）
    apply_trend : bool
        トレンド係数を推奨発注数に反映するか

    Returns
    -------
    pd.DataFrame
        商品ごとの発注判断指標を含むDataFrame
    """
    df = get_product_summary()

    # --- トレンド係数 ---
    # 直近30日の日平均 ÷ 直近90日の日平均
    df["trend_coef"] = np.where(
        df["avg_daily_90d"] > 0,
        (df["avg_daily_30d"] / df["avg_daily_90d"]).round(3),
        1.0,
    )
    df["trend_coef"] = df["trend_coef"].clip(0.1, 5.0)

    # --- 在庫回転率（90日） ---
    df["turnover_rate_90d"] = np.where(
        df["total_stock"] > 0,
        (df["qty_90d"] / df["total_stock"]).round(3),
        np.nan,
    )

    # --- 安全在庫数 ---
    daily_base = df["avg_daily_30d"]
    df["safety_stock"] = (
        daily_base * df["lead_time_days"] * safety_factor
    ).clip(lower=0).round(0).astype(int)

    # --- 推奨発注数 ---
    effective_daily = (
        daily_base * df["trend_coef"] if apply_trend else daily_base
    )
    df["recommended_order"] = (
        effective_daily * order_cycle_days
        + df["safety_stock"]
        - df["total_stock"]
        - df["qty_reserved"]
    ).clip(lower=0).round(0).astype(int)

    # --- 緊急度フラグ ---
    df["urgency"] = "通常"
    df.loc[
        df["stock_days"] < df["lead_time_days"] * 1.5, "urgency"
    ] = "注意"
    df.loc[
        df["stock_days"] < df["lead_time_days"], "urgency"
    ] = "緊急"

    # --- 発注金額 ---
    df["order_amount"] = df["recommended_order"] * df["unit_cost"]

    return df


def calc_color_order_breakdown(
    metrics_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    色別の推奨発注内訳を計算する。

    Parameters
    ----------
    metrics_df : pd.DataFrame
        calc_order_metrics() の結果

    Returns
    -------
    pd.DataFrame
        columns: product_id, color, sales_ratio, recommended_order_color,
                 current_stock, reserved_qty, stock_fill_rate
    """
    color_df = get_color_breakdown()

    merged = color_df.merge(
        metrics_df[["product_id", "recommended_order"]],
        on="product_id",
        how="left",
    )

    # 色別推奨発注数（販売比率で按分）
    merged["recommended_order_color"] = (
        merged["recommended_order"] * merged["sales_ratio"]
    ).fillna(0).clip(lower=0).round(0).astype(int)

    # 在庫充足率（現在庫 ÷ 推奨発注数、ゼロ除算防止）
    merged["stock_fill_rate"] = np.where(
        merged["recommended_order_color"] > 0,
        (merged["current_stock"] / merged["recommended_order_color"]).round(3),
        np.nan,
    )

    return merged


def get_alert_summary(metrics_df: pd.DataFrame) -> dict:
    """
    アラートサマリーを返す。

    Parameters
    ----------
    metrics_df : pd.DataFrame
        calc_order_metrics() の結果

    Returns
    -------
    dict
        urgent_count, caution_count, normal_count
    """
    counts = metrics_df["urgency"].value_counts()
    return {
        "urgent_count": int(counts.get("緊急", 0)),
        "caution_count": int(counts.get("注意", 0)),
        "normal_count": int(counts.get("通常", 0)),
    }


if __name__ == "__main__":
    print("=== 発注判断指標サンプル ===")
    df = calc_order_metrics()
    print(df[["product_id", "product_name", "stock_days", "urgency",
              "recommended_order", "order_amount"]].head(10).to_string(index=False))
    print(f"\n緊急: {(df['urgency']=='緊急').sum()} 件")
    print(f"注意: {(df['urgency']=='注意').sum()} 件")
    print(f"通常: {(df['urgency']=='通常').sum()} 件")

"""
在庫補充アラートエンジン
在庫状況・トレンドに基づいてアラートを生成する
"""

import pandas as pd

from src.analysis.order_metrics import calc_order_metrics

# アラートレベル定義
LEVEL_URGENT = "🔴 緊急"
LEVEL_CAUTION = "🟡 注意"
LEVEL_TRENDING = "🟠 要注目"
LEVEL_OVERSTOCK = "🔵 情報"

LEVEL_ORDER = {LEVEL_URGENT: 0, LEVEL_CAUTION: 1, LEVEL_TRENDING: 2, LEVEL_OVERSTOCK: 3}


def generate_alerts(metrics_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    在庫補充アラートを生成する。

    Parameters
    ----------
    metrics_df : pd.DataFrame, optional
        calc_order_metrics() の結果。Noneの場合は内部で取得。

    Returns
    -------
    pd.DataFrame
        columns: product_id, product_name, category, alert_level,
                 reason, recommended_action, stock_days, lead_time_days,
                 trend_coef, total_stock, recommended_order
    """
    if metrics_df is None:
        metrics_df = calc_order_metrics()

    alerts = []

    for _, row in metrics_df.iterrows():
        stock_days = row["stock_days"]
        lead_time = row["lead_time_days"]
        trend = row["trend_coef"]
        pid = row["product_id"]
        name = row["product_name"]
        cat = row["category"]

        # ルール1: 在庫切れ緊急アラート
        if stock_days < lead_time:
            alerts.append(_make_alert(
                row,
                LEVEL_URGENT,
                f"在庫日数 {stock_days:.1f}日 < リードタイム {lead_time}日",
                "至急発注手配を行ってください。代替調達も検討してください。",
            ))
            continue  # 最も深刻なルールが適用されたら以降はスキップ

        # ルール2: 在庫補充推奨アラート
        if stock_days < lead_time * 1.5:
            alerts.append(_make_alert(
                row,
                LEVEL_CAUTION,
                f"在庫日数 {stock_days:.1f}日 < リードタイム×1.5 ({lead_time * 1.5:.0f}日)",
                "早めの発注を検討してください。",
            ))
            continue

        # ルール3: 売れ筋品薄アラート（上記より優先度低）
        if trend > 1.2 and stock_days < 30:
            alerts.append(_make_alert(
                row,
                LEVEL_TRENDING,
                f"上昇トレンド（係数 {trend:.2f}）かつ在庫日数 {stock_days:.1f}日 < 30日",
                "トレンド上昇中のため、多めの発注を検討してください。",
            ))
            continue

        # ルール4: 過剰在庫アラート
        if stock_days > 90 and trend < 0.8:
            alerts.append(_make_alert(
                row,
                LEVEL_OVERSTOCK,
                f"在庫日数 {stock_days:.1f}日 > 90日 かつ 下降トレンド（係数 {trend:.2f}）",
                "販売促進またはセール価格での在庫消化を検討してください。",
            ))

    df = pd.DataFrame(alerts)
    if df.empty:
        return df

    df["_level_order"] = df["alert_level"].map(LEVEL_ORDER)
    df = df.sort_values("_level_order").drop(columns="_level_order")
    return df.reset_index(drop=True)


def _make_alert(row: pd.Series, level: str, reason: str, action: str) -> dict:
    return {
        "product_id": row["product_id"],
        "product_name": row["product_name"],
        "category": row["category"],
        "alert_level": level,
        "reason": reason,
        "recommended_action": action,
        "stock_days": row["stock_days"],
        "lead_time_days": row["lead_time_days"],
        "trend_coef": row["trend_coef"],
        "total_stock": row["total_stock"],
        "recommended_order": row["recommended_order"],
    }


def get_alert_counts(alerts_df: pd.DataFrame) -> dict:
    """
    アラート件数サマリーを返す。

    Returns
    -------
    dict
        {level: count, ...}
    """
    if alerts_df.empty:
        return {LEVEL_URGENT: 0, LEVEL_CAUTION: 0,
                LEVEL_TRENDING: 0, LEVEL_OVERSTOCK: 0}
    return alerts_df["alert_level"].value_counts().to_dict()


if __name__ == "__main__":
    alerts = generate_alerts()
    print(f"アラート総件数: {len(alerts)}")
    counts = get_alert_counts(alerts)
    for level, cnt in counts.items():
        print(f"  {level}: {cnt} 件")
    print()
    print(alerts[["product_name", "alert_level", "reason"]].to_string(index=False))

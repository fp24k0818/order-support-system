"""
商品発注判断支援システム — Streamlit ダッシュボード
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# パス解決
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from src.analysis.order_metrics import (
    calc_color_order_breakdown,
    calc_order_metrics,
    get_daily_sales,
)
from src.simulation.order_simulator import (
    compare_scenarios,
    get_scenario_totals,
    run_scenario,
)
from src.analysis.alert_engine import generate_alerts, get_alert_counts

st.set_page_config(
    page_title="商品発注判断支援システム",
    page_icon="📦",
    layout="wide",
)

# ─── データ読み込み（キャッシュ）─────────────────────────────────────────
@st.cache_data(ttl=300)
def load_metrics(order_cycle: int = 30, safety_factor: float = 1.2,
                 apply_trend: bool = True) -> pd.DataFrame:
    return calc_order_metrics(
        order_cycle_days=order_cycle,
        safety_factor=safety_factor,
        apply_trend=apply_trend,
    )


@st.cache_data(ttl=300)
def load_color_breakdown(order_cycle: int, safety_factor: float,
                          apply_trend: bool) -> pd.DataFrame:
    df = load_metrics(order_cycle, safety_factor, apply_trend)
    return calc_color_order_breakdown(df)


@st.cache_data(ttl=300)
def load_daily_sales(product_id: str, days: int = 90) -> pd.DataFrame:
    return get_daily_sales(product_id, days)


# ─── サイドバー ──────────────────────────────────────────────────────────
st.sidebar.title("📦 発注判断支援")
page = st.sidebar.radio(
    "ページ選択",
    ["🏠 全体概況", "🔍 商品別詳細", "📋 発注一覧", "🧮 発注シミュレーション"],
)

# ─── ページ1：全体概況 ───────────────────────────────────────────────────
if page == "🏠 全体概況":
    st.title("🏠 全体概況")

    df = load_metrics()

    # ── アラートセクション ──────────────────────────────────────────────
    @st.cache_data(ttl=300)
    def load_alerts():
        return generate_alerts()

    alerts_df = load_alerts()
    counts = get_alert_counts(alerts_df)

    st.subheader("🚨 在庫アラート")
    ac1, ac2, ac3, ac4 = st.columns(4)
    ac1.metric("🔴 緊急", f"{counts.get('🔴 緊急', 0)} 件")
    ac2.metric("🟡 注意", f"{counts.get('🟡 注意', 0)} 件")
    ac3.metric("🟠 要注目", f"{counts.get('🟠 要注目', 0)} 件")
    ac4.metric("🔵 情報（過剰在庫）", f"{counts.get('🔵 情報', 0)} 件")

    alert_level_filter = st.selectbox(
        "アラートレベルでフィルタ",
        ["すべて", "🔴 緊急", "🟡 注意", "🟠 要注目", "🔵 情報"],
    )
    filtered_alerts = alerts_df if alert_level_filter == "すべて" \
        else alerts_df[alerts_df["alert_level"] == alert_level_filter]

    if filtered_alerts.empty:
        st.info("該当するアラートはありません。")
    else:
        alert_disp = filtered_alerts[[
            "alert_level", "product_name", "category",
            "stock_days", "trend_coef", "reason", "recommended_action",
        ]].rename(columns={
            "alert_level": "レベル", "product_name": "商品名",
            "category": "カテゴリ", "stock_days": "在庫日数",
            "trend_coef": "トレンド係数", "reason": "理由",
            "recommended_action": "推奨アクション",
        })
        st.dataframe(alert_disp, use_container_width=True, hide_index=True)

    st.divider()

    # KPI カード
    total_products = len(df)
    near_stockout = int((df["stock_days"] < 30).sum())
    urgent = int((df["urgency"] == "緊急").sum())
    avg_stock_days = df.loc[df["stock_days"] < 9999, "stock_days"].mean()
    avg_stock_days_str = f"{avg_stock_days:.1f} 日" if not pd.isna(avg_stock_days) else "N/A"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総商品数", f"{total_products} 商品")
    c2.metric("在庫切れ間近（30日未満）", f"{near_stockout} 商品", delta=None)
    c3.metric("緊急発注が必要な商品数", f"{urgent} 商品",
              delta=f"{'要対応' if urgent > 0 else '問題なし'}")
    c4.metric("平均在庫日数", avg_stock_days_str)

    st.divider()

    # 緊急発注テーブル
    st.subheader("🔴 緊急発注が必要な商品")
    urgent_df = df[df["urgency"] == "緊急"].sort_values("stock_days").copy()
    if urgent_df.empty:
        st.success("緊急発注が必要な商品はありません。")
    else:
        display_cols = {
            "product_id": "商品ID",
            "product_name": "商品名",
            "category": "カテゴリ",
            "stock_days": "在庫日数",
            "lead_time_days": "リードタイム",
            "total_stock": "現在庫",
            "recommended_order": "推奨発注数",
            "order_amount": "発注金額(円)",
        }
        st.dataframe(
            urgent_df[display_cols.keys()].rename(columns=display_cols),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # カテゴリ別在庫状況
    st.subheader("📊 カテゴリ別・緊急度別 商品数")
    cat_urgency = (
        df.groupby(["category", "urgency"])
        .size()
        .reset_index(name="count")
    )
    urgency_order = ["緊急", "注意", "通常"]
    color_map = {"緊急": "#EF4444", "注意": "#F59E0B", "通常": "#10B981"}
    fig = px.bar(
        cat_urgency,
        x="category", y="count", color="urgency",
        category_orders={"urgency": urgency_order},
        color_discrete_map=color_map,
        labels={"category": "カテゴリ", "count": "商品数", "urgency": "緊急度"},
        barmode="stack",
    )
    st.plotly_chart(fig, use_container_width=True)


# ─── ページ2：商品別詳細 ─────────────────────────────────────────────────
elif page == "🔍 商品別詳細":
    st.title("🔍 商品別詳細")

    df = load_metrics()
    product_options = {
        f"{row['product_id']} — {row['product_name']}": row["product_id"]
        for _, row in df.iterrows()
    }
    selected_label = st.selectbox("商品を選択", list(product_options.keys()))
    selected_id = product_options[selected_label]
    row = df[df["product_id"] == selected_id].iloc[0]

    # 基本情報カード
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("カテゴリ", row["category"])
    c2.metric("原価", f"¥{row['unit_cost']:,}")
    c3.metric("売価", f"¥{row['selling_price']:,}")
    c4.metric("粗利率", f"{row['gross_margin_rate'] * 100:.1f}%")

    st.divider()

    col_left, col_right = st.columns([3, 2])

    with col_left:
        # 売上推移
        st.subheader("📈 日別売上推移（直近90日）")
        daily = load_daily_sales(selected_id, 90)
        if daily.empty:
            st.info("売上データがありません。")
        else:
            fig = px.line(
                daily, x="sale_date", y="quantity",
                labels={"sale_date": "日付", "quantity": "販売数量"},
            )
            fig.update_traces(line_color="#3B82F6")
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        # 色別販売構成
        st.subheader("🎨 色別販売構成（直近90日）")
        color_df = load_color_breakdown(30, 1.2, True)
        color_row = color_df[color_df["product_id"] == selected_id]
        if color_row.empty:
            st.info("色別データがありません。")
        else:
            fig = px.pie(
                color_row, names="color", values="qty_total",
                hole=0.4,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 在庫・予約テーブル（色別）
    st.subheader("📦 在庫・予約状況（色別）")
    if not color_row.empty:
        disp = color_row[["color", "current_stock", "reserved_qty",
                           "sales_ratio", "recommended_order_color"]].copy()
        disp.columns = ["色", "現在庫", "予約数", "販売比率", "推奨発注数(色別)"]
        disp["販売比率"] = (disp["販売比率"] * 100).round(1).astype(str) + "%"
        st.dataframe(disp, use_container_width=True, hide_index=True)

    st.divider()

    # 発注判断指標
    st.subheader("🎯 発注判断指標")
    m1, m2, m3 = st.columns(3)
    m1.metric("日平均販売数（30日）", f"{row['avg_daily_30d']:.2f} 個/日")
    m2.metric("トレンド係数", f"{row['trend_coef']:.2f}",
              delta=f"{'上昇' if row['trend_coef'] >= 1.0 else '下降'}トレンド")
    m3.metric("在庫日数", f"{row['stock_days'] if row['stock_days'] < 9999 else '∞'} 日",
              delta=row["urgency"])

    m4, m5, m6 = st.columns(3)
    m4.metric("安全在庫数", f"{row['safety_stock']:,} 個")
    m5.metric("推奨発注数", f"{row['recommended_order']:,} 個")
    m6.metric("推奨発注金額", f"¥{row['order_amount']:,}")


# ─── ページ3：発注一覧 ───────────────────────────────────────────────────
elif page == "📋 発注一覧":
    st.title("📋 発注一覧")

    df = load_metrics()

    # フィルタ
    fc1, fc2 = st.columns(2)
    with fc1:
        categories = ["すべて"] + sorted(df["category"].unique().tolist())
        sel_cat = st.selectbox("カテゴリ", categories)
    with fc2:
        urgencies = ["すべて", "緊急", "注意", "通常"]
        sel_urgency = st.selectbox("緊急度", urgencies)

    filtered = df.copy()
    if sel_cat != "すべて":
        filtered = filtered[filtered["category"] == sel_cat]
    if sel_urgency != "すべて":
        filtered = filtered[filtered["urgency"] == sel_urgency]

    filtered = filtered.sort_values("stock_days")

    display_cols = {
        "product_id": "商品ID",
        "product_name": "商品名",
        "category": "カテゴリ",
        "urgency": "緊急度",
        "stock_days": "在庫日数",
        "lead_time_days": "リードタイム",
        "total_stock": "現在庫",
        "qty_reserved": "予約数",
        "avg_daily_30d": "日平均販売数",
        "safety_stock": "安全在庫数",
        "recommended_order": "推奨発注数",
        "order_amount": "発注金額(円)",
        "gross_margin_rate": "粗利率",
    }

    out_df = filtered[display_cols.keys()].rename(columns=display_cols).copy()
    out_df["粗利率"] = (out_df["粗利率"] * 100).round(1).astype(str) + "%"
    out_df["在庫日数"] = out_df["在庫日数"].apply(
        lambda x: "∞" if x >= 9999 else f"{x:.1f}"
    )

    st.dataframe(out_df, use_container_width=True, hide_index=True)
    st.caption(f"表示件数: {len(out_df)} 件")

    # CSVダウンロード
    csv = out_df.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label="📥 CSVダウンロード",
        data=csv.encode("utf-8-sig"),
        file_name="order_list.csv",
        mime="text/csv",
    )


# ─── ページ4：発注シミュレーション ──────────────────────────────────────
elif page == "🧮 発注シミュレーション":
    st.title("🧮 発注シミュレーション")

    st.sidebar.subheader("パラメータ設定")
    sf = st.sidebar.slider("安全係数", 0.5, 2.0, 1.2, 0.1)
    ocd = st.sidebar.number_input("発注サイクル日数", 7, 90, 30, 1)
    at = st.sidebar.checkbox("トレンド係数を反映する", value=True)
    budget = st.sidebar.number_input(
        "予算上限（円）※0=制限なし", min_value=0, max_value=100_000_000,
        value=0, step=100_000,
    )
    budget_limit = float(budget) if budget > 0 else None

    # シナリオ比較テーブル
    st.subheader("📊 シナリオ別 発注推奨一覧")
    compare_df = compare_scenarios(
        safety_factor=sf, apply_trend=at,
        order_cycle_days=ocd, budget_limit=budget_limit,
    )
    st.dataframe(compare_df, use_container_width=True, hide_index=True)

    st.divider()

    # 合計金額棒グラフ
    st.subheader("💰 シナリオ別 合計発注金額")
    totals = get_scenario_totals(
        safety_factor=sf, apply_trend=at,
        order_cycle_days=ocd, budget_limit=budget_limit,
    )
    fig = px.bar(
        totals, x="シナリオ", y="合計発注金額(円)",
        color="シナリオ",
        color_discrete_sequence=["#EF4444", "#3B82F6", "#10B981"],
        text="合計発注金額(円)",
    )
    fig.update_traces(texttemplate="¥%{text:,.0f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(totals, use_container_width=True, hide_index=True)

    st.divider()

    # シナリオ選択して発注リストDL
    st.subheader("📥 発注リストのダウンロード")
    sel_scenario = st.selectbox("ダウンロードするシナリオ", ["売上重視", "利益重視", "バランス"])
    dl_df = run_scenario(
        sel_scenario,
        safety_factor=sf, apply_trend=at,
        order_cycle_days=ocd, budget_limit=budget_limit,
    )
    dl_cols = {
        "product_id": "商品ID", "product_name": "商品名", "category": "カテゴリ",
        "urgency": "緊急度", "recommended_order": "推奨発注数",
        "order_amount": "発注金額(円)",
    }
    dl_out = dl_df[dl_cols.keys()].rename(columns=dl_cols)
    csv_dl = dl_out.to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        label=f"📥 {sel_scenario}シナリオ CSVダウンロード",
        data=csv_dl.encode("utf-8-sig"),
        file_name=f"order_{sel_scenario}.csv",
        mime="text/csv",
    )

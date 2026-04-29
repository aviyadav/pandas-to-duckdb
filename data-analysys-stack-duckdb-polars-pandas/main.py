import duckdb
import polars as pl
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for saving to file
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier

# STEP 1: DuckDB ingests and pre-filters parquet on disk
print("Step 1: Loading and filtering data with DuckDB...")
raw = duckdb.sql("""
    SELECT user_id, event_type, amount, created_at 
    FROM 'data/events_*.parquet'
    WHERE created_at >= '2025-01-01'
""").pl()  # → convert to Polars, zero-copy

print(f"  Loaded {len(raw):,} rows after filtering.")

# STEP 2: Polars handles complex multi-column transformations fast
print("Step 2: Feature engineering with Polars...")
features = (
    raw
    .with_columns([
        pl.col("created_at").dt.hour().alias("hour"),
        pl.col("created_at").dt.weekday().alias("weekday"),
        (pl.col("amount") / pl.col("amount").mean()).alias("rel_amount"),
    ])
    .group_by("event_type")
    .agg([
        pl.col("amount").mean().alias("avg_amount"),
        pl.col("amount").std().alias("std_amount"),
        pl.col("amount").median().alias("median_amount"),
        pl.col("user_id").n_unique().alias("unique_users"),
        pl.col("hour").mean().alias("avg_hour"),
        pl.col("weekday").mean().alias("avg_weekday"),
        pl.len().alias("event_count"),
    ])
)

print(f"  Generated {len(features)} feature rows (one per event_type).")
print(features)

# STEP 3: Tiny final result → Pandas → sklearn
print("Step 3: Training model with sklearn...")
X = features.to_pandas().drop("event_type", axis=1)
# Create dummy binary labels for the demo
labels = np.random.randint(0, 2, size=len(X))
model = RandomForestClassifier(n_estimators=50, random_state=42).fit(X, labels)
print("  Training completed!")

# Feature importance from the model
importances = model.feature_importances_
feature_names = X.columns.tolist()

# STEP 4: Visualisation — save charts to output/
print("Step 4: Generating charts...")

fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle("Event Data Analysis Dashboard", fontsize=18, fontweight="bold")

# --- Chart 1: Average Amount by Event Type (bar chart) ---
ax1 = axes[0, 0]
feat_pd = features.to_pandas().sort_values("avg_amount", ascending=False)
colors = plt.cm.viridis(np.linspace(0.2, 0.8, len(feat_pd)))
bars = ax1.bar(feat_pd["event_type"], feat_pd["avg_amount"], color=colors, edgecolor="black", linewidth=0.5)
ax1.set_title("Average Amount by Event Type", fontsize=13, fontweight="bold")
ax1.set_ylabel("Average Amount ($)")
ax1.set_xlabel("Event Type")
ax1.bar_label(bars, fmt="%.1f", fontsize=8)
ax1.tick_params(axis="x", rotation=30)

# --- Chart 2: Event Count Distribution (horizontal bar) ---
ax2 = axes[0, 1]
feat_sorted = feat_pd.sort_values("event_count", ascending=True)
bar_colors = plt.cm.plasma(np.linspace(0.2, 0.8, len(feat_sorted)))
hbars = ax2.barh(feat_sorted["event_type"], feat_sorted["event_count"], color=bar_colors, edgecolor="black", linewidth=0.5)
ax2.set_title("Event Count by Type", fontsize=13, fontweight="bold")
ax2.set_xlabel("Count")
ax2.bar_label(hbars, fmt="%,.0f", fontsize=8, padding=3)

# --- Chart 3: Unique Users per Event Type (pie chart) ---
ax3 = axes[1, 0]
explode = [0.03] * len(feat_pd)
wedge_colors = plt.cm.Set2(np.linspace(0, 1, len(feat_pd)))
ax3.pie(
    feat_pd["unique_users"], labels=feat_pd["event_type"], autopct="%1.1f%%",
    startangle=140, colors=wedge_colors, explode=explode,
    textprops={"fontsize": 9}
)
ax3.set_title("Unique Users per Event Type", fontsize=13, fontweight="bold")

# --- Chart 4: Feature Importance from RandomForest ---
ax4 = axes[1, 1]
sorted_idx = np.argsort(importances)
imp_colors = plt.cm.coolwarm(np.linspace(0.2, 0.8, len(sorted_idx)))
ax4.barh(
    [feature_names[i] for i in sorted_idx],
    importances[sorted_idx],
    color=imp_colors, edgecolor="black", linewidth=0.5
)
ax4.set_title("Feature Importance (RandomForest)", fontsize=13, fontweight="bold")
ax4.set_xlabel("Importance")

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig("output/dashboard.png", dpi=150, bbox_inches="tight")
print("  Dashboard saved to output/dashboard.png")
plt.close()

print("\nDone!")
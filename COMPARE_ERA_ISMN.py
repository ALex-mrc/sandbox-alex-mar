
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

# ── Chemins ──────────────────────────────────────────────────────────────────
era_path  = "/home/alex/Documents/Projet_Stage/era5_soil_moisture_extracted.xlsx"
ismn_path = "/home/alex/Documents/Projet_Stage/PROJET/DATA/Stations_insitu/stations_SOD140_mai2021.xlsx"
out_dir   = "/home/alex/Documents/Projet_Stage/Plot"

# Correspondance profondeur ISMN (cm) → variable ERA5
# swvl1 : 0–7 cm | swvl2 : 7–28 cm | swvl3 : 28–100 cm
DEPTH_MAP = {
    5:  "swvl1",
    10: "swvl1",
    20: "swvl2",
    40: "swvl3",
    80: "swvl3",
}

# ── Chargement ────────────────────────────────────────────────────────────────
era  = pd.read_excel(era_path,  parse_dates=["date"])
ismn = pd.read_excel(ismn_path, parse_dates=["Date"])

# ── Moyennes journalières ─────────────────────────────────────────────────────
era_daily = (
    era.groupby(era["date"].dt.normalize())[["swvl1", "swvl2", "swvl3"]]
    .mean()
    .rename_axis("date")
)

ismn_daily = (
    ismn.groupby([ismn["Date"].dt.normalize(), "Profondeur_cm"])["Soil_moisture"]
    .mean()
    .rename_axis(["date", "Profondeur_cm"])
    .reset_index()
)

# ── Statistiques ──────────────────────────────────────────────────────────────
def compute_stats(ref, pred):
    """ref = ISMN, pred = ERA5."""
    diff  = pred - ref
    bias  = diff.mean()
    rmse  = np.sqrt((diff ** 2).mean())
    ubrmse = np.sqrt(((diff - bias) ** 2).mean())
    r     = np.corrcoef(ref, pred)[0, 1]
    return {"Biais": bias, "RMSE": rmse, "ubRMSE": ubrmse, "R": r, "N": len(ref)}

stats_rows = []
depths = sorted(DEPTH_MAP.keys())

# ── Figure : séries temporelles ───────────────────────────────────────────────
fig_ts, axes_ts = plt.subplots(len(depths), 1, figsize=(12, 3 * len(depths)), sharex=True)

# ── Figure : scatter ──────────────────────────────────────────────────────────
fig_sc, axes_sc = plt.subplots(1, len(depths), figsize=(4 * len(depths), 4))

for ax_ts, ax_sc, depth in zip(axes_ts, axes_sc, depths):
    era_var = DEPTH_MAP[depth]

    ismn_sel = ismn_daily[ismn_daily["Profondeur_cm"] == depth].set_index("date")["Soil_moisture"]
    era_sel  = era_daily[era_var]

    merged = pd.DataFrame({"ISMN": ismn_sel, "ERA5": era_sel}).dropna()
    if merged.empty:
        continue

    stats = compute_stats(merged["ISMN"], merged["ERA5"])
    stats_rows.append({"Profondeur_cm": depth, "ERA5_var": era_var, **stats})

    # Série temporelle
    ax_ts.plot(merged.index, merged["ISMN"], label="ISMN (SOD140)", color="steelblue", linewidth=1.2)
    ax_ts.plot(merged.index, merged["ERA5"], label=f"ERA5 ({era_var})", color="tomato",
               linewidth=1.2, linestyle="--")
    ax_ts.set_ylabel("θ (m³/m³)", fontsize=9)
    ax_ts.set_title(f"Profondeur {depth} cm  |  Biais={stats['Biais']:+.4f}  RMSE={stats['RMSE']:.4f}  R={stats['R']:.3f}",
                    fontsize=9)
    ax_ts.legend(fontsize=8)
    ax_ts.grid(True, linestyle="--", alpha=0.4)
    ax_ts.tick_params(axis="x", rotation=30)

    # Scatter
    vmin = min(merged["ISMN"].min(), merged["ERA5"].min()) - 0.005
    vmax = max(merged["ISMN"].max(), merged["ERA5"].max()) + 0.005
    ax_sc.scatter(merged["ISMN"], merged["ERA5"], s=20, alpha=0.7, color="steelblue", edgecolors="none")
    ax_sc.plot([vmin, vmax], [vmin, vmax], "k--", linewidth=0.8, label="1:1")
    ax_sc.set_xlabel("ISMN (m³/m³)", fontsize=9)
    ax_sc.set_ylabel("ERA5 (m³/m³)", fontsize=9)
    ax_sc.set_title(f"{depth} cm  R={stats['R']:.3f}", fontsize=9)
    ax_sc.set_xlim(vmin, vmax)
    ax_sc.set_ylim(vmin, vmax)
    ax_sc.set_aspect("equal", adjustable="box")
    ax_sc.legend(fontsize=7)
    ax_sc.grid(True, linestyle="--", alpha=0.4)

fig_ts.suptitle("Comparaison ERA5 vs ISMN (SOD140) — Mai 2021\n(moyennes journalières)", fontsize=11)
fig_ts.tight_layout()
fig_ts.savefig(f"{out_dir}/compare_timeseries_era5_ismn.png", dpi=150)
plt.close(fig_ts)

fig_sc.suptitle("Scatter ERA5 vs ISMN (SOD140) — Mai 2021", fontsize=11)
fig_sc.tight_layout()
fig_sc.savefig(f"{out_dir}/compare_scatter_era5_ismn.png", dpi=150)
plt.close(fig_sc)

# ── Tableau de synthèse ───────────────────────────────────────────────────────
df_stats = pd.DataFrame(stats_rows).round(4)
print("\n=== Statistiques ERA5 vs ISMN ===")
print(df_stats.to_string(index=False))
df_stats.to_excel(f"{out_dir}/stats_era5_vs_ismn.xlsx", index=False)
print(f"\nGraphiques et tableau sauvegardés dans : {out_dir}/")


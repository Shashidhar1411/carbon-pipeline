"""Where does the accuracy error come from?  Reads data/accuracy_results.csv written by eval.accuracy_check.

    python -m eval.accuracy_breakdown

Prints overall metrics, a breakdown by Pacific hour and by month, and the worst intervals.
MAPE = mean of |ours-caiso|/caiso per interval (unstable when CAISO's total is small, e.g. sunny middays).
WAPE = sum|ours-caiso| / sum(caiso): error weighted by how much CO2 there actually was.
bias = (sum ours - sum caiso) / sum caiso; negative = we compute less CO2 than CAISO.
"""
import pandas as pd

from src.common import config


def metrics(g: pd.DataFrame) -> dict:
    diff = g["ours_tons"] - g["caiso_tons"]
    return {"n": len(g), "MAPE": g["abs_pct_err"].mean(), "median": g["abs_pct_err"].median(),
            "WAPE": diff.abs().sum() / g["caiso_tons"].sum(), "bias": diff.sum() / g["caiso_tons"].sum(),
            "caiso_t_per_5min": g["caiso_tons"].mean()}


def table(df, key):
    return pd.DataFrame([{key: k, **metrics(g)} for k, g in df.groupby(key)]).set_index(key)


if __name__ == "__main__":
    df = pd.read_csv(config.DATA_DIR / "accuracy_results.csv")
    df["interval_utc"] = pd.to_datetime(df["interval_utc"], utc=True)
    loc = df["interval_utc"].dt.tz_convert("America/Los_Angeles")
    df["hour"], df["month"] = loc.dt.hour, loc.dt.strftime("%Y-%m")
    pd.set_option("display.width", 200)
    print("OVERALL\n", pd.Series(metrics(df)).round(3).to_string())
    print("\nSHARE OF INTERVALS WITH ERROR ABOVE:",
          {f"{int(t*100)}%": round(float((df["abs_pct_err"] > t).mean()), 3) for t in (0.1, 0.25, 0.5, 1.0)})
    print("\nBY PACIFIC HOUR\n", table(df, "hour").round(3).to_string())
    print("\nBY MONTH\n", table(df, "month").round(3).to_string())
    worst = df.nlargest(12, "abs_pct_err")[["interval_utc", "ours_tons", "caiso_tons", "abs_pct_err"]]
    print("\nWORST 12 INTERVALS\n", worst.round({"ours_tons": 1, "caiso_tons": 1, "abs_pct_err": 3}).to_string(index=False))

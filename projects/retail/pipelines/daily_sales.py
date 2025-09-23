from pathlib import Path
import pandas as pd
from lineage_client.wrapper import with_lineage, file_uri

# repo root = three levels up from this file
ROOT = Path(__file__).resolve().parents[3]
INP  = ROOT / "datasets" / "input"  / "sales.csv"
OUT  = ROOT / "datasets" / "output" / "sales_daily.parquet"

# ensure demo data exists
INP.parent.mkdir(parents=True, exist_ok=True)
OUT.parent.mkdir(parents=True, exist_ok=True)
if not INP.exists():
    INP.write_text("date,store,amount\n2025-09-20,A,100\n2025-09-20,B,150\n2025-09-21,A,90\n")

@with_lineage(
    job_name="daily_sales",
    inputs=[file_uri(INP)],
    outputs=[file_uri(OUT)],
)
def main():
    df = pd.read_csv(INP, parse_dates=["date"])
    agg = (
        df.groupby("date", as_index=False)["amount"]
          .sum()
          .rename(columns={"amount": "total_amount"})
    )
    agg.to_parquet(OUT, index=False)

if __name__ == "__main__":
    main()
import pandas as pd
import glob

files = glob.glob("data/*.parquet")

df = pd.concat([pd.read_parquet(f) for f in files[:2]])  # test small

print(df.head())
print(df.shape)
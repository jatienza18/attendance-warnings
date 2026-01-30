import pandas as pd

try:
    df = pd.read_excel("faltes_grup_Tots copia.xlsx")
    print("Columns found:", df.columns.tolist())
    print("First 5 rows:")
    print(df.head())
except Exception as e:
    print(e)

from pytde import to_tde
import pandas as pd


df = pd.read_csv("dataset.csv")


to_tde(df)
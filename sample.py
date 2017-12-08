import pytde
import pandas as pd

df = pd.read_csv("dataset.csv")
pytde.to_tde(df,'extract.tde')

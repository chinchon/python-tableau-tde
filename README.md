# python-tableau-tde

Build Tableau Data Extract (.tde) from Pandas DataFrame using Tableau SDK

### requirements
1. Python 2.7 (Python 3 not supported)
2. Tableau SDK
    * documentation: https://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm
    * download: https://downloads.tableau.com/tssoftware/Tableau-SDK-Python-Win-64Bit-10-3-5.zip

### sample.py
```python
import pytde
import pandas as pd

df = pd.read_csv("dataset.csv")
pytde.to_tde(df,'extract.tde')
```


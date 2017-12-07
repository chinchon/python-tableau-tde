import sys
from tableausdk import *
from tableausdk.Extract import *
import numpy as np

# available tableau datatypes
# INTEGER, DOUBLE, BOOLEAN, DATE, DATETIME, DURATION, 
# CHAR_STRING, UNICODE_STRING, SPATIAL
DTYPE_MAP = {
    np.dtype(np.int64): Type.INTEGER,
    np.dtype(np.float64): Type.DOUBLE,
    np.dtype('O'): Type.UNICODE_STRING,
}

def tableau_datatype(dtype):
    return DTYPE_MAP[dtype]

def make_table_definition(df):
    table_definition = TableDefinition()
    for column in df.columns:
        table_definition.addColumn(column,tableau_datatype(df[column].dtype))
    return table_definition

def to_tde(df):
    ExtractAPI.initialize()
    new_extract = Extract('extract.tde')
    
    table_definition = make_table_definition(df)
    new_table = new_extract.addTable('Extract', table_definition)
    new_row = Row(table_definition)
    
    for row in df.iterrows():
        for i,column in enumerate(df.columns):
            if df[column].dtype == np.dtype(np.int64):
                new_row.setInteger(i, int(row[1][i]))
            elif df[column].dtype == np.dtype('O'):
                new_row.setString(i, str(row[1][i]))
            elif df[column].dtype == np.dtype(np.float64):
                new_row.setDouble(i, float(row[1][i]))
        new_table.insert(new_row)
        
    new_extract.close()
    ExtractAPI.cleanup()


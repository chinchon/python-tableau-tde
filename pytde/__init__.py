from tableausdk.Extract import Row,TableDefinition,ExtractAPI,Extract
from tableausdk.Types import Type
import numpy as np
import pandas as pd


# Tableau datatypes: INTEGER, DOUBLE, BOOLEAN, DATE, DATETIME, 
# DURATION, CHAR_STRING, UNICODE_STRING, SPATIAL
mapper = {
    np.dtype(np.int64): {
        'tableau_datatype': Type.INTEGER,
        'tableau_set_function':Row.setInteger,
        'value_modifier': lambda x: [x] if not np.isnan(x) else None,
    },
    np.dtype(np.float64): {
        'tableau_datatype': Type.DOUBLE,
        'tableau_set_function':Row.setDouble,
        'value_modifier': lambda x: [x] if not np.isnan(x) else None,
    },
    np.dtype('O'): {
        'tableau_datatype': Type.UNICODE_STRING,
        'tableau_set_function':Row.setString,
        'value_modifier': lambda x: [unicode(str(x), errors='replace')] if x else None,
    },
    np.dtype('<M8[ns]'): {
        'tableau_datatype': Type.DATETIME,
        'tableau_set_function':Row.setDateTime,
        'value_modifier': lambda x: [x.year,x.month,x.day,x.hour,x.minute,x.second,0] if not np.isnan(x.year) else None,
    },
}

def make_table_definition(df):
    table_definition = TableDefinition()
    for column in df.columns:
        tableau_column = column.title().replace('_',' ')
        tableau_dtype = mapper[df[column].dtype]['tableau_datatype']
        table_definition.addColumn(tableau_column,tableau_dtype)
    return table_definition

def dedup_column_name(df):
    # rename duplicate columns (https://stackoverflow.com/questions/24685012/pandas-dataframe-renaming-multiple-identically-named-columns)
    # I don't understand how it's done either
    cols=pd.Series(df.columns)
    for dup in df.columns.get_duplicates(): cols[df.columns.get_loc(dup)]=[dup+'_'+str(d_idx) if d_idx!=0 else dup for d_idx in range(df.columns.get_loc(dup).sum())]
    df.columns=cols
    return df

def to_tde(df,tde_filename = 'extract.tde'):
    ExtractAPI.initialize()
    new_extract = Extract(tde_filename)
    
    df = dedup_column_name(df)
    table_definition = make_table_definition(df)
    new_table = new_extract.addTable('Extract', table_definition)
    
    for row in df.iterrows():
        new_row = Row(table_definition)
        for i,column in enumerate(df.columns):
            column_data_type = df[column].dtype
            value = mapper[column_data_type]['value_modifier'](row[1][i])
                
            if value:
                params = [new_row,i]+value
                mapper[column_data_type]['tableau_set_function'](*params)

        new_table.insert(new_row)
        
    new_extract.close()
    ExtractAPI.cleanup()


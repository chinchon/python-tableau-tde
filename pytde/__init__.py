from __future__ import unicode_literals
from tableausdk.Extract import Row,TableDefinition,ExtractAPI,Extract
from tableausdk.Types import Type
import numpy as np
import pandas as pd
import os
import tqdm
import datetime


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
        #'value_modifier': lambda x: [unicode(x, errors='replace')] if x else None,
        'value_modifier': lambda x: [str(x)] if x else None,
    },
    np.dtype('bool'): {
        'tableau_datatype': Type.BOOLEAN,
        'tableau_set_function':Row.setBoolean,
        #'value_modifier': lambda x: [unicode(x, errors='replace')] if x else None,
        'value_modifier': lambda x: [x] if x else None,
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

# rename duplicate columns (https://stackoverflow.com/questions/24685012/pandas-dataframe-renaming-multiple-identically-named-columns)
# def dedup_column_name(df):
#     cols=pd.Series(df.columns)
#     for dup in df.columns.get_duplicates(): cols[df.columns.get_loc(dup)]=[dup+'_'+str(d_idx) if d_idx!=0 else dup for d_idx in range(df.columns.get_loc(dup).sum())]
#     df.columns=cols
#     return df

def dedup_column_name(df):
    import itertools
    cols = pd.Series(df.columns)

    def zip_numbers(l):
        al = [str(i) for i in range(1, len(df.columns)+1)]
        return ['_'.join(i) for i in zip(l, al)] if len(l) > 1 else l

    df.columns = [j for _, i in itertools.groupby(cols) for j in zip_numbers(list(i))]
    return df

def to_tde(df,tde_filename = 'extract.tde'):
    if os.path.isfile(tde_filename):
        os.remove(tde_filename)
    ExtractAPI.initialize()
    new_extract = Extract(tde_filename)

    df = dedup_column_name(df)
    row_cnt, col_cnt = df.shape
    list_dtypes = df.dtypes

    table_definition = make_table_definition(df)
    new_table = new_extract.addTable('Extract', table_definition)

    # for j, row in df.iterrows():
    for j, row in tqdm.tqdm(df.iterrows(), total=row_cnt):
        new_row = Row(table_definition)
        for i, (cell, column_data_type) in enumerate(zip(row, list_dtypes)):
            value = mapper[column_data_type]['value_modifier'](cell)
            if value:
                params = [new_row, i] + value
                mapper[column_data_type]['tableau_set_function'](*params)

#        for i,column in enumerate(df.columns):
#            column_data_type = df[column].dtype
#            value = mapper[column_data_type]['value_modifier'](row[1][i])
#
#            if value:
#                params = [new_row,i]+value
#                mapper[column_data_type]['tableau_set_function'](*params)
#
        new_table.insert(new_row)

    new_extract.close()
    ExtractAPI.cleanup()

def to_tde_new(df, tde_filepath, showProgress=True):

    # try to use apply to test how it speeds up.
    # apply is a little faster than iterrows(), about 1.5x
    # testing by my dataset which contains 100000 rows and 25 cols

    if os.path.isfile(tde_filepath):
        os.remove(tde_filepath)
    ExtractAPI.initialize()
    new_extract = Extract(tde_filepath)

    df = dedup_column_name(df)
    row_cnt, col_cnt = df.shape
    list_dtypes = df.dtypes
    table_definition = make_table_definition(df)
    new_table = new_extract.addTable('Extract', table_definition)

    def insert_tde(x):
        new_row = Row(table_definition)
        for i, (cell, column_data_type) in enumerate(zip(x, list_dtypes)):
            value = mapper[column_data_type]['value_modifier'](cell)
            if value:
                params = [new_row, i] + value
                mapper[column_data_type]['tableau_set_function'](*params)

        new_table.insert(new_row)

    tqdm.tqdm.pandas(desc='My bar!')
    df.progress_apply(insert_tde, axis=1)
    new_extract.close()
    ExtractAPI.cleanup()

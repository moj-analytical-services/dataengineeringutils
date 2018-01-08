import re

def normalise_string(colname):
    colname = colname.lower()
    colname = colname.replace("'", "")
    colname = re.sub("[^\w]", "_", colname)
    return colname

def clean_and_normalise_df_column_names(df):
    df.columns = [normalise_string(c) for c in df.columns]
    return df
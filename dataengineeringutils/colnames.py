import re

def normalise_string(colname):
    colname = colname.lower()
    colname = colname.replace("'", "")
    colname = re.sub("[^\w]", "_", colname)
    return colname

def normalise_df_columns(df):
    df.columns = [normalise_string(c) for c in df.columns]
    return df
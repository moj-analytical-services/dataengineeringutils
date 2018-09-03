import pkg_resources
import json
import pandas as pd
import numpy as np

def _get_np_datatype_from_metadata(col_name, table_metadata):

    columns_metadata = table_metadata["columns"]
    with pkg_resources.resource_stream(__name__, "data/data_type_conversion.csv") as io:
        type_conversion = pd.read_csv(io)
    type_conversion = type_conversion.set_index("metadata")
    type_conversion_dict = type_conversion.to_dict(orient="index")

    col = None
    for c in columns_metadata:
        if c["name"] == col_name:
            col = c

    if col:
        agnostic_type = col["type"]
        numpy_type = type_conversion_dict[agnostic_type]["pandas"]
        return np.typeDict[numpy_type]
    else:
        return None

def _pd_dtype_dict_from_metadata(table_metadata):
    """
    Convert the table metadata to the dtype dict
    """

    with pkg_resources.resource_stream(__name__, "data/data_type_conversion.csv") as io:
        type_conversion = pd.read_csv(io)

    type_conversion = type_conversion.set_index("metadata")

    type_conversion_dict = type_conversion.to_dict(orient="index")

    table_metadata = table_metadata["columns"]
    dtype = {}
    parse_dates = []

    for c in table_metadata:
        colname = c["name"]
        coltype = c["type"]
        coltype = type_conversion_dict[coltype]['pandas']
        dtype[colname] = np.typeDict[coltype]

    return dtype

def _pd_date_parse_list_from_metadatadata(table_metadata):
    """
    Get list of columns to pass to the pandas.to_csv date_parse argument from table metadata
    """

    table_metadata = table_metadata["columns"]

    parse_dates = []

    for c in table_metadata:
        colname = c["name"]
        coltype = c["type"]

        if coltype in ["date", "datetime"]:
            parse_dates.append(colname)

    return parse_dates


def pd_read_csv_using_metadata(filepath_or_buffer, table_metadata, *args, **kwargs):
    """
    Use pandas to read a csv imposing the datatypes specified in the table_metadata

    Passes through kwargs to pandas.read_csv
    """
    dtype = _pd_dtype_dict_from_metadata(table_metadata)
    parse_dates = _pd_date_parse_list_from_metadatadata(table_metadata)

    return pd.read_csv(filepath_or_buffer, dtype = dtype, parse_dates = parse_dates, *args, **kwargs)

def _pd_df_contains_same_columns_as_metadata(df, table_metadata):
    """
    Check a pandas dataframe contains the same columns as those specified in the table metadata
    """

    pd_columns = set(df.columns)
    md_columns = set([c["name"] for c in table_metadata["columns"]])

    return pd_columns == md_columns

def _check_pd_df_contains_same_columns_as_metadata(df, table_metadata):

    if not _pd_df_contains_same_columns_as_metadata(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")


def _pd_df_cols_matches_metadata_column_ordered(df, table_metadata):

    pd_columns = list(df.columns)
    md_columns = [c["name"] for c in table_metadata["columns"]]

    return pd_columns == md_columns

def _check_pd_df_cols_matches_metadata_column_ordered(df, table_metadata):

    if not _pd_df_contains_same_columns_as_metadata(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not _pd_df_cols_matches_metadata_column_ordered(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")


def pd_df_conforms_to_metadata_data_types(df, table_metadata):

    expected_dtypes = _pd_dtype_dict_from_metadata(table_metadata)
    date_cols = _pd_date_parse_list_from_metadatadata(table_metadata)

    for col in date_cols:
        expected_dtypes[col] = np.typeDict["datetime64"]

    actual_numpy_types = dict(df.dtypes)

    for dt in actual_numpy_types:
        actual_numpy_types[dt] = actual_numpy_types[dt].type

    return actual_numpy_types == expected_dtypes

def _check_pd_df_conforms_to_metadata_data_types(df, table_metadata):

    if not pd_df_conforms_to_metadata_data_types(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different datatypes to those expected by the metadata")


def check_pd_df_exactly_conforms_to_metadata(df, table_metadata):

    if not _pd_df_contains_same_columns_as_metadata(df, table_metadata):
            raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not _pd_df_cols_matches_metadata_column_ordered(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not pd_df_conforms_to_metadata_data_types(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different datatypes to those expected by the metadata")


def impose_metadata_column_order_on_pd_df(df, table_metadata, create_cols_if_not_exist=False, delete_superflous_colums=True):
    """
    Return a dataframe where the column order conforms to the metadata
    Note: This does not check the types match the metadata
    """
    md_cols = [c["name"] for c in table_metadata["columns"]]
    actual_cols = df.columns

    md_cols_set = set(md_cols)
    actual_cols_set = set(actual_cols)

    if len(md_cols) != len(md_cols_set):
        raise ValueError("You have a duplicated column names in your metadata")

    if len(actual_cols) != len(actual_cols_set):
        raise ValueError("You have a duplicated column names in your data")

    # Delete superflous columns if option set
    superflous_cols = actual_cols_set - md_cols_set

    if superflous_cols and not delete_superflous_colums:
        raise ValueError("You chose delete_superflous_colums = False, but superflous columns were found")
    else:
        for c in superflous_cols:
            del df[c]

    # Create columns if not in data and option is set
    missing_cols = md_cols_set - actual_cols_set
    if missing_cols and not create_cols_if_not_exist:
        raise ValueError("You create_cols_if_not_exist = False, but there are missing columns in your data")
    else:
        for c in missing_cols:
            np_type = _get_np_datatype_from_metadata(c, table_metadata)
            df[c] = pd.Series(dtype=np_type)

    return df[md_cols]


def impose_metadata_data_types_on_pd_df(df, table_metadata):
    """
    Impost correct data type on all columns in metadata.
    Doesn't modify columns not in metadata
    """

    df_cols_set = set(df.columns)

    metadata_date_cols_set = set(_pd_date_parse_list_from_metadatadata(table_metadata))
    metadata_cols_set = set([c["name"] for c in table_metadata["columns"]])

    # Cols that may need conversion without date cols
    try_convert_nodate = df_cols_set.intersection(metadata_cols_set) - metadata_date_cols_set
    try_convert_date = df_cols_set.intersection(metadata_date_cols_set)

    for col in try_convert_nodate:
        expected_type = _get_np_datatype_from_metadata(col, table_metadata)
        actual_type = df[col].dtype.type

        if expected_type != actual_type:
            df[col] = df[col].astype(expected_type)

    for col in try_convert_date:
        expected_type = np.typeDict["Datetime64"]
        actual_type = df[col].dtype.type

        if expected_type != actual_type:
            df[col] = pd.to_datetime(df[col])

    return df


def impose_exact_conformance_on_pd_df(df, table_metadata):
    df = impose_metadata_column_order_on_pd_df(df, table_metadata, delete_superflous_colums=True)
    df = impose_metadata_data_types_on_pd_df(df, table_metadata)
    return df




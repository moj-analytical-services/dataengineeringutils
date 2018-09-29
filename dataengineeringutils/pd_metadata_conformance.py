import pkg_resources
import json
import pandas as pd
import numpy as np

def _remove_paritions_from_table_metadata(table_metadata):

    if "partitions" in table_metadata:
        table_metadata["columns"] = [c for c in table_metadata["columns"] if c["name"] not in table_metadata["partitions"]]
        table_metadata["partitions"] = []

    return table_metadata


def _get_np_datatype_from_metadata(col_name, table_metadata):
    """
    Lookup the datatype from the metadata, and our conversion table
    """

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

def _pd_dtype_dict_from_metadata(table_metadata, ignore_partitions=False):
    """
    Convert the table metadata to the dtype dict that needs to be
    passed to the dtype argument of pd.read_csv
    """

    with pkg_resources.resource_stream(__name__, "data/data_type_conversion.csv") as io:
        type_conversion = pd.read_csv(io)

    type_conversion = type_conversion.set_index("metadata")

    type_conversion_dict = type_conversion.to_dict(orient="index")

    table_metadata_columns = table_metadata["columns"]

    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

    dtype = {}
    parse_dates = []

    for c in table_metadata_columns:
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

def pd_read_csv_using_metadata(filepath_or_buffer, table_metadata, ignore_partitions=False, *args, **kwargs):
    """
    Use pandas to read a csv imposing the datatypes specified in the table_metadata

    Passes through kwargs to pandas.read_csv

    If ignore_partitions=True, assume that partitions are not columns in the dataset
    """
    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

    dtype = _pd_dtype_dict_from_metadata(table_metadata, ignore_partitions)
    parse_dates = _pd_date_parse_list_from_metadatadata(table_metadata)

    return pd.read_csv(filepath_or_buffer, dtype = dtype, parse_dates = parse_dates, *args, **kwargs)

def _pd_df_cols_match_metadata_cols(df, table_metadata):
    """
    Is the set of columns in the metadata equal to the set of columns in the dataframe?

    This check is irrespective of column order, does not check for duplicates.
    """

    pd_columns = set(df.columns)
    md_columns = set([c["name"] for c in table_metadata["columns"]])

    return pd_columns == md_columns

def _check_pd_df_cols_match_metadata_cols(df, table_metadata):

    if not _pd_df_cols_match_metadata_cols(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")


def _pd_df_cols_match_metadata_cols_ordered(df, table_metadata):
    """
    Are the columns in the metadata exactly the same as those in the dataframe
    i.e. same columns in the same order
    """

    pd_columns = list(df.columns)
    md_columns = [c["name"] for c in table_metadata["columns"]]

    return pd_columns == md_columns

def _check_pd_df_cols_match_metadata_cols_ordered(df, table_metadata):

    if not _pd_df_cols_match_metadata_cols(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not _pd_df_cols_match_metadata_cols_ordered(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")


def pd_df_datatypes_match_metadata_data_types(df, table_metadata, ignore_partitions=False):
    """
    Do the data types in the pandas dataframe match those in table_metadata
    """

    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

    expected_dtypes = _pd_dtype_dict_from_metadata(table_metadata)
    date_cols = _pd_date_parse_list_from_metadatadata(table_metadata)

    for col in date_cols:
        expected_dtypes[col] = np.typeDict["datetime64"]

    actual_numpy_types = dict(df.dtypes)

    for dt in actual_numpy_types:
        actual_numpy_types[dt] = actual_numpy_types[dt].type

    return actual_numpy_types == expected_dtypes

def _check_pd_df_datatypes_match_metadata_data_types(df, table_metadata):

    if not pd_df_datatypes_match_metadata_data_types(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different datatypes to those expected by the metadata")


def check_pd_df_exactly_conforms_to_metadata(df, table_metadata, ignore_partitions=False):

    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

    if not _pd_df_cols_match_metadata_cols(df, table_metadata):
            raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not _pd_df_cols_match_metadata_cols_ordered(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different columns to your metadata")

    if not pd_df_datatypes_match_metadata_data_types(df, table_metadata):
        raise ValueError("Your pandas dataframe contains different datatypes to those expected by the metadata")


def impose_metadata_column_order_on_pd_df(df, table_metadata, create_cols_if_not_exist=False, delete_superflous_colums=True, ignore_partitions=False):
    """
    Return a dataframe where the column order conforms to the metadata
    Note: This does not check the types match the metadata
    """

    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

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

    if len(superflous_cols) > 0 and not delete_superflous_colums:
        raise ValueError(f"You chose delete_superflous_colums = False, but the following superflous columns were found: {superflous_cols}")
    else:
        for c in superflous_cols:
            del df[c]

    # Create columns if not in data and option is set
    missing_cols = md_cols_set - actual_cols_set

    if len(missing_cols) > 0 and not create_cols_if_not_exist:
        raise ValueError(f"You create_cols_if_not_exist = False, but the following columns are missing from your data {missing_cols}")
    else:
        for c in missing_cols:
            np_type = _get_np_datatype_from_metadata(c, table_metadata)
            df[c] = pd.Series(dtype=np_type)

    return df[md_cols]


def impose_metadata_data_types_on_pd_df(df, table_metadata, errors='raise', ignore_partitions=False):
    """
    Impost correct data type on all columns in metadata.
    Doesn't modify columns not in metadata

    Allows you to pass arguments through to the astype e.g. to errors = 'ignore'
    https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.astype.html
    """

    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

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
            df[col] = df[col].astype(expected_type, errors=errors)

        if expected_type == np.typeDict["object"]:
            df[col] = df[col].to_string()

    for col in try_convert_date:
        expected_type = np.typeDict["Datetime64"]
        actual_type = df[col].dtype.type

        if expected_type != actual_type:
            df[col] = pd.to_datetime(df[col], errors=errors)

    return df


def impose_exact_conformance_on_pd_df(df, table_metadata, ignore_partitions=False):

    if ignore_partitions:
        table_metadata = _remove_paritions_from_table_metadata(table_metadata)

    df = impose_metadata_column_order_on_pd_df(df, table_metadata, delete_superflous_colums=True)
    df = impose_metadata_data_types_on_pd_df(df, table_metadata)
    return df




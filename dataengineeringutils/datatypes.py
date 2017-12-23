import pandas as pd
import os
import pkg_resources

def translate_metadata_type_to_glue_type(column_type):
    #
    io = pkg_resources.resource_stream(__name__, "data/data_type_conversion.csv")
    df = pd.read_csv(io)
    df = df.set_index("metadata")
    lookup = df.to_dict(orient="index")
    return lookup[column_type]["glue"]
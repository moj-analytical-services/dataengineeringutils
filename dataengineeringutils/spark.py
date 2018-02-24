import json
import pandas as pd
import os
import pyspark.sql.types

from dataengineeringutils.utils import read_json

from dataengineeringutils.datatypes import translate_metadata_type_to_type


def get_customschema_from_metadata(metadata):

    columns = metadata["columns"]

    custom_schema = pyspark.sql.types.StructType()

    for c in columns:
        this_name = c["name"]
        this_type = translate_metadata_type_to_type(c["type"], "spark")
        this_type = getattr(pyspark.sql.types, this_type)
        this_field = pyspark.sql.types.StructField(this_name, this_type())
        custom_schema.add(this_field)
    return custom_schema
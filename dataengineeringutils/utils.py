import collections
import json

def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]

# Read json file
def read_json(filename) :
    with open(filename) as json_data:
        data = json.load(json_data)
    return data

# Write json file
def write_json(data, filename) :
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=4, separators=(',', ': '))

# Read first line of csv and return a list
def get_csv_header(file_path, convert_to_lower = False) :
    with open(file_path) as f:
        line = f.readline()
        column_names = line.rstrip().split(",")
        if convert_to_lower :
            column_names = [c.lower() for c in column_names]
    return column_names

# Combine a list of sql queries into a single query
def combine_sql_select_statements(list_of_queries) :
    return ', '.join(list_of_queries)

# Create a list of column names into a single sql statement with specified cols excluded and with table name alias
def col_names_to_sql_select(col_list, table_alias = None, exclude = None) :
    if table_alias is None :
        table_alias = ''
    else :
        table_alias = table_alias + '.'
    
    if exclude is not None :
        col_list = [c for c in col_list if c not in exclude]
    
    return ", ".join([table_alias + c for c in col_list])


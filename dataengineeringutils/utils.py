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
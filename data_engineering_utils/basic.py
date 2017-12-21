import json

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
            column_names = [c.lower() for c in columns]
    return column_names
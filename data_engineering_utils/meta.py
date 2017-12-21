import pandas as pd
import json
import os 

# Node constructor
def Node(identifier, metadata) :
    node = {'id' : identifier,
            'metadata' : metadata
            }
    return node

# Link constructor
def Link(source, target, directed, metadata) :
    link = {"source": source,
            "target": target,
            "directed": directed,
            "metadata": metadata
            }
    return link

# Return columns in meta data format needed for meta.json. Column types are read from csv lookup table
def columns_from_lookup(list_of_column_names, look_up_table, primary_keys = None, foreign_keys = None) :
    if primary_keys is None :
        primary_keys = []
    if foreign_keys is None :
        foreign_keys = {}

    meta_data_lookup = pd.read_csv(look_up_table)[['variable_name', 'type', 'description']]
    meta_data_lookup = meta_data_lookup[meta_data_lookup['variable_name'].isin(list_of_column_names)]
    meta_data_lookup.set_index('variable_name', inplace = True)
    
    columns = []
    for col in list_of_column_names :
        columns.append({'name' : col,
                        'type' : meta_data_lookup.loc[col, 'type'],
                        'description' : meta_data_lookup.loc[col, 'description'],
                        'primary_keys' : col in primary_keys,
                        'foreign_keys' : foreign_keys[col] if col in foreign_keys.keys() else []})
    return columns


# Create meta data object
def table_metadata(table_name, database, columns, table_desc = None) :
    
    if table_desc is None :
        table_desc = ""
    
    table_meta = {"table_name" : table_name,
                  "table_description": table_desc,
                  "database" : database, 
                  "columns": columns
    }
    
    return table_meta

# Create a meta data file from a variable lookup and data file
def create_meta_from_lookup(data_file_path, look_up_table, table_description = None, primary_keys = None, foreign_keys = None) :
    col_names = get_csv_header(data_file_path)
    cols = columns_from_lookup(col_names, look_up_table, primary_keys, foreign_keys)
    table_name = data_file_path.split('/')[-1].split('.')[0]
    db_name = data_file_path.split('/')[-3]
    if table_description is None :
        table_description = ""
        
    return table_metadata(table_name, db_name, cols, table_description)

# get node from meta.json file
def get_node_from_meta_file(file_path) :
    
    metadata = read_json(file_path)
    node_id = metadata['database']+'.'+metadata['table_name'] 
    node = Node(node_id, metadata)
    return node

# get folder path
def define_folder_path(table_name, parent_folder_path) :
    return parent_folder_path + '/' + table_name if parent_folder_path is not None else table_name

# Read json file
def read_json(filename) :
    with open(filename) as json_data:
        data = json.load(json_data)
    return data

# Write json file
def write_json(data, filename) :
    with open(filename, 'w') as outfile:
        json.dump(data, outfile, indent=4, separators=(',', ': '))

# maps all column names from a list of nodes into a single list
def get_all_column_names_from_node_list(nodes) :
    return [n['id'] + '.' + c['name'] for n in nodes for c in n['metadata']['columns']]
    
# Create links from a list of nodes (looks through foriegn keys of each nodes and maps via those)
def create_links_from_nodes(nodes) :
    all_columns = get_all_column_names_from_node_list(nodes)
    
    links = []
    
    for n in nodes :
        for c in n['metadata']['columns'] :
            for fk in c['foreign_keys'] :
                if fk in all_columns :
                    links.append(Link(source = n['id'] + '.' + c['name'], target = fk, directed = True, metadata = ""))
    
    return links

#### DEPRECIATED ####

# No longer used in example but kept incase useful later
def create_links_from_csv(filepath) :
    links_table = pd.read_csv(filepath).to_dict('records')
    final_links = []
    for l in links_table :
        final_links.append({'source' : l['source_database'] + '.' + l['source_table'] + '.' + l['source_column'],
                            'target' : l['source_database'] + '.' + l['source_table'] + '.' + l['source_column']})
    return final_links

# No longer used in example but kept incase useful later
def create_database_meta(database_folder_path, links = None) :
    tables = [x for x in next(os.walk(database_folder_path))[1] if '.' not in x]
    database_name = database_folder_path.split('/')[-1]
    
    if links is None :
        link_csv = database_folder_path + "/" + "links.csv"
        links = create_links_from_csv(link_csv)
        
    meta = {'name' : database_name,
            'tables': tables,
            'links' : links
           }
    return meta

# No longer used in example but kept incase useful later
def create_database_graph(database_folder_path) :
    db_name = database_folder_path.split("/")[-1]
    db_meta = read_json(database_folder_path + "/" + db_name + '.meta.json')
    graph = {
        "directed": True,
        "metadata": db_meta['tables'],
        "nodes": [],
        "links": db_meta['links']
    }
    
    for t in db_meta['tables'] :
        table_meta_path = '/'.join([database_folder_path, t, t+'.meta.json'])
        graph['nodes'].append(Node('.'.join([db_name, t]), read_json(table_meta_path)))
    return graph
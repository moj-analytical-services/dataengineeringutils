import pandas as pd
import numpy as np
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

# Constructor for column meta data
def Column_meta(name, data_type = None, description = None) :
    if data_type is None :
        data_type = 'string'

    if description is None :
        description = 'Not yet added'

    col =  {
        'name' : name,
        'data_type' : data_type,
        'description' : description
    }
    return col

# Constructor for table meta
def Table_metadata(table_name, bucket, columns, table_description = None) :
    
    if table_desc is None :
        table_desc = ""
    
    table_meta = {"table_name" : table_name,
                  "table_description": table_desc,
                  "bucket" : bucket, 
                  "columns": columns
    }
    return table_meta

# Get a list of accepted base datatypes for glue table definitions
def get_base_data_types() :
    return base_types ['boolean', 'bigint', 'double', 'string', 'timestamp', 'date']

def create_columns_meta(column_names, data_type_overrides = None, description_overrides = None) :
    if data_type_overrides is None :
        data_type_overrides = []
    
    if description_overrides is None :
        description_overrides = []
    
    base_types = get_base_data_types()
    
    data_type_keys = list(data_type_overrides)
    data_desc_keys = list(description_overrides)
    # Error checking
    for dtk in data_type_keys :
        if data_type_overrides[dtk] not in base_types :
            raise ValueError("Column name: \"{}\" in input data_type_overrides: has an invalid datatype. Valid datatypes are {}".format(dtk,", ".join(base_types)))
    
    # Create meta data
    col_meta = []
    for c in column_names :
        Column_meta(name = c,
            data_type = None if c not in data_type_keys else data_type_keys[c],
            description = None if c not in data_type_keys else data_desc_keys[c])
    return(col_meta)


# Super basic atm assumes everything that isn't an int or double as a string
def get_col_types_from_df(df) :
    col_names = list(df)
    table_col_meta = []
    for col in col_names :
        if df[col].dtype == np.int64 :
            table_col_meta.append(meta_utils.Column_meta(name = col, data_type = 'bigint', description = ''))
        elif df[col].dtype == np.float :
            table_col_meta.append(meta_utils.Column_meta(name = col, data_type = 'double', description = ''))
        else :
            table_col_meta.append(meta_utils.Column_meta(name = col, data_type = 'string', description = ''))
    
    return(table_col_meta)

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

#### Commenting out everything until it is rewriten to work with rest of package
# Create a meta data file from a variable lookup and data file
# def create_meta_from_lookup(data_file_path, look_up_table, table_description = None, primary_keys = None, foreign_keys = None) :
#     col_names = get_csv_header(data_file_path)
#     cols = columns_from_lookup(col_names, look_up_table, primary_keys, foreign_keys)
#     table_name = data_file_path.split('/')[-1].split('.')[0]
#     db_name = data_file_path.split('/')[-3]
#     if table_description is None :
#         table_description = ""
        
#     return table_metadata(table_name, db_name, cols, table_description)

# # get node from meta.json file
# def get_node_from_meta_file(file_path) :
    
#     metadata = read_json(file_path)
#     node_id = metadata['database']+'.'+metadata['table_name'] 
#     node = Node(node_id, metadata)
#     return node

# # get folder path
# def define_folder_path(table_name, parent_folder_path) :
#     return parent_folder_path + '/' + table_name if parent_folder_path is not None else table_name

# # maps all column names from a list of nodes into a single list
# def get_all_column_names_from_node_list(nodes) :
#     return [n['id'] + '.' + c['name'] for n in nodes for c in n['metadata']['columns']]
    
# # Create links from a list of nodes (looks through foriegn keys of each nodes and maps via those)
# def create_links_from_nodes(nodes) :
#     all_columns = get_all_column_names_from_node_list(nodes)
    
#     links = []
    
#     for n in nodes :
#         for c in n['metadata']['columns'] :
#             for fk in c['foreign_keys'] :
#                 if fk in all_columns :
#                     links.append(Link(source = n['id'] + '.' + c['name'], target = fk, directed = True, metadata = ""))
    
#     return links

# #### DEPRECIATED ####

# # No longer used in example but kept incase useful later
# def create_links_from_csv(filepath) :
#     links_table = pd.read_csv(filepath).to_dict('records')
#     final_links = []
#     for l in links_table :
#         final_links.append({'source' : l['source_database'] + '.' + l['source_table'] + '.' + l['source_column'],
#                             'target' : l['source_database'] + '.' + l['source_table'] + '.' + l['source_column']})
#     return final_links

# # No longer used in example but kept incase useful later
# def create_database_meta(database_folder_path, links = None) :
#     tables = [x for x in next(os.walk(database_folder_path))[1] if '.' not in x]
#     database_name = database_folder_path.split('/')[-1]
    
#     if links is None :
#         link_csv = database_folder_path + "/" + "links.csv"
#         links = create_links_from_csv(link_csv)
        
#     meta = {'name' : database_name,
#             'tables': tables,
#             'links' : links
#            }
#     return meta

# # No longer used in example but kept incase useful later
# def create_database_graph(database_folder_path) :
#     db_name = database_folder_path.split("/")[-1]
#     db_meta = basic_utils.read_json(database_folder_path + "/" + db_name + '.meta.json')
#     graph = {
#         "directed": True,
#         "metadata": db_meta['tables'],
#         "nodes": [],
#         "links": db_meta['links']
#     }
    
#     for t in db_meta['tables'] :
#         table_meta_path = '/'.join([database_folder_path, t, t+'.meta.json'])
#         graph['nodes'].append(Node('.'.join([db_name, t]), basic_utils.read_json(table_meta_path)))
#     return graph
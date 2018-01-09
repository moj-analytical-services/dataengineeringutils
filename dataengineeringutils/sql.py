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


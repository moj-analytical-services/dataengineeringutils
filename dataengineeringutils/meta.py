from dataengineeringutils.utils import read_json, write_json
from copy import copy
class Meta :

    supported_column_types = ('int', 'character', 'float', 'date', 'datetime', 'boolean', 'long')
    supported_data_formats = ('avro', 'csv', 'csv_quoted_nodate', 'regex', 'orc', 'par', 'parquet')

    def __init__(self, filepath) :
        self.meta = read_json(filepath)
        self.__update_column_names()

    def get_table_name(self) :
        return self.meta["table_name"]

    def change_table_name(self, new_name) :
        self.meta["table_name"] = new_name
        
    def get_table_desc(self) :
        return self.meta["table_desc"]
        
    def change_table_desc(self, new_table_desc) :
        self.meta["table_desc"] = new_table_desc
        
    def get_data_format(self) :
        return self.meta["data_format"]
        
    def change_data_format(self, new_data_format) :
        if new_data_format in self.supported_data_formats :
            self.meta["data_format"] = new_data_format
        else :
            ValueError("new_data_format ({}) is invalid. Please use one of the following {}".format(new_data_format, ', '.join(self.supported_data_formats)))
    
    def get_location(self) :
        return self.meta["table_name"]
        
    def change_location(self, new_location) :
        self.meta["location"] = new_location if new_location[-1] == '/' else new_location + '/'

    def get_id(self) :
        return self.meta["table_name"]
        
    def change_id(self, new_id) :
        self.meta['id'] = new_id

    def update_column(self, column_name, column_type = None, column_desc = None) :
        
        self.__check_column_type(column_type)
        self.__check_column_desc(column_desc)

        (b, i) = self.__is_column(column_name)
        # Update existing column
        if b :
            if column_type is not None :
                self.meta['columns'][i]['column_type'] = column_type
            if column_desc is not None :
                self.meta['columns'][i]['column_desc'] = column_desc
        
        # Add new column
        else :
            if column_type is None :
                column_type = 'character'
            if column_desc is None :
                column_desc = 'column description not yet set'

            self.meta['columns'].append({
                'name' : column_name,
                'type' : column_type,
                'description' : column_desc
            })
            self.__update_column_names()

    def remove_column(self, column_name) :
        (b, i) = self.__is_column(column_name)
        if not b :
            ValueError('column_name does not exist in meta data')
        self.meta['columns'] = [x for x in self.meta['columns'] if x['name'] != column_name]
        self.__update_column_names()
    
    def rename_column(self, old_column_name, new_column_name) :
        (old_b, old_i) = self.__is_column(old_column_name)
        if not old_b :
            ValueError("{} does not exist in meta".format(old_column_name))
        
        (new_b, new_i) = self.__is_column(old_column_name)
        if new_b :
            ValueError("{} already exists in meta".format(new_column_name))
        
        self.meta['columns'][old_i]['name'] = new_column_name
        self.__update_column_names()

    def set_columns_as_file_partitions(self, list_of_cols = None) :
        if list_of_cols is None :
            del self.meta["glue_specific"]
    
        else :
            self.meta["glue_specific"] = {"PartitionKeys" : []}
            for c in list_of_cols :
                self.__is_column(c)
                self.meta["glue_specific"]["PartitionKeys"].append({"Name" : c, "Type" : self.get_column(c)['type']})

    def write_to_json(self, filepath) :
        write_json(self.meta, filepath)

    def get_column(self, column_name, properties = ['name', 'type', 'description']) :
        (b, i) = self.__is_column(column_name)
        if b :
           return copy(self.meta['columns'][i])
        else :
            ValueError("{} is not in meta".format(column_name))

    def __is_column(self, column_name) :
        if column_name in self.column_names :
            b = True
            i = self.column_names.index(column_name)
        else :
            b = False
            i = -1
        return (b, i)

    def __check_column_type(self, column_type) :
        if column_type is not None :
            if column_type not in self.supported_column_types :
                ValueError("column_type: {} is not supported please use {}".format(column_type, ",".join(self.supported_column_types)))
    
    def __check_column_desc(self, column_desc) :
        if column_desc is not None :
            if type(column_desc) is not str :
                ValueError("column_desc must be type str")
    
    def __update_column_names(self) :
        self.column_names = [x['name'] for x in self.meta['columns']]
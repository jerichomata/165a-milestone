from lstore.table import Table, Record
from lstore.index import Index



class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        rid = self.table.record_does_exist(primary_key)
        if rid == None:
            return False
        
        # Once found, update delete value to true in page directory
        self.table.page_directory[rid]["deleted"] = True
            # rid is just a list 
            # use drop record in index 
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # schema_encoding = '0' * self.table.num_columns
        unique_identifier = columns[0]
        columns_list = list(columns)
        if len(columns_list) != self.table.num_columns:
            return False
        if not self._check_values_are_valid(columns_list):
            return False
        # if self.table.record_does_exist(key=unique_identifier) != None:
        #     return False

        # New record passed the checks, set schema encoding to 0, create a new record, and write to the table
        blank_schema_encoding = 0
        new_rid = self.table.new_base_rid()
        self.table.index_on_primary_key[unique_identifier] = new_rid
        new_record = Record(key=unique_identifier, rid=new_rid, schema_encoding=blank_schema_encoding, column_values=columns_list)
        did_successfully_write = self.table.write_new_record(record=new_record, rid=new_rid)

        if did_successfully_write:
            return True
        else:
            return False

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_key, column, query_columns):
        if column > self.table.num_columns or column < 0:
            # column argument out of range
            return False
        if len(query_columns) != self.table.num_columns:
            # length of query columns must equal the number of columns in the table
            return False
        for value in query_columns:
            # incoming query column values must be 0 or 1
            if value != 0 and value != 1:
                return False

        # Make sure that the record selected by the user exists in our database
        valid_rid = self.table.record_does_exist(index_key=index_key)
            # use locate instead in index
            # index stored in table 
            # table.index.locate 
        if valid_rid == None:
            return False

        # If exists, read the most update record by looking at the scheme encoding
        # Values with 0 will come from the base pages, values with a 1 will come from the most recent update
        selected_record = self.table.read_record(rid=valid_rid)
        if selected_record == False:
            return False

        record_return_list = [] # List of records to be returned
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                # keep value
                continue
            else:
                # replace value with None
                selected_record.user_data[i] = None

        record_return_list.append(selected_record)
        return record_return_list
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rid = self.table.index.locate(primary_key)
        if(rid == None):
            return False
        else:
            new_record = Record(rid, primary_key, columns)
            self.table.update_record(new_record, rid)
            return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        keys = sorted(self.table.keys)
        start_index = 0
        end_index = 0
        count = 0
        for key in keys:
            if key == start_range:
                start_index = count
            elif key == end_range:
                end_index = count
            count += 1
        selected_keys = keys[start_index : end_index + 1]
        result = 0
        for key in selected_keys:
            encoder = [0] * self.table.num_columns
            encoder[aggregate_column_index] = 1
            result += (self.select(key, encoder))[0].columns[aggregate_column_index]
        return result

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

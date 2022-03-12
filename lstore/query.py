from re import S
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
        self.keys = []
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        if(primary_key in self.keys):
            self.table.delete_record(primary_key)
            self.keys.remove(primary_key)
            return True
        return False

    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        columns =  list(columns)
        new_record = Record(self.table.get_new_rid(), columns[self.table.primary_key_column], columns)
        
        return self.table.add_record(new_record)

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        records_objects = []
        locations = self.table.index.locate(index_value, index_column)
        for location in locations:
            columns = self.table.get_record(location, query_columns)
            if columns == False:
                return False
            new_record = Record(location, columns[self.table.primary_key_column], columns)
            records_objects.append(new_record)
        return records_objects
        
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        base_rid = self.table.index.locate(primary_key)
        if base_rid is None:
            return False
        else:
            base_rid = base_rid[0]
        
        rid = self.table.get_new_rid()
        new_record = Record(rid, primary_key, list(columns))
        

        return self.table.update_record(new_record, base_rid)
            

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum = 0
        rids = self.table.index.locate_range(start_range, end_range)
        if(rids):
            for rid in rids:
                sum += self.table.get_newest_value(rid, aggregate_column_index+4)
            return sum
        return False

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
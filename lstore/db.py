from lstore.bpool import bufferpool
from asyncio.windows_events import NULL
from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self):
        #db needs a bufferpool now
        self.bpool = bufferpool()
        pass

    def close(self):
        #all changes non-commited changes must be written to disk before closure
        self.bpool.make_clean()
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index, self.bpool)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                print("dropped table " + table.name)
                return
        print("couldn't find " + table.name)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        return NULL

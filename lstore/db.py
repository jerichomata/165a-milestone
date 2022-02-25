from importlib.metadata import metadata
from lstore.bpool import bufferpool
from asyncio.windows_events import NULL
from lstore.table import Table
import os
import pickle

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path): 
        #db needs a bufferpool now
        self.bpool = bufferpool()
        for filename in os.listdir(path):
            for files in os.listdir(path.join(path+filename)):
                if files.equals("metadata"):
                    with open(path.join(path+filename+files), 'rb') as file:
                        self.tables.append(pickle.load(file))
        pass

    def close(self):
        #all changes non-commited changes must be written to disk before closure
        self.bpool.make_clean()
        for table in self.tables:
            cwd = os.getcwd()
            with open(cwd + "\lstore\ECS165\\" + self.table.name + "\\metadata", 'wb') as file:
                pickle.dump(table, file)
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

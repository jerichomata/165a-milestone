from lstore.bpool import bufferpool
from lstore.table import Table
import os
import pickle

class Database():

    def __init__(self):
        self.tables = []
        pass

    # Not required for milestone1
    def open(self, path = "./ECS165"): 
        #db needs a bufferpool now
        self.bpool = bufferpool()
        #if database is new and there are previous files 
        files = []
        try:
            os.mkdir(path)
        except:
            pass

        for filename in os.listdir(path):
            files.append(filename)

        if len(self.tables) == 0 and len(files) != 0:
            for filename in files:
                newpath = os.path.join(path, filename)
                for file in os.listdir(newpath):
                    if file == "metadata":
                        print("good")
                        final = newpath+"\\metadata"
                        with open(final, 'rb') as target:
                            self.tables.append(pickle.load(target))
                            print("done")
        pass

    def close(self):
        #all changes non-commited changes must be written to disk before closure
        self.bpool.make_clean()
        for table in self.tables:
            #print(type(table))
             cwd = os.getcwd()
             with open(cwd + "\ECS165\\" + table.name + "\\metadata", 'wb') as file:
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
        self.tables.append(table)
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
        return None

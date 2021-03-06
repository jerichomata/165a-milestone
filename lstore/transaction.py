from lstore.table import Table, Record
from lstore.index import Index
from lstore.logger import Logger
import threading
import os

#check_lock -> go to hashtable, 
#make_shared
#make_lock
#make_unlock

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.transaction_ids = []
        self.transaction_type = []
        self.tables = []
        self.threading_lock = threading.Lock()
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.tables.append(table)
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        i = 0
        for query, args in self.queries:
            #Acquire the lock to check if the record is ok to continue
            self.threading_lock.acquire()
            #passes in base_rid & query type to check if it can run the operation
            can_run = self.tables[i].check_lock(args[0], query.__name__)
            if(can_run):
                self.threading_lock.release()
                if(can_run == False):
                    self.abort()
                else:
                    #gets transaction_id from log & appends to transactions list
                    #query will return bool False if fail, transaction_id if successful
                    result = query(args)

                    self.transaction_type.append(query.__name__)
                    self.transaction_ids.append(result)
            else:
                self.threading_lock.release()
                return self.abort()
            # If the query has failed the transaction should abort
            if type(result) == bool:
                return self.abort()
            else:
                #increment table list. 
                i = i+1
                pass
        return self.commit()

    def abort(self):
        i = 0
        for table in self.tables:
            table.undo(self.transaction_ids[i])
        return False

    def commit(self):

        cwd = os.getcwd()
        i = 0
        for j in self.tables:
            path = cwd + +"\\" + j + "\log\\" + self.transaction_ids[i]
            os.remove(path)
            i = i+1
        return True
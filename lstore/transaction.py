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
    def add_query(self, table, query, *args):
        self.tables.append(table)
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            #Acquire the lock to check if the record is ok to continue
            self.threading_lock.acquire()
            can_execute = self.lock_switch(args[0].check_lock(query.__name__))
            if(can_execute):
                self.threading_lock.release()
                if(can_execute == False):
                    self.abort()
                else:
                    #gets transaction_id from log & appends to transactions list
                    result = query(args)
                    self.transaction_type.append(query.__name__)
                    self.transaction_ids.append(result)
            # If the query has failed the transaction should abort
            if type(result) == bool:
                return self.abort()
            else:
                pass
        return self.commit()

    def abort(self):
        i = 0
        for table in self.tables:
            table.undo(self.transaction_type, self.transaction_ids[i])
        return False

    def commit(self):

        cwd = os.getcwd()
        i = 0
        for j in self.tables:
            path = cwd + +"\\" + j + "\log\\" + self.transaction_ids[i]
            os.remove(path)
            i = i+1
        return True

    def lock_switch(self, check_lock, query_type):
        match check_lock:
            case "shared":
                if(query_type.equals("read")):
                    return True
                return False
            case "lock":
                return False
            case "unlock":
                return True
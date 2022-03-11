from lstore.table import Table, Record
from lstore.index import Index
from lstore.logger import Logger
import threading
import os
import pickle

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
        for i, query in enumerate(self.queries):
            #Acquire the lock to check if the record is ok to continue
            self.threading_lock.acquire()
            #passes in base_rid & query type to check if it can run the operation
            can_run = self.tables[i].lock_manager.check_lock(query[1][0], query[0].__name__)
            if(can_run):
                if query[0].__name__ == "update" or query[0].__name__ == "insert":
                    self.tables[i].lock_manager.set_lock(query[1][0])
                elif query[0].__name__ == "select":
                    self.tables[i].lock_manager.set_shared(query[1][0])
                self.threading_lock.release()
                #gets transaction_id from log & appends to transactions list
                #query will return bool False if fail, transaction_id if successful
                result = query[0](*query[1])

                self.transaction_type.append(query[0].__name__)
                self.transaction_ids.append(result)
            else:
                self.threading_lock.release()
                return self.abort()
            # If the query has failed the transaction should abort
            if type(result) == bool:
                return self.abort()
        return self.commit()

    def undo(self, transaction_id, table):
        with open("./log/" + table.name + "/"  + str(transaction_id), 'rb') as file:
            query = pickle.load(file)
        if query['operation'] == "update":
            table.undo_update(query['rid'], query['old_rid'], query['old_indirection'], query['old_schema'])
        if query['operation'] == "insert":
            table.undo_insert(query['rid'])

    def abort(self):
        print("abort", self.transaction_ids)
        for i in range(len(self.transaction_ids)):
            self.undo(self.transaction_ids[i], self.tables[i])
        return False

    def commit(self):
        print("commit", self.transaction_ids)
        cwd = os.getcwd()
        for i, query_id in enumerate(self.transaction_ids):
            path = cwd  + "\log\\"  + self.tables[i].name +"\\" + str(query_id)
            os.remove(path)
        self.transaction_ids = []
        self.queries = []
        self.tables = []
        self.transaction_type = []
        return True
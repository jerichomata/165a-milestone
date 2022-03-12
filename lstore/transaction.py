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
        self.query_ids = []
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
        self.queries.append((query, table, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query in self.queries:
            result = query[0](*query[2])
            
            # If the query has failed the transaction should abort
            if result == False:
                self.abort()
                return False
            else:
                if query[0].__name__ == "insert" or query[0].__name__ == "update":
                    self.query_ids.append([query[0].__name__, result, query[1]])
        
        self.commit()
        return True

    def undo(self, query_id, table):
        with open("./log/" + table.name + "/"  + str(query_id), 'rb') as file:
            query_log = pickle.load(file)
        if query_log['operation'] == "update":
            table.undo_update(query_log['rid'], query_log['old_rid'], query_log['old_indirection'], query_log['old_schema'])
        if query_log['operation'] == "insert":
            table.undo_insert(query_log['rid'])

    def abort(self):
        for query in self.query_ids:
            print(query[0], query[1])
            query[2].undo(query[1])
        self.release_locks()
        return False

    def commit(self):
        self.release_locks()
        return True

    def release_locks(self):
        for query in self.query_ids:
            if query[0] != "select":
                with open("./log/" + query[2].name + "/"  + str(query[1]), 'rb') as file:
                    query_log = pickle.load(file)
                if query[0] == "insert":
                    query[2].lock_manager.release_lock(query_log['rid'])
                elif query[0] == "update":
                    query[2].lock_manager.release_lock(query_log['rid'])
                    query[2].lock_manager.release_lock(query_log['old_rid'])

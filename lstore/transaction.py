from lstore.table import Table, Record
from lstore.index import Index
from lstore.logger import Logger
import threading
import os
import pickle


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append({'function': query, 'args': args, 'table': table, 'key': args[0], 'name': query.__name__, 'id': None})
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        lock = threading.Lock()
        for query in self.queries:
            lock.acquire()
            passes_lock = query['table'].lock_manager.check_lock(query['key'], query['name'])
            get_lock = query['table'].lock_manager.get_lock(query['key'])
            # print(passes_lock)
            if(passes_lock):
                if query['name'] == "update" or query['name'] == "insert":
                    query['table'].lock_manager.set_lock(query['key'])

                elif query['name'] == "select":
                    query['table'].lock_manager.set_shared(query['key'])
                    query['table'].lock_manager.count[query['key']]+=1

                lock.release()

                query['id'] = query['function'](*query['args'])

                lock.acquire()
                if(query['name'] == "select"):
                    query['table'].lock_manager.count[query['key']]-=1
                    if query['table'].lock_manager.count[query['key']] == 0:
                        query['table'].lock_manager.release_lock(query['key'])
                lock.release()
                
            else:
                lock.release()
                print("aborted", query['key'], query['name'], get_lock)
                self.abort()
                return False

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
        # print([x['id'] for x in self.queries if(x['id'] != None)])
        for query in self.queries:
            if(query['id'] != None and query['name'] != "select"):
                self.undo(query['id'], query['table'])

    def commit(self):
        cwd = os.getcwd()
        for query in self.queries:
            query['table'].lock_manager.release_lock(query['key'])
            path = cwd + "\log\\" + query['table'].name + "\\"  + str(query['id'])
            # try:
            #     os.remove(path)
            # except:
            #     pass
        
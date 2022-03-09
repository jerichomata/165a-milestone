from lstore.table import Table, Record
from lstore.index import Index
from lstore.logger import Logger
import threading


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
        self.queries.append((query, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            #Acquire the lock to check if the record is ok to continue
            self.threading_lock.acquire()
            can_execute = self.lock_switch(args[0].check_lock)
            if(can_execute):
                self.threading_lock.release()
                result = query(args)
            # If the query has failed the transaction should abort
            if result == False or can_execute == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database

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
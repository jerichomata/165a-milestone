import pickle
import os

lock_table = {}



def check_lock(self, base_rid, query_type):
    result = lock_table[base_rid] 
    if result.equals("unlocked"):
        return True
    elif result.equals("locked"):
        return False
    elif result.equals("shared") and query_type.equals("select"):
        return True
    else:
        return False

def make_shared(self, base_rid):
    lock_table[base_rid] = "shared"
def make_lock(self, base_rid):
    lock_table[base_rid] = "locked"
def make_unlock(self, base_rid):
    lock_table[base_rid] = "unlocked"

def undo(self, transaction_id):
    cwd = os.getcwd()
    with open(cwd + "\log" + "\\" + transaction_id, 'rb') as file:
        transaction = pickle.load(file)
    match transaction['operation']:
        case "update":
            self.undo_update(transaction)
        case "insert":
            self.undo_insert(transaction)

def undo_update(self, transaction):
    

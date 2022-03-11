import pickle
import os

lock_table = {}

class LockManager:

    def __init__(self, table):
        self.table = table
        self.lock_table = {}

    def check_lock(self, key, query_type):
        if key not in self.lock_table.keys():
            self.lock_table[key] = "unlocked"

        result = self.lock_table[key] 
        if result == "unlocked":
            return True
        elif result == "locked":
            return False
        elif result == "shared" and query_type == "select":
            return True
        else:
            return False

    def set_shared(self, key):
        self.lock_table[key] = "shared"

    def set_lock(self, key):
        self.lock_table[key] = "locked"

    def release_lock(self, key):
        self.lock_table[key] = "unlocked"

    def undo(self, transaction_id):
        with open("./log/" + self.table.name + "/"  + transaction_id, 'rb') as file:
            transaction = pickle.load(file)
        if transaction['operation'] == "update":
            self.undo_update(transaction)
        if transaction['operation'] == "insert":
            self.undo_insert(transaction)


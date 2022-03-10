import pickle
import os
import threading
class Logger:

    def __init__(self, table_name):
        cwd = os.getcwd()
        self.table_name = table_name
        self.path = cwd + +"\\" + table_name + "\log\\"
        self.current_transaction = 0
        self.logger_transaction = {}

    # self.logger_dictionary[transaction_id] = {["function", old_rid, old_key, new_rid, new_key, commit_bool]}
    def log_insert(self, new_record, lock):
        lock.acquire()
        self.current_transaction+=1
        self.logger_transaction = {'id': self.current_transaction, 'operation': "write", 'rid': new_record.rid, 'key': new_record.key , 'commited': False, 'aborted': False}
        with open(self.path + str(self.current_transaction), 'wb') as file:
            pickle.dump(self.logger_transaction, file)
        lock.release()

    def log_update(self, new_record, old_rid, old_indirection, old_schema, lock):
        lock.acquire()
        self.current_transaction+=1
        self.logger_transaction = {'id': self.current_transaction, 'operation': "update", 'key': new_record.key, 'old_rid': old_rid, 'indirection': old_indirection, 'schema': old_schema, 'rid': new_record.rid, 'commited': False, 'aborted': False}
        with open(self.path + str(self.current_transaction), 'wb') as file:
            pickle.dump(self.logger_transaction, file)
        lock.release()

    def log_delete(self, record, lock):
        lock.acquire()
        self.current_transaction+=1
        self.logger_transaction = {'id': self.current_transaction, 'operation': "delete", 'rid': record.rid, 'key': record.key , 'commited': False, 'aborted': False}
        with open(self.pat + str(self.current_transaction), 'wb') as file:
            pickle.dump(self.logger_transaction, file)
        lock.release()


    def log_commit(self, transaction_id, lock):
        lock.acquire()
        self.logger_dictionary[transaction_id]['commited'] = True
        with open(self.path + str(self.current_transaction), 'wb') as file:
            pickle.dump(self.logger_transaction, file)
        lock.release()

    def log_abort(self, transaction_id, lock):
        lock.acquire()
        self.logger_dictionary[transaction_id]['aborted'] = True
        with open(self.path + str(self.current_transaction), 'wb') as file:
            pickle.dump(self.logger_transaction, file)
        lock.release()


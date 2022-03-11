import pickle
import os
import threading
class Logger:

    def __init__(self, table_name):
        cwd = os.getcwd()
        self.table_name = table_name
        self.path = "./log/" + table_name + "/"
        self.current_query = 0
        self.logger_query = {}

    # self.logger_dictionary[query_id] = {["function", old_rid, old_key, new_rid, new_key, commit_bool]}
    def log_insert(self, new_record, lock):
        lock.acquire()
        self.current_query+=1
        self.logger_query = {'id': self.current_query, 'operation': "write", 'rid': new_record.rid, 'key': new_record.key , 'commited': False, 'aborted': False, 'table': self.table_name}
        with open(self.path + str(self.current_query), 'wb') as file:
            pickle.dump(self.logger_query, file)
        return_id = self.current_query
        lock.release()
        return return_id

    def log_update(self, new_record, old_rid, old_indirection, old_schema, lock):
        lock.acquire()
        self.current_query+=1
        self.logger_query = {'id': self.current_query, 'operation': "update", 'key': new_record.key, 'old_rid': old_rid, 
            'indirection': old_indirection, 'schema': old_schema, 'rid': new_record.rid, 'commited': False, 'aborted': False, 'table': self.table_name}
        with open(self.path + str(self.current_query), 'wb') as file:
            pickle.dump(self.logger_query, file)
        return_id = self.current_query
        lock.release()
        return return_id

    def log_delete(self, record, lock):
        lock.acquire()
        self.current_query+=1
        self.logger_query = {'id': self.current_query, 'operation': "delete", 'rid': record.rid, 'key': record.key , 'commited': False, 'aborted': False, 'table': self.table_name}
        with open(self.pat + str(self.current_query), 'wb') as file:
            pickle.dump(self.logger_query, file)
        lock.release()

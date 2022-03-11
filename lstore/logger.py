import pickle
import os
import threading
class Logger:

    def __init__(self, table_name):
        self.table_name = table_name
        self.path = str("./log/" + table_name + "/")
        self.current_query = 0
        self.logger_query = {}
        os.makedirs(self.path, exist_ok=True)

    # self.logger_dictionary[query_id] = {["function", old_rid, old_key, new_rid, new_key, commit_bool]}
    def log_insert(self, new_record):
        self.current_query+=1
        self.logger_query = {'id': self.current_query, 'operation': "write", 'rid': new_record.rid, 'key': new_record.key , 'commited': False, 'aborted': False, 'table': self.table_name}
        with open(self.path + "/" + str(self.current_query), 'wb') as file:
            pickle.dump(self.logger_query, file)
        return_id = self.current_query
        return return_id

    def log_update(self, new_record, old_rid, old_indirection, old_schema):
        self.current_query+=1
        self.logger_query = {'id': self.current_query, 'operation': "update", 'key': new_record.key, 'old_rid': old_rid, 
            'indirection': old_indirection, 'schema': old_schema, 'rid': new_record.rid, 'commited': False, 'aborted': False, 'table': self.table_name}
        with open(self.path + str(self.current_query), 'wb') as file:
            pickle.dump(self.logger_query, file)
        return_id = self.current_query
        return return_id

    def log_delete(self, record):
        self.current_query+=1
        self.logger_query = {'id': self.current_query, 'operation': "delete", 'rid': record.rid, 'key': record.key , 'commited': False, 'aborted': False, 'table': self.table_name}
        with open(self.pat + str(self.current_query), 'wb') as file:
            pickle.dump(self.logger_query, file)

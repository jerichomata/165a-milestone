import pickle
import os
class Logger:

    def __init__(self, table_name):
        self.table_name = table_name
        self.path = "./log/" + table_name +"/" 
        self.current_query = 0
        os.makedirs(self.path, exist_ok=True)

    # self.logger_dictionary[transaction_id] = {["function", old_rid, old_key, new_rid, new_key, commit_bool]}
    def log_insert(self, new_record, lock):
        lock.acquire()
        self.current_query+=1
        current_query = self.current_query
        lock.release()
        logger_transaction = {'id': current_query, 'operation': "write", 'rid': new_record.rid, 'key': new_record.key , 'commited': False, 'aborted': False}
        
        with open(self.path + str(current_query), 'wb') as file:
            pickle.dump(logger_transaction, file)

        return current_query

    def log_update(self, new_record, old_rid, old_indirection, old_schema, lock):
        lock.acquire()
        self.current_query+=1
        current_query = self.current_query
        lock.release()
        logger_transaction = {'id': current_query, 'operation': "update", 'key': new_record.key, 'old_rid': old_rid, 'indirection': old_indirection, 'schema': old_schema, 'rid': new_record.rid, 'commited': False, 'aborted': False}
        
        with open(self.path + str(current_query), 'wb') as file:
            pickle.dump(logger_transaction, file)

        

        return current_query


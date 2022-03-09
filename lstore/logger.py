import pickle
import os
class Logger:

    def init(self):
        cwd = os.getcwd()
        self.path = cwd + "\log.txt"
        self.current_transaction = 0
        self.logger_dictionary = {}
        if(os.exists(self.path)):
            with open(self.path, 'rb') as file:
                self.logger_dictionary = pickle.load(file)

    # self.logger_dictionary[transaction_id] = {["function", old_rid, old_key, new_rid, new_key, commit_bool]}
    def log_write(self, new_record):
        self.current_transaction+=1
        self.logger_dictionary[self.current_transaction] = {'operation': "write", 'rid': new_record.rid, 'key': record. , 'commited': False, 'aborted': False}
        with open(self.path, 'wb') as file:
            pickle.dump(self.logger_dictionary, file)

    def log_update(self, record, new_record):
        self.current_transaction+=1
        updated_columns = len(record.columns)-4
        indirection_column = len(record.columns)-3
        self.logger_dictionary[self.current_transaction] = {'operation': "update", 'key': record.key, 'old_rid': record.rid, 'indirection': record.columns[indirection_column], 'schema': record.columns[-1], 'rid': new_record.rid , 'query': new_record.columns[:updated_columns], 'commited': False, 'aborted': False}
        with open(self.path, 'wb') as file:
            pickle.dump(self.logger_dictionary, file)


    def log_delete(self, record):
        self.current_transaction+=1
        self.logger_dictionary[self.current_transaction] = {'operation': "delete", 'rid': record.rid, 'key': record.key , 'commited': False, 'aborted': False}
        with open(self.path, 'wb') as file:
            pickle.dump(self.logger_dictionary, file)


    def log_commit(self, transaction_id):
        self.logger_dictionary[transaction_id]['commited'] = True
        with open(self.path, 'wb') as file:
            pickle.dump(self.logger_dictionary, file)

    def log_abort(self, transaction_id):
        self.logger_dictionary[transaction_id]['aborted'] = True
        with open(self.path, 'wb') as file:
            pickle.dump(self.logger_dictionary, file)


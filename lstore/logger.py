import pickle

class Logger:

    def init(self):
        self.current_transaction = 0
        self.logger_dictionary = {}

        pass

    # self.logger_dictionary[transaction_id] = {["function", old_rid, old_key, new_rid, new_key, commit_bool]}
    def log_write(self, new_record):
        self.logger_dictionary[self.current_transaction] = {["write", new_record.rid, new_record.key , False]}
        log = open('log', 'a')
        pickle.dump(self.logger_dictionary, log)
        log.close()

        pass

    def log_update(self, record, new_record):
        self.logger_dictionary[self.current_transaction] = {["update", record.rid, record.key, new_record.rid, new_record.key, False]}
        log = open('log', 'a')
        pickle.dump(self.logger_dictionary, log)
        log.close()

        pass

    def log_delete(self, record):
        self.logger_dictionary[self.current_transaction] = {{"delete", record.key, False}}
        log = open('log', 'a')
        pickle.dump(self.logger_dictionary, log)
        log.close()

        pass

    def log_commit(self, transaction_id):
        self.logger_dictionary[transaction_id][-1] = True
        log = open('log', 'a')
        pickle.dump(self.logger_dictionary, log)
        log.close()

        pass

    def log_abort(self, transaction_id):
        self.logger_dictionary[transaction_id].append("aborted")
        log = open('log', 'a')
        pickle.dump(self.logger_dictionary, log)
        log.close()
        
        pass


from lstore.table import Table, Record
from lstore.transaction import Transaction
import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        self.thread = threading.Thread(target= self.__run)
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread.start()
        pass
    

    """
    Waits for the worker to finish
    """
    def join(self):
        self.thread.join()
        pass


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

        if False in self.stats:
            for transaction in self.transactions:
                transaction.abort()
        else:
            for transaction in self.transactions:
                transaction.commit()



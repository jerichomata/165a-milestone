
from operator import truediv


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if(self.num_records < len(self.data)):
            return True
        return False


    def write(self, value):
        if self.has_capacity():
            self.data[self.num_records] = value
            self.num_records += 1
            print("Written" + value)
        else:
            print("Could not write")



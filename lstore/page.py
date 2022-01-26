
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):

    def write(self, value):
        if self.has_capacity():
            self.num_records += 1
            self.data.append(value)
            print("Written" + value)
        else:



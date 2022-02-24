with open( ".\lstore\disk\Grades\B1-0", 'rb') as file:
        lines = file.read(5004)
        print(bytearray(lines[32:40]))
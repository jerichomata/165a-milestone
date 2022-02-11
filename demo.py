from lstore.db import Database
from lstore.query import Query
from random import choice, randrange

def demo():
    demo = Database()
    demo_table = demo.create_table('Grades', 5, 0)
    demo_query = Query(demo_table)
    demo_keys = []

    while (1):
        userInput = input("MILESTONE 1 >> ")
        userCmd = userInput.split(' ')

        if userCmd[0] == "exit":
            print("Have a nice day!")
            break
        elif userCmd[0] == "table":
            print( "{demo_table.name} with {demo_table.num_colums} columns and {demo_table.key} key" )
            break
        elif userCmd[0] == "delete":
            grade = int(userCmd[1])
            demo_del = demo_query.delete(grade)
            if demo_del:
                print("You have deleted", grade)
            else:
                print("Unfortunately grade has not been deleted") 
        elif userCmd[0] == "sum":
            startRange = int(userCmd[1])
            endRange = int(userCmd[2])
            aggrCol = int(userCmd[3])
            demo_sum = demo_query.sum(startRange, endRange, aggrCol)
            if (demo_sum == False):
                print("Grades have not been added")
            else:
                print("Calculated sum is", demo_sum)

        elif userCmd[0] == "insert":
            userCmd.pop(0)
            new_record = []
            for i, arg in enumerate(userCmd):
                if(i == 0):
                    demo_keys.append(int(arg))
                new_record.append(int(arg))

            if(demo_query.insert(*new_record)):
                print("Record Successfully Inserted.")
            else:
                print("Record Insertion Failed.")
            

        elif userCmd[0] == "select":
            userCmd.pop(0)
            index_value = int(userCmd.pop(0))
            index_column = int(userCmd.pop(0))
            query_columns = []
            for arg in userCmd:
                query_columns.append(int(arg))

            records = demo_query.select(index_value, index_column, query_columns)
            print("Selected: " + str(len(records)) + " Records")
            for record in records:
                print(record.columns)

        elif userCmd[0] == "update":
            userCmd.pop(0)
            key = int(userCmd.pop(0))
            new_record = []
            for i, arg in enumerate(userCmd):
                new_record.append(int(arg))

            if(demo_query.update(key, *new_record)):
                print("Record Successfully Updated.")
            else:
                print("Record Update Failed.")

        elif userCmd[0] == "populate":
            for i in range(0, 10000):
                update_cols = [randrange(0, 100), randrange(0, 100), randrange(0, 100), randrange(0, 100)]
                demo_query.insert(i, *update_cols)

        else:
            print("COMMAND NOT FOUND") 

    return

demo()

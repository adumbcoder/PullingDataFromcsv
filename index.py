
import os
import shutil
import time
from datetime import date
import pyodbc
import pandas as pd
from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

# create a watchdog for folder MRO_MCAULIFFES
if __name__ == "__main__":
    patterns = ["*"]
    ignore_patterns = None
    ignore_directories = False
    case_sensitive = True
    my_event_handler = PatternMatchingEventHandler(patterns, ignore_patterns, ignore_directories, case_sensitive)


# Event start
def on_created(event):
    print(f"File was created: {event.src_path}")
    currDate = date.today()
    convDate = currDate.strftime("%m%d%y")
    newFile = event.src_path
    shortFilePath = os.path.basename(newFile)
    if shortFilePath == "basename":
        try:
            # Read the file being created 
            data = pd.read_csv (r'path/to/folder')
            # Insert data into DataFrames
            dfChargeOut = pd.DataFrame(data)
            # Created two columns needed for Epicor
            
            

            print(dfChargeOut)
            # Connect to the database
            conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=server;'
                            'Database=database;'
                            'Trusted_Connection=yes;')
            cursor = conn.cursor()
            
            # Create query for inserting data into sql
            # for index, row in dfChargeOut.iterrows():
            
            cursor.executemany(
                '''
                SQL code goes here
                ''', 
                list(dfChargeOut.itertuples(index=False))
                )
            conn.commit()
            conn.close()
            # If the process was a success move it to processed and append date to filename
            shutil.move(r"path/to/folder" , r"path/to/folder"+ convDate + ".csv")
        except pyodbc.Error as ex:
            sqlstate = ex.args[1]
            sqlstate = sqlstate.split(".")
            print(sqlstate)      
            # If there was an error move file to error folder with date appeneded.
            shutil.move(r"path/to/folder" , r"path/to/folder"+ convDate + ".csv")
    #When receipt file is created in folder 
    elif shortFilePath == "basename":
        try:
            # Read the file being created 
            data = pd.read_csv (r'path/to/folder')
            dfReceipt = pd.DataFrame(data)
            # Insert two new columns needed for epicor side.
            
            print(dfReceipt)
            # Connect to the database
            conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=server;'
                            'Database=database;'
                            'Trusted_Connection=yes;')
            cursor = conn.cursor()
            # Create query for inserting data into sql
            # for row in dfReceipt.itertuples():
            cursor.executemany(
            '''
            Sql query goes here
            ''',
            list(dfReceipt.itertuples(index=False)) 
            )
            conn.commit()
            conn.close()

            shutil.move(r"path/to/folder" , r"path/to/folder"+ convDate + ".csv")

        except pyodbc.Error as ex:
            sqlstate = ex.args[1]
            sqlstate = sqlstate.split(".")
            print(sqlstate)
            shutil.move(r"path/to/folder" , r"path/to/folder"+ convDate + ".csv")




my_event_handler.on_created = on_created

path = "path/of/folder/to/watch"
go_recursively = True
my_observer = Observer()
my_observer.schedule(my_event_handler, path, recursive=go_recursively)

my_observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    my_observer.stop()
    my_observer.join()









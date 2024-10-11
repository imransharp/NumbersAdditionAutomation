# add employee name with department and employee number.

import mysql.connector
import pandas as pd

# Connect to the MySQL database
def connect_to_db():
    connection = mysql.connector.connect(
    
    )
    return connection

    connection.close()

if __name__ == "__main__":
    process_numbers()

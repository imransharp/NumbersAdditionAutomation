print("hello world")
import mysql.connector

# Step 1: Connect to the MySQL database
def connect_to_db():
    connection = mysql.connector.connect(
        host="172.21.163.162",       # Replace with your MySQL host
        user="root",   # Replace with your MySQL username
        password="Z0ng@311#315!",  # Replace with your MySQL password
        database="jxb_zongtrack_lbsdev"  # Replace with your MySQL database name
    )
    return connection


# Step 2: Insert number into tbl_whitelist with '92' prepended
def insert_number_to_whitelist(connection, number):
    cursor = connection.cursor()
    number_with_prefix = f"92{number}"  # Prepend '92' to the number
    query = "INSERT INTO tbl_whitelist (msisdn) VALUES (%s)"  # Replace 'number_column_name' with the actual column name in tbl_whitelist
    cursor.execute(query, (number_with_prefix,))
    connection.commit()

# Step 3: Read numbers from the text file stored in D drive
def read_numbers_from_file(file_path):
    with open(file_path, 'r') as file:
        numbers = [line.strip() for line in file]  # Keep numbers as strings to handle large values
    return numbers

# Step 4: Main process to read and insert numbers
def process_numbers():
    connection = connect_to_db()

    numbers = read_numbers_from_file('D:\\auto_upload_folder\\test.txt')  # Path to your file on D drive

    for number in numbers:
        insert_number_to_whitelist(connection, number)

    connection.close()

if __name__ == "__main__":
    process_numbers()

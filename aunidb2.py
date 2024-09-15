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

# Step 2: Check if number exists in tbl_whitelist
def check_if_number_exists(connection, number_with_prefix):
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM tbl_whitelist WHERE msisdn = %s"  # Replace 'number_column_name' with your actual column name
    cursor.execute(query, (number_with_prefix,))
    result = cursor.fetchone()[0]  # Get the count of matching rows
    return result > 0  # Return True if number exists

# Step 3: Insert number into tbl_whitelist with '92' prepended
def insert_number_to_whitelist(connection, number):
    number_with_prefix = f"92{number}"  # Prepend '92' to the number

    # Check if number already exists
    if not check_if_number_exists(connection, number_with_prefix):
        cursor = connection.cursor()
        query = "INSERT INTO tbl_whitelist (msisdn) VALUES (%s)"  # Replace 'number_column_name' with the actual column name
        cursor.execute(query, (number_with_prefix,))
        connection.commit()
        print(f"Inserted {number_with_prefix} into tbl_whitelist")
    else:
        print(f"{number_with_prefix} already exists in tbl_whitelist")

# Step 4: Fetch ntn and client_name from clients table
def get_ntn_and_client_name(connection, client_search_name):
    cursor = connection.cursor()
    query = "SELECT client_ntn, client_name FROM clients WHERE client_name LIKE %s"
    cursor.execute(query, (f"%{client_search_name}%",))  # Search with wildcard
    result = cursor.fetchone()  # Fetch the first match
    if result:
        return result
    else:
        print(f"No matching client found for '{client_search_name}'")
        return None, None

# Step 5: Insert number into ntn_records with msisdn, ntn, and client_name
def insert_number_to_ntn_records(connection, msisdn, ntn, client_name):
    cursor = connection.cursor()
    query = "INSERT INTO ntn_records (msisdn, ntn, client_name) VALUES (%s, %s, %s)"
    cursor.execute(query, (msisdn, ntn, client_name))
    connection.commit()
    print(f"Inserted {msisdn} with ntn {ntn} and client {client_name} into ntn_records")


# Step 0: Check if employee already exists based on msisdn
def check_if_employee_exists(connection, msisdn):
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM employees WHERE employee_username = %s"
    cursor.execute(query, (msisdn,))
    result = cursor.fetchone()[0]  # Get the count of matching rows
    return result > 0  # Return True if employee exists


# Step 0: Get client_id from the clients table based on client_name
def get_client_id(connection, client_name):
    cursor = connection.cursor()
    query = "SELECT client_id FROM clients WHERE client_name LIKE %s"
    cursor.execute(query, (f"%{client_name}%",))
    result = cursor.fetchone()  # Fetch the first match
    if result:
        return result[0]  # Return the client_id
    else:
        print(f"No matching client found for '{client_name}'")
        return None


# Step 0: Insert employee into the employees table
def insert_employee(connection, employee_name, employee_username, msisdn, client_id):
    # Static values
    employee_email = "lbs_employee@zong.com.pk"
    is_lbs_enabled = 1
    department_id = 0  # This will be updated later
    is_employee = 1

    # Check if employee already exists
    if not check_if_employee_exists(connection, msisdn):
        cursor = connection.cursor()
        query = """
            INSERT INTO employees (employee_name, employee_username, employee_email, 
                                   is_lbs_enabled, department_id, is_employee, client_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (employee_name, employee_username, employee_email, 
                               is_lbs_enabled, department_id, is_employee, client_id))
        connection.commit()
        print(f"Inserted employee {employee_name} with msisdn {msisdn} into employees")
    else:
        print(f"Employee with msisdn {msisdn} already exists in employees")


# Step 4: Read numbers from the text file stored in D drive
def read_numbers_from_file(file_path):
    with open(file_path, 'r') as file:
        numbers = [line.strip() for line in file]  # Keep numbers as strings to handle large values
    return numbers

# Step 5: Main process to read and insert numbers
def process_numbers():
    connection = connect_to_db()

    numbers = read_numbers_from_file('D:\\auto_upload_folder\\test.txt')  # Path to your file on D drive

    # for number in numbers:
    #     insert_number_to_whitelist(connection, number)

# Get client name from console input
    client_search_name = input("Enter the client name to search (e.g., 'Road Prince Group'): ")

    # Fetch ntn and client_name for the provided client name
    ntn, client_name = get_ntn_and_client_name(connection, client_search_name)

    # Fetch client_id for the provided client name
    client_id = get_client_id(connection, client_search_name)

    if ntn and client_name:
        for number in numbers:
            insert_number_to_whitelist(connection, number)  # Insert into tbl_whitelist

            msisdn_with_prefix = f"92{number}"  # Prepend '92' for msisdn
            insert_number_to_ntn_records(connection, msisdn_with_prefix, ntn, client_name)  # Insert into ntn_records

            if client_id:                                
                msisdn_with_prefix = f"92{number}"  # Prepend '92' for msisdn
                # Insert into employees table
                insert_employee(connection, msisdn_with_prefix, msisdn_with_prefix, msisdn_with_prefix, client_id)

    # if client_id:
    #     for number in numbers:
    #         # Here, you can replace these placeholder employee_name and employee_username values with actual values
    #         employee_name = number
    #         employee_username = number
            
    #         msisdn_with_prefix = f"92{number}"  # Prepend '92' for msisdn

    #         # Insert into employees table
    #         insert_employee(connection, employee_name, employee_username, msisdn_with_prefix, client_id)

        connection.close()

if __name__ == "__main__":
    process_numbers()

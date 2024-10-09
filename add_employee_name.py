# add employee name with department and employee number.

import mysql.connector
import pandas as pd

# Connect to the MySQL database
def connect_to_db():
    connection = mysql.connector.connect(
    
    )
    return connection

# Check if number exists in tbl_whitelist
def check_if_number_exists(connection, number_with_prefix):
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM tbl_whitelist WHERE msisdn = %s"
    cursor.execute(query, (number_with_prefix,))
    result = cursor.fetchone()[0]  
    return result > 0 

# Fetch ntn and client_name from clients table
def get_ntn_and_client_name(connection, client_search_name):
    cursor = connection.cursor()
    query = "SELECT client_ntn, client_name FROM clients WHERE client_name LIKE %s"
    cursor.execute(query, (f"%{client_search_name}%",))  
    result = cursor.fetchone() 
    if result:
        return result
    else:
        print(f"No matching client found for '{client_search_name}'")
        return None, None

# Check if employee already exists based on msisdn
def check_if_employee_exists(connection, msisdn):
    cursor = connection.cursor()
    query = "SELECT COUNT(*) FROM employees WHERE employee_username = %s"
    cursor.execute(query, (msisdn,))
    result = cursor.fetchone()[0] 
    return result > 0 

# Get client_id from the clients table based on client_name
def get_client_id(connection, client_name):
    cursor = connection.cursor()
    query = "SELECT client_id FROM clients WHERE client_name LIKE %s"
    cursor.execute(query, (f"%{client_name}%",))
    result = cursor.fetchone()  
    if result:
        return result[0]  
    else:
        print(f"No matching client found for '{client_name}'")
        return None

# Get department_id from the clients table based on client_name
def get_department_id(connection, client_name):
    cursor = connection.cursor()
    query = "SELECT client_id FROM clients WHERE client_name LIKE %s"
    cursor.execute(query, (f"%{client_name}%",))
    result = cursor.fetchone()  
    if result:
        return result[0]  
    else:
        print(f"No matching client found for '{client_name}'")
        return None

# Read Excel file stored in the given path
def read_excel_file(file_path):
    df = pd.read_excel(file_path)
    return df

# Main process to read and insert numbers and employee names
def process_numbers():
    connection = connect_to_db()

    # Read the Excel file from D drive
    file_path = 'D:\\auto_upload_folder\\add_employee_name.xlsx'
    df = read_excel_file(file_path)

    # Get client name from console input
    client_search_name = input("Enter the client name to search (e.g., 'Road Prince Group'): ")

    while True:
        # Fetch client_id for the provided client name
        client_id = get_client_id(connection, client_search_name)

        if client_id:
            # Fetch the 'created_by' field using the retrieved client_id
            query = f"SELECT created_by FROM employees WHERE client_id = {client_id} AND is_admin = 0 LIMIT 1;"
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                created_by = result[0]
                print(f"Created By: {created_by}")

                # Get department input from the user
                department_name = input("Enter the department name (e.g., 'Department 1'): ")

                # Fetch department details using created_by and department_name
                department_query = "SELECT * FROM departments WHERE created_by = %s AND department_name LIKE %s;"
                cursor.execute(department_query, (created_by, f"%{department_name}%"))
                department_info = cursor.fetchone()

                if department_info:
                    print(f"Department Info: {department_info}")
                    department_id = department_info[0]

                    # Insert all the numbers and names from the Excel file into the employees table
                    for index, row in df.iterrows():
                        try:
                            number = row['Number']
                            name = row['Name']
                            number_with_prefix = f"92{number}"  # Prepend '92' to the number

                            insert_employee_query = f"""
                                INSERT INTO employees (employee_name, employee_username, employee_email, employee_number,
                                employee_password, is_lbs_enabled, is_employee, client_id, department_id, created_by)
                                VALUES ('{name}', '{number_with_prefix}', 'lbs_employee@zong.com.pk', '{number_with_prefix}',
                                '982f790301d0cf13cba2b52fc4add9168824e71d5debf19e24b87a2f3f0c4322', 1, 1, {client_id}, {department_id}, {created_by})
                                ON DUPLICATE KEY UPDATE employee_name = VALUES(employee_name);
                            """
                            
                            cursor.execute(insert_employee_query)
                            print(f"Added or updated employee: {name}, {number_with_prefix}")
                        except Exception as e:
                            print(f"Failed to insert or update number {number_with_prefix}: {e}")

                       

                    # Commit the transaction after inserting all numbers and names
                    connection.commit()
                    print("All employees have been successfully added to the employees table.")
                else:
                    print(f"No department found with name '{department_name}' for created_by '{created_by}'.")
                break
            else:
                print("No employees found with the given client ID. Please enter another client name.")
                client_search_name = input("Enter another client name: ")
        else:
            print("Client not found. Please enter a valid client name.")
            client_search_name = input("Enter another client name: ")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    process_numbers()

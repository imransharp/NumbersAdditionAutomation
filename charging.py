import mysql.connector

# Step 1: Connect to the MySQL database
def connect_to_db():
    connection = mysql.connector.connect(
        host="172.21.163.162",       # Replace with your MySQL host
        user="root",                 # Replace with your MySQL username
        password="Z0ng@311#315!",    # Replace with your MySQL password
        database="jxb_zongtrack"  # Replace with your MySQL database name
    )
    return connection

# Step 2: Retrieve the list of numbers from log_charging where conditions match
def get_failed_numbers(connection):
    cursor = connection.cursor()
    
    query = """
        SELECT c.client_master_msisdn 
        FROM log_charging lc
        INNER JOIN clients c ON lc.client_id = c.client_id
        WHERE lc.clog_month = 'September-2024'
        AND lc.clog_charging_status = 'FAILED'
        AND c.client_type = 'client'
        AND c.client_status = 'active'
        AND NOT EXISTS (
            SELECT 1
            FROM log_charging lc2
            WHERE lc2.clog_charging_status = 'SUCCESS'
            AND lc2.clog_month = lc.clog_month
            AND lc2.client_id = lc.client_id
        )
    """
    
    cursor.execute(query)
    failed_numbers = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    return failed_numbers

# Step 3: Check if charging is done for the list of numbers and log results
def check_charging_for_numbers(connection, numbers, year, month):
    cursor = connection.cursor()
    charged_numbers = []
    
    # SQL query to check if charging is done
    query = f"""
    SELECT 
        mml_msisdn,
        SUM(CAST(SUBSTRING(
                mml_result, 
                LOCATE('FEE=', mml_result) + 4, 
                LOCATE(' ', mml_result, LOCATE('FEE=', mml_result)) - (LOCATE('FEE=', mml_result) + 4)
            ) AS UNSIGNED) / 100) AS total_fee
    FROM log_mml
    WHERE mml_type = 'DEDUCTBALANCE'
    AND mml_msisdn IN ({', '.join(['%s'] * len(numbers))})
    AND YEAR(datetime) = %s
    AND MONTH(datetime) = %s
    GROUP BY mml_msisdn
    ORDER BY datetime DESC;
    """
    
    cursor.execute(query, (*numbers, year, month))
    results = cursor.fetchall()
    
    # Log numbers that have already been charged
    for row in results:
        msisdn, total_fee = row
        if total_fee > 0:
            charged_numbers.append(msisdn)
        else:
            
    
    # Print and log the charged numbers
    if charged_numbers:
        print("These numbers have already been charged:")
        for number in charged_numbers:
            print(number)
    else:
        print("No numbers have been charged for this month.")
    
    cursor.close()

# Step 4: Main function to handle logic
def main():
    # Connect to the database
    connection = connect_to_db()
    
    # Get the list of numbers that have failed charges but no success in September 2024
    numbers = get_failed_numbers(connection)
    
    # Define the year and month for the charging check
    year = 2024
    month = 9  # September
    
    # Check if charging is done
    if numbers:
        check_charging_for_numbers(connection, numbers, year, month)
    else:
        print("No numbers found with failed charges for the specified criteria.")
    
    # Close the connection
    connection.close()

# Run the main function
if __name__ == "__main__":
    main()

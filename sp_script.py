import logging
import psycopg2

# Configure logging
logging.basicConfig(
    filename='sp.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format of the log messages
)

# Initialize cursor and connection
cur = None
conn = None

try:
    # Database connection setup
    conn = psycopg2.connect(
        dbname="analytics",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("SELECT filter_transact()")  # Calls the procedure
    result_sp = cur.fetchone()  # Fetch a single result
    logging.info("Function filter_transact Output: %s", result_sp)

    # Now call the function to get the count
    cur.execute("SELECT transaction_fn()")  # Ensure transaction_fn is a function

    # Fetch the result from the function
    result = cur.fetchone()  # Fetch a single result
    logging.info("Function transaction_fn Output: %s", result)

except Exception as e:
    logging.error("Error occurred: %s", str(e))

finally:
    # Cleanup
    if cur:
        cur.close()
    if conn:
        conn.close()

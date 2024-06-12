import pandas as pd
import sqlite3

# Function to read data from Excel files
def read_data_from_excel(file_path, region):
    '''
    This function helps to Read the file from file and appending in the list
    '''
    df = pd.read_excel(file_path)
    data = []
    for index, row in df.iterrows():
        order_id = row.get('OrderId')
        if pd.notna(order_id):
            order_id = order_id
        data.append((
            order_id,
            int(row['OrderItemId']),
            int(row['QuantityOrdered']),
            float(row['ItemPrice']),
            row['PromotionDiscount'],
            region
        ))
    return data

# Function to transform data
def transform_data(data_a, data_b):
    '''
    Function for Transforming the data from both files
    argumens : data_a and data_b
    returns combined and transformed data
    '''
    combined_data = data_a + data_b # combin both data
    seen_order_ids = set() #Ensuring no duplicates using set
    transformed_data = []

    for row in combined_data:
        order_id, order_item_id, quantity_ordered, item_price, promotion_discount, region = row
        total_sales = quantity_ordered * item_price

        if order_id is not None:
            if order_id not in seen_order_ids:
                seen_order_ids.add(order_id)
            else:
                continue
        
        transformed_data.append((
            order_item_id,
            order_id,
            quantity_ordered,
            item_price,
            total_sales,
            region,
            promotion_discount
        ))

    return transformed_data

# Function to set up the database
def setup_database(db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS sales_data (
        OrderItemId INTEGER PRIMARY KEY,
        OrderId INTEGER,
        QuantityOrdered INTEGER,
        ItemPrice REAL,
        TotalSales REAL,
        Region TEXT,
        PromotionDiscount REAL
    )
    ''')
    
    conn.commit()
    conn.close()

# Function to load data into the database
def load_data_to_db(transformed_data, db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    for row in transformed_data:
        cur.execute('''
        INSERT OR REPLACE INTO sales_data (
            OrderItemId, OrderId, QuantityOrdered, ItemPrice, TotalSales, Region, PromotionDiscount
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', row)
    
    conn.commit()
    conn.close()

def validate_data(db_name='sales_data.db'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    
    # a. Count the total number of records
    cur.execute('SELECT COUNT(*) AS total_records FROM sales_data')
    total_records = cur.fetchone()[0]
    print(f"Total number of records: {total_records}")

    # b. Find the total sales amount by region
    cur.execute('''
    SELECT Region, SUM(TotalSales) AS total_sales_amount
    FROM sales_data
    GROUP BY Region
    ''')
    total_sales_by_region = cur.fetchall()
    for row in total_sales_by_region:
        print(f"Region: {row[0]}, Total Sales Amount: {row[1]}")

    # c. Find the average sales amount per transaction
    cur.execute('SELECT AVG(TotalSales) AS average_sales_amount_per_transaction FROM sales_data')
    average_sales_amount_per_transaction = cur.fetchone()[0]
    print(f"Average sales amount per transaction: {average_sales_amount_per_transaction}")

    # d. Ensure there are no duplicate OrderItemId values
    cur.execute('''
    SELECT OrderItemId, COUNT(*) AS count
    FROM sales_data
    GROUP BY OrderItemId
    HAVING count > 1
    ''')
    duplicate_ids = cur.fetchall()
    if not duplicate_ids:
        print("No duplicate OrderItemId values found.")
    else:
        print("Duplicate OrderItemId values found:")
        for row in duplicate_ids:
            print(f"OrderItemId: {row[0]}, Count: {row[1]}")

    conn.close()


if __name__ == '__main__':
    # Read sales data from both the files
    data_a = read_data_from_excel('order_region_a.xlsx', 'A')
    data_b = read_data_from_excel('order_region_b.xlsx', 'B')

    #data transformation driver function
    transformed_data = transform_data(data_a, data_b)

    #Setup database operation sqlite3
    setup_database()

    #import data to db
    load_data_to_db(transformed_data)
    validate_data(db_name='sales_data.db')

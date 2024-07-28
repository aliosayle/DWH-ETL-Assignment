import pandas as pd
from sqlalchemy import create_engine

def main():

    # Database connection strings
    staging_db_connection_string = 'mysql+pymysql://root:password@localhost/SalesStagingDB'
    dwh_db_connection_string = 'mysql+pymysql://root:password@localhost/SalesDWHDB'

    # Create database engines
    staging_db_engine = create_engine(staging_db_connection_string)
    dwh_db_engine = create_engine(dwh_db_connection_string)

    def load_table_from_staging(table_name):
        return pd.read_sql_table(table_name, staging_db_engine)

    def save_table_to_dwh(df, table_name):
        df.to_sql(table_name, dwh_db_engine, if_exists='replace', index=False)

    # Extract data from staging
    cities_df = load_table_from_staging('Cities')
    customers1_df = load_table_from_staging('Customers1')
    customers_detailed_df = load_table_from_staging('Customers_Detailed')
    international_sales_df = load_table_from_staging('International_Sales')
    orders_df = load_table_from_staging('Orders')
    people_df = load_table_from_staging('People')
    postal_codes_df = load_table_from_staging('Postal_Codes')
    product_df = load_table_from_staging('Product')
    product_category_df = load_table_from_staging('Product_Category')
    product_sub_category_df = load_table_from_staging('Product_Sub_Category')
    segment_df = load_table_from_staging('Segment')
    ship_mode_df = load_table_from_staging('Ship_Mode')

    product_df.rename(columns={
        'Product ID': 'ProductID',
        'Cat ID': 'CatID',
        'Sub Cat ID': 'SubCatID',
        'Product Name': 'ProductName'
    }, inplace=True)

    product_df = product_df.drop_duplicates(subset=['ProductID'])

    product_df['CatID'] = product_df['CatID'].astype(str)
    product_df['CatID'] = product_df['CatID'].str.strip()
    batch_size = 1000

    for start in range(0, len(product_df), batch_size):
        end = start + batch_size
        product_df.iloc[start:end].to_sql('Product', dwh_db_engine, if_exists='append', index=False)


    cities_df = cities_df.drop_duplicates(subset='City', keep='first')
    cities_df.to_sql('Cities', dwh_db_engine, if_exists='append', index=False)

    customers1_df = customers1_df.drop_duplicates(subset='Customer ID', keep='first')
    customers1_df.rename(columns={'Customer ID': 'CustomerID', 'Customer Name': 'CustomerName'}, inplace=True)
    customers1_df.to_sql('Customers', dwh_db_engine, if_exists='append', index=False)

    column_mapping = {
        'CUSTOMERNAME': 'CustomerName',
        'PHONE': 'Phone',
        'ADDRESSLINE1': 'AddressLine1',
        'ADDRESSLINE2': 'AddressLine2',
        'CITY': 'City',
        'STATE': 'State',
        'POSTALCODE': 'PostalCode',
        'COUNTRY': 'Country',
        'TERRITORY': 'Territory',
        'CONTACTLASTNAME': 'ContactLastName',
        'CONTACTFIRSTNAME': 'ContactFirstName',
        'DEALSIZE': 'DealSize'
    }

    customers_detailed_df.rename(columns=column_mapping, inplace=True)
    customers_detailed_df.to_sql('CustomersDetailed', dwh_db_engine, if_exists='replace', index=False)

    international_sales_df.to_sql('InternationalSales', dwh_db_engine, if_exists='replace', index=False)
    ship_mode_df.rename(columns={'ID':'ShipModeID',
                                 'Ship Mode': 'ShipMode'}, inplace=True)
    print(ship_mode_df.info())
    ship_mode_df.to_sql('ShipMode', dwh_db_engine, if_exists='append', index=False)

    orders_df = orders_df.drop_duplicates(subset='Row ID', keep='last')
    orders_df['Order ID'] = pd.to_numeric(orders_df['Order ID'], errors='coerce').astype('Int64')
    orders_df['Postal Code'] = orders_df['Postal Code'].astype(str)
    orders_df['Ship Mode'] = pd.to_numeric(orders_df['Ship Mode'], errors='coerce').astype('Int64')
    orders_df['Segment'] = pd.to_numeric(orders_df['Segment'], errors='coerce').astype('Int64')

    orders_df.rename(columns={
        'Row ID': 'RowID',
        'Order ID': 'OrderID',
        'Order Date': 'OrderDate',
        'Ship Date': 'ShipDate',
        'Ship Mode': 'ShipModeID',
        'Customer ID': 'CustomerID',
        'Segment': 'SegmentID',
        'Postal Code': 'PostalCode',
        'Product ID': 'ProductID',
        'Sales': 'Sales',
        'Quantity': 'Quantity',
        'Discount': 'Discount',
        'Profit': 'Profit'
    }, inplace=True)

    print(orders_df.info())
    orders_df.drop_duplicates(subset=['row_id', 'order_id'], inplace=True)
    orders_df.to_sql('Orders', dwh_db_engine, if_exists='append', index=False)

    people_df.to_sql('People', dwh_db_engine, if_exists='append', index=False)
    print(people_df.info())

    print(postal_codes_df.info())
    postal_codes_df['State'] = None
    postal_codes_df.rename(columns={'Postal Code': 'PostalCode'}, inplace=True)  
    postal_codes_df = postal_codes_df.drop_duplicates(subset=['PostalCode'])
    postal_codes_df.to_sql('PostalCodes', dwh_db_engine, if_exists='append', index=False)

    print(product_category_df.info())
    product_category_df = product_category_df.drop_duplicates(subset=['ID'])
    product_category_df.rename(columns={'ID': 'CatID', 'Name': 'Name'}, inplace=True)
    product_category_df.to_sql('ProductCategory', dwh_db_engine, if_exists='append', index=False)

    print(product_sub_category_df.info())
    product_sub_category_df = product_sub_category_df.drop_duplicates(subset=['ID'])
    product_sub_category_df.rename(columns={'ID': 'SubCatID', 'Name': 'Name'}, inplace=True)
    product_sub_category_df['SubCatID'] = product_sub_category_df['SubCatID'].astype(str)
    product_sub_category_df.to_sql('ProductSubCategory', dwh_db_engine, if_exists='append', index=False)

    print(segment_df.info())
    segment_df = segment_df.drop_duplicates(subset=['Segment ID'])
    segment_df.rename(columns={'Segment ID': 'SegmentID', 'Segment': 'Segment'}, inplace=True)
    segment_df['SegmentID'] = segment_df['SegmentID'].astype(int)
    segment_df.to_sql('Segment', dwh_db_engine, if_exists='append', index=False)

    print(postal_codes_df.info())

    unique_states = postal_codes_df[['State', 'Country']].drop_duplicates().reset_index(drop=True)
    unique_states['StateID'] = unique_states.index + 1


    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
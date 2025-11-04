import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_fact_orders():
    staging = PostgresHook(postgres_conn_id='postgres')
    warehouse = PostgresHook(postgres_conn_id='postgres')

    # Đọc dữ liệu từ staging
    df_orders = staging.get_pandas_df("SELECT * FROM staging.stg_orders")
    df_order_items = staging.get_pandas_df("SELECT * FROM staging.stg_order_items")
    df_order_payments = staging.get_pandas_df("SELECT * FROM staging.stg_order_payments")
    df_customers = staging.get_pandas_df("SELECT customer_id, customer_zip_code_prefix FROM staging.stg_customers")

    # Kết hợp
    df = pd.merge(df_orders, df_order_items, on='order_id', how='left')
    df = pd.merge(df, df_order_payments, on='order_id', how='left')
    df = pd.merge(df, df_customers, on='customer_id', how='left')

    # Làm sạch và chuẩn hóa dữ liệu
    df['order_status'] = df['order_status'].str.lower()
    date_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    for col in ['price', 'freight_value', 'payment_value']:
        df[col] = df[col].fillna(0)

    # Metrics
    df['total_amount'] = df['price'] + df['freight_value']
    df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400
    df['estimated_delivery_time'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds() / 86400

    # Tạo các foreign keys
    df['customer_key'] = df['customer_id']
    df['product_key'] = df['product_id']
    df['seller_key'] = df['seller_id']

    # Kiểm tra xem cột customer_zip_code_prefix có tồn tại không
    if 'customer_zip_code_prefix' in df.columns:
        df['geolocation_key'] = df['customer_zip_code_prefix']
    else:
        print("Cột customer_zip_code_prefix không tồn tại. Sử dụng giá trị mặc định.")
        df['geolocation_key'] = 'unknown'
    
    df['payment_key'] = df['payment_type'].astype('category').cat.codes + 1
    df['order_date_key'] = df['order_purchase_timestamp'].dt.date

    # Chọn cột fact
    fact_columns = [
        'order_id', 'customer_key', 'product_key', 'seller_key',
        'geolocation_key', 'payments_key', 'order_date_key',
        'order_status', 'price', 'freight_value', 'total_amount',
        'payment_value', 'delivery_time', 'estimated_delivery_time'
    ]

    # Kiểm tra thiếu cột
    missing_cols = [col for col in fact_columns if col not in df.columns]
    if missing_cols:
        print(f"Cảnh báo: thiếu các cột {missing_cols}")
        return

    df_fact = df[fact_columns]

    # Lưu dữ liệu vào warehouse
    engine = warehouse.get_sqlalchemy_engine()
    df_fact.to_sql(
        name='fact_orders',
        con=engine,
        schema='warehouse',
        if_exists='replace',
        index=False,
        chunksize=5000
    )

    print("Hoàn thành transform và lưu dữ liệu vào warehouse.fact_orders")
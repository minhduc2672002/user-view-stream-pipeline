import psycopg2
from pyspark.sql import DataFrame

class PostgresOperate:
    def __init__(self, postgres_conf):
        self.postgres_conf = postgres_conf
    def get_connection(self):
        try:
            connection = psycopg2.connect(
                dbname=self.postgres_conf['dbname'],
                user=self.postgres_conf['user'],
                password=self.postgres_conf['password'],
                host=self.postgres_conf['host'],
                port=self.postgres_conf['port']
            )
            print("Connection to PostgreSQL established.")
            return connection
        except Exception as e:
            print("Error connecting to PostgreSQL:", e)
            return None

    def create_table(self, create_table_query):
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
            print("Table created successfully.")
        except Exception as e:
            print("Error creating table:", e)
        finally:
            cursor.close()
            conn.close()

    def save_to_postgres(self, df: DataFrame, dbtable: str):
        df.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://{self.postgres_conf['host']}:{self.postgres_conf['port']}/{self.postgres_conf['dbname']}") \
            .option("dbtable", dbtable) \
            .option("user", self.postgres_conf['user']) \
            .option("password", self.postgres_conf['password']) \
            .mode("append") \
            .save()

    def upsert_to_table(self, partition, insert_query, columns,batch_size=100):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            batch = []

            for row in partition:
                try:
                    values = [getattr(row, col) for col in columns]
                    batch.append(values)

                    if len(batch) >= batch_size:
                        cursor.executemany(insert_query, batch)
                        conn.commit()
                        batch.clear()  # Xóa batch sau khi insert
                    # cursor.execute(insert_query, values)
                except Exception as e:
                    print(f"Error inserting row {row}: {e}")
            if batch:
                cursor.executemany(insert_query, batch)
                conn.commit()
        except Exception as e:
            print(f"Error in partition: {e}")
        finally:
            if 'conn' in locals() and conn:
                cursor.close()
                conn.close()

    def upsert_to_dim_browser(self, df_browser: DataFrame):
        columns = ["browser_key", "browser_name"]
        column_names = ",".join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO dim_browser ({column_names}) VALUES ({placeholders}) ON CONFLICT (browser_key) DO NOTHING"
        df_browser.foreachPartition(lambda partition: self.upsert_to_table(partition, insert_query, columns))

    def upsert_to_dim_os(self, df_os: DataFrame):
        columns = ["os_key", "os_name"]
        column_names = ",".join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO dim_operating_system ({column_names}) VALUES ({placeholders}) ON CONFLICT (os_key) DO NOTHING"
        df_os.foreachPartition(lambda partition: self.upsert_to_table(partition, insert_query, columns))

    def upsert_to_dim_reference(self, df_reference: DataFrame):
        columns = ["reference_key", "reference_domain", "is_self_reference"]
        column_names = ",".join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO dim_reference_domain ({column_names}) VALUES ({placeholders}) ON CONFLICT (reference_key) DO NOTHING"
        df_reference.foreachPartition(lambda partition: self.upsert_to_table(partition, insert_query, columns))

    def upsert_to_fact_view(self, df_fact_view: DataFrame):
        columns = ["key", "date_key", "location_key", "product_key", "store_id", "reference_key", "browser_key", "os_key", "total_view"]
        column_names = ",".join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"""INSERT INTO fact_view ({column_names}) 
                            VALUES ({placeholders}) 
                            ON CONFLICT (key) 
                            DO UPDATE SET total_view = fact_view.total_view + EXCLUDED.total_view;"""
        df_fact_view.foreachPartition(lambda partition: self.upsert_to_table(partition, insert_query, columns))

    def create_db(self):
        create_table_query = """
        DROP TABLE IF EXISTS Dim_Date;
        CREATE TABLE Dim_Date (
            datetime_key INT PRIMARY KEY,
            full_date DATE,
            day_of_week VARCHAR(10),
            day_of_week_short VARCHAR(10),
            hour INT,
            day_of_month INT,
            month INT,
            year INT
        );

        DROP TABLE IF EXISTS Dim_Product;
        CREATE TABLE Dim_Product (
            product_key INT PRIMARY KEY,
            product_name VARCHAR(50)
        );

        DROP TABLE IF EXISTS Dim_Location;
        CREATE TABLE Dim_Location (
            location_key INT PRIMARY KEY,
            country_iso2 VARCHAR(20),
            country_iso3 VARCHAR(20),
            country_name VARCHAR(50),
            country_nicename VARCHAR(50),
            country_numcode INT,
            country_phonecode INT
        );

        DROP TABLE IF EXISTS Dim_Reference_Domain;
        CREATE TABLE Dim_Reference_Domain (
            reference_key INT PRIMARY KEY,
            reference_domain VARCHAR(255),
            is_self_reference BOOLEAN
        );

        DROP TABLE IF EXISTS Dim_Operating_System;
        CREATE TABLE Dim_Operating_System (
            os_key INT PRIMARY KEY,
            os_name VARCHAR(50)
        );

        DROP TABLE IF EXISTS Dim_Browser;
        CREATE TABLE Dim_Browser (
            browser_key INT PRIMARY KEY,
            browser_name VARCHAR(50)
        );

        DROP TABLE IF EXISTS Fact_View;
        CREATE TABLE Fact_View (
            key VARCHAR(255) PRIMARY KEY, 
            product_key INT NOT NULL,
            location_key INT NOT NULL,
            date_key INT NOT NULL,
            reference_key INT NOT NULL,
            os_key INT NOT NULL,
            browser_key INT NOT NULL,
            store_id INT,
            total_view INT
        );
        """
        self.create_table(create_table_query)
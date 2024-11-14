import snowflake.connector

class SnowflakeLoader:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user='YOUR_USERNAME',
            password='YOUR_PASSWORD',
            account='YOUR_ACCOUNT'
        )

    def load_data(self, data):
        cursor = self.conn.cursor()
        # Insert data into Snowflake table
        cursor.execute("INSERT INTO your_table (columns) VALUES (values)")
        self.conn.commit()

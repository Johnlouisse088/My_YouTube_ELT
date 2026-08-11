from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"


def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()  # example output: <connection object at 0x...>
    cur = conn.cursor(cursor_factory=RealDictCursor) # example output: <cursor object at 0x...>
    return conn, cur


# # This cleans up the resources you opened earlier
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):  # creates a PostgreSQL schema
    conn, cur = get_conn_cursor()  # This connects to PostgreSQL. example output: <connection object at 0x...> <cursor object at 0x...>
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)  # Create a PostgreSQL schema named staging, but only if it doesn't already exist.  Take note That query only creates the schema, not a table.
    conn.commit()  # save
    close_conn_cursor(conn, cur)  # This cleans up the resources you opened earlier


def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                );
            """

        """
        It will looks like this:
        elt_db
        └── staging
            └── yt_api
                ├── Video_ID
                ├── Video_Title
                ├── Upload_Date
                ├── Duration
                ├── Video_Views
                ├── Likes_Count
                └── Comments_Count
        """
    else:
        table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """

    cur.execute(table_sql) # executes the CREATE TABLE SQL.
    conn.commit() # save
    close_conn_cursor(conn, cur) # closes the cursor and database connection.

# This function simply gets all existing video IDs from a database table and returns them as a Python list.
def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()  # fetchall() retrieves all rows returned by the SELECT query.
    video_ids = [row["Video_ID"] for row in ids]
    return video_ids  # example output: table_ids = ["abc123", "xyz456", "qwe789"]

import requests
from infi.clickhouse_orm import Database,Model
from clickhouse_driver import Client
from config import pipeline_logger
#### Clickhouse basic functions




import re


def extract_table_and_column_names(schema: str):
    table_match = re.search(r'(\w+)\s*\(', schema)
    table_name = table_match.group(1) if table_match else None
    schema = re.sub(r'\s+', ' ', schema)  # Normalize whitespace
    nested_pattern = re.compile(r'(\w+)\s+Nested\s*\((.*?)\)', re.DOTALL)
    normal_pattern = re.compile(r'(\w+)\s+(\w+)')

    columns = []

    # Extract nested columns
    for match in nested_pattern.finditer(schema):
        parent_col = match.group(1)
        nested_body = match.group(2)
        nested_fields = re.findall(r'(\w+)\s+\w+', nested_body)
        for field in nested_fields:
            columns.append(f"{parent_col}.{field}")

    # Remove nested definitions from schema
    schema_cleaned = nested_pattern.sub('', schema)

    # Extract normal columns
    for match in normal_pattern.finditer(schema_cleaned):
        col_name = match.group(1)
        col_type = match.group(2)
        if col_name.lower() not in ('engine', 'order', 'by'):  # skip SQL keywords
            columns.append(col_name)
    
    return  table_name, columns


    



def Create_clickhouse_table(
                            database:str,
                            host:str,
                            port:int,
                            password:str,
                            table_schema:str,
                            user="default",
                            
                             ) -> bool:
    
    """This function takes a table schema defined as a child class of the model class.
    Inputs : 
    Databse_name: the name of the database u wish to create the table in.
    db_url: the url of the clickhouse instance 
    table: the class that defines the schema of the table 
     """
    try:
        client = Client(
            host=host,
            port=port,       
            user=user,
            password=password,
            database=database
        )   
      
        client.execute(f"""CREATE TABLE IF NOT EXISTS {table_schema}
                        ENGINE =MergeTree()
                        ORDER BY timestamp;""")
        


        return client , True
    except Exception as e:
        pipeline_logger.error(f"There have been a problem Creating the table in clickhouse : {e}")
        raise Exception(f"there have been a problem creating the database, check Logs!")




def write_data_to_clickhouse(host:str,port:int,password:str,
                            database:str,rows:list,user:str,
                                table_schema:str
                             ):
    

    """
    Inserts a single row of data into a specified ClickHouse table.

    Parameters:
        host (str): The hostname or IP address of the ClickHouse server.
        port (int): The port number to connect to ClickHouse (e.g., 9000).
        password (str): The password for authenticating the ClickHouse user.
        database (str): The name of the ClickHouse database.
        row (dict): A dictionary representing the data to insert, where keys are column names and values are the corresponding data.
        user (str): The ClickHouse username.
        table (Model): The table schema, defined as a subclass of infi.clickhouse_orm.Model.

    Returns:
        None

    Raises:
        Logs an error message if the data cannot be written to the database.
    """
    for row in rows:
        try:
            client = Client(
            host=host,
            port=port,       
            user=user,
            password=password,
            database=database
        )
            table_name, columns = extract_table_and_column_names(table_schema)
            ### this logic deals witht th fact that clickhouse doesn't support having two nested columns with diffrent array lengths
            event_schema = r"event*"
            for col in columns:
                if re.match(event_schema, col):
                    if col not in row.keys():
                        pipeline_logger.info("Ha7na f col mafihom walo")
                        row[col] = ['']
                    else:
                        pipeline_logger.info(f"Before prcoessing : {row[col]}")
                        row[col] = [row[col]] if isinstance(row[col], str) else row[col]
                        pipeline_logger.info(f"after processing : {row[col]}")
                    
                    
            query = f'INSERT INTO {database}.{table_name}  VALUES'
            client.execute(
                    query,
                    [row]
            ) 

            pipeline_logger.info(f"pushed it successfully to the clickhouse")
        except Exception as e:
            pipeline_logger.error(f"{row} can't be written to the database : {e}")
        
        

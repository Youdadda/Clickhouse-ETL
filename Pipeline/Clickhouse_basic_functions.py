import requests
from infi.clickhouse_orm import Database,Model
from clickhouse_driver import Client
from config import pipeline_logger

#### Clickhouse basic functions




def Create_clickhouse_table(
                            database_name:str,
                            db_url:str,
                            table:Model,
                            password:str,
                             user_name="default",
                             ) -> bool:
    
    """This function takes a table schema defined as a child class of the model class.
    Inputs : 
    Databse_name: the name of the database u wish to create the table in.
    db_url: the url of the clickhouse instance; if it's locally i could be http://localhost:9000 
    table: the class that defines the schema of the table 
     """
    try:
        db = Database(
        database_name,
        db_url=db_url,
        username=user_name,
        password=password
    )
        db.create_table(table)
        return True
    except Exception as e:
        pipeline_logger.error(f"There have been a problem Creating the table in clickhouse : {e}")
        raise Exception(f"there have been a problem creating the database, check Logs!")




def write_data_to_clickhouse(host:str,port:int,password:str,
                                database:str,rows:list,user:str,
                                table:Model
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
            schema = []
            for name, _ in table._fields.items():
                schema.append(name) 
            table_name = table.__name__.lower()
            schema = [name for name, _ in table._fields.items()]
            row_values = [row.get(name, "") for name in schema]
            columns = ", ".join(schema)
            query = f'INSERT INTO {database}.{table_name} ({columns}) VALUES'
            client.execute(
                    query,
                    [tuple(row_values)]
            ) 
            pipeline_logger.info(f"pushed it successfully to the clickhouse")
        except Exception as e:
            pipeline_logger.error(f"{row} can't be written to the database : {e}")
        
        

from utils.postgres_connection import PostgresDB
from utils import config as cfg
from datetime import date

from typing import Union
from fastapi import FastAPI

app = FastAPI()

# Get current date in YYYY-MM-DD format
today = date.today().strftime("%Y-%m-%d") 
postgres_conn = cfg.postgres['DWH']

def connect_postgresdb():   
    """
    Return a connection to postgresdb
    Args: None
    Returns: postgresdb
    """      
    postgresdb = PostgresDB(    dbname = postgres_conn['dbname'], 
                                host = postgres_conn['host'], 
                                port = postgres_conn['port'], 
                                user = postgres_conn['username'], 
                                password = postgres_conn['password']
                    )
    postgresdb.initialize_pool()
    
    return postgresdb

@app.get("/employer/{employer_id}")
def read_an_employer(employer_id: str, q: Union[str, None] = None):
    
    postgresdb = connect_postgresdb()
    
    columns = 'employer_name, employer_url, location'
    print(employer_id)
    condition = { 'employer_id': employer_id}
    employer = postgresdb.select_one('stg.cv_employer_detail', columns, condition)
    
    if employer:
        return {
                'employer_name': employer[0] ,
                'employer_url': employer[1],
                'location': employer[2]
                }
    return None
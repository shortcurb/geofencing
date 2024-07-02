import mariadb
import os
import re
from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd
from typing import Dict, List, Any, Union, Optional
from dotenv import load_dotenv

class Database:
    """
    A class used to represent a Database connection to MariaDB.

    Methods
    -------
    connect(database_name: str) -> None
        Connects to the specified database.
    close() -> None
        Closes the current database connection.
    get_current_database() -> Optional[str]
        Returns the name of the current database.
    __enter__() -> 'Database'
        Enters the runtime context related to this object.
    __exit__(exc_type, exc_val, exc_tb) -> None
        Exits the runtime context related to this object.
    execute_query(query: str, params: Optional[tuple] = None, database: str = 'work', autoconnect: bool = False) -> Union[None, List[Any]]
        Executes a given query on the database.
    """

    def __init__(self):
        """
        Initializes the Database class and connects to the default database.
        """
        load_dotenv()
        self.credentials = {
            'password': os.getenv('dbpassword'),
            'host': os.getenv('dbhost'),
            'port': int(os.getenv('dbport')),
            'user': os.getenv('dbuser'),
        }
        self.connect('work')

    def connect(self, database_name: str) -> None:
        """
        Connects to the specified database using the stored credentials.

        Parameters:
        database_name (str): The name of the database to connect to.
        """
        self.credentials.update({
            'database': database_name
        })
        try:
            self.connection = mariadb.connect(**self.credentials)
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB: {e}")

    def close(self) -> None:
        """
        Closes the current database connection.
        """
        try:
            self.connection.close()
        except mariadb.ProgrammingError:
            print('Connection already closed')

    def get_current_database(self) -> Optional[str]:
        """
        Returns the name of the current database.

        Returns:
        Optional[str]: The name of the current database or None if an error occurs.
        """
        try:
            cursor = self.connection.cursor(dictionary = True)
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()['DATABASE()']
            cursor.close()
            return current_db
        except mariadb.ProgrammingError:
            print("Mariadb error when trying to determine what current database connection is to")
            return None

    def __enter__(self) -> 'Database':
        """
        Enters the runtime context related to this object.

        Returns:
        Database: The Database object itself.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exits the runtime context related to this object.

        Parameters:
        exc_type: The exception type.
        exc_val: The exception value.
        exc_tb: The traceback object.
        """
        self.close()

    def execute_query(self, query: str, params: Optional[tuple] = None, database: str = Optional[None], autoconnect: bool = False) -> Union[None, List[Any]]:
        """
        Executes a given query on the database.

        Parameters:
        query (str): The SQL query to execute.
        params (Optional[tuple]): Parameters for the SQL query.
        database (str): The database to connect to for this query.
        autoconnect (bool): If true, automatically manage the connection.

        Returns:
        Union[None, List[Any]]: The result of the query or None if an error occurs.
        """
        if autoconnect:
            # If autoconnect is true but no database is specified, assume the "default" database of work
            if database == None:
                database = 'work'
            # If the specified database is different than the currently connected one, 
            if database != self.get_current_database():
                self.close()
                self.connect(database)

        cursor = self.connection.cursor(dictionary = True)
        try:
            cursor.execute(query, params)
            if cursor.description:
                result = cursor.fetchall()
            else:
                result = None
            self.connection.commit()
        except mariadb.Error as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
            result = None
        finally:
            cursor.close()
            if autoconnect:
                self.close()
        return result
    
    def write_many(self, query: str, params: tuple = None, database: str = Optional[None], autoconnect: bool = False):
        if autoconnect:
            # If autoconnect is true but no database is specified, assume the "default" database of work
            if database == None:
                database = 'work'
            # If the specified database is different than the currently connected one, 
            if database != self.get_current_database():
                self.close()
                self.connect(database)

        cursor = self.connection.cursor(dictionary = True)
        try:
            cursor.executemany(query,params)
            self.connection.commit()
        except mariadb.Error as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()
        finally:
            cursor.close()
            if autoconnect:
                self.close()

class GeoDB:
    def __init__(self,database):

        load_dotenv()
        self.credentials = {
            'password': os.getenv('dbpassword'),
            'host': os.getenv('dbhost'),
            'port': int(os.getenv('dbport')),
            'user': os.getenv('dbuser'),
        }
        connection_string = connection_string = f"mysql+pymysql://{self.credentials['user']}:{self.credentials['password']}@{self.credentials['host']}:{self.credentials['port']}/{database}"
        self.engine = create_engine(connection_string)

    def insert_gdf(self,groupname,polygon):
        with self.engine.connect() as conn:
            # Iterate through the GeoDataFrame rows
            for index, row in polygon.iterrows():
                conn.execute("""
                INSERT INTO geofencing.fences (groupname,name, geom)
                VALUES (%s, ST_GeomFromText(%s));
                """, (groupname,row['name'], row['geom_wkt']))


    def select_gdf(self, query: str, data: Optional[dict] = None) -> gpd.GeoDataFrame:
        # I will probably never remember to select the geom as ST_AsText, so just let this function alter the query for me
        pattern = r', ?geom'
        matches = re.findall(pattern,query)
        if matches != []:
            for match in matches:
                query = query.replace(match, ", ST_AsText(geom)")
        if data:
            df = pd.read_sql(query, self.engine, params=data)
        else:
            df = pd.read_sql(query, self.engine)

        df['geometry'] = df['geom'].apply(lambda x: gpd.GeoSeries.from_wkt([x])[0])
        
        gdf_result = gpd.GeoDataFrame(df, geometry='geometry')
        
        return gdf_result





if __name__ == '__main__':
    '''
    db = Database()
#    db.close()
    print(db.execute_query('SELECT name FROM markets LIMIT 1', autoconnect=False))

    # Using a one-off connection
    a = Database().execute_query('SELECT * FROM zohoreliabilitymismatch',database = 'reliability',autoconnect = True)
    print(a)
    Database().close()

    # Using a persistent connection
    db = Database()
    db.connect('functiondb')
    b = db.execute_query('SELECT * FROM functionstate')
    print(b)
    '''
    query = "SELECT name,ST_AsText(geom) FROM fences"
    query = "SELECT name,geom as geom FROM fences"
    a = GeoDB('geofencing').select_gdf(query)
    #print(a)

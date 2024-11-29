import os
from pathlib import Path

import pyodbc
import tqdm
from dotenv import load_dotenv
from pandas import DataFrame

from utils.api_requets import User


class DatabaseConnection:
    def __init__(self, database_name: str, recreate_database: bool):
        self.connection_string: str
        self.recreate_database = recreate_database
        self.database_name = database_name

        self._set_connection_string()

    def __enter__(self):
        self.db_connection = pyodbc.connect(self.connection_string)

        self._init_database()
        self._init_tables()

        return self

    def __exit__(self, *_):
        self.db_connection.close()

    def _init_database(self):
        if self.recreate_database:
            self.execute(f"DROP DATABASE IF EXISTS {self.database_name};")

        self.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name};")
        self.execute(f"USE {self.database_name};")

    def _init_tables(self):
        self.execute("""
                     CREATE TABLE IF NOT EXISTS Users (
                     UserID SMALLINT PRIMARY KEY,
                     Name VARCHAR(255) NOT NULL,
                     Gender CHAR(1) NOT NULL,
                     Birthdate DATE NOT NULL,
                     Country VARCHAR(255) NOT NULL
                     );
                     """)

        self.execute("""
                     CREATE TABLE IF NOT EXISTS Transactions (
                     TransactionID INT PRIMARY KEY,
                     Date DATETIME NOT NULL,
                     UserID SMALLINT NOT NULL,
                     MerchantID MEDIUMINT NOT NULL,
                     Amount DECIMAL(10,2) NOT NULL,
                     FOREIGN KEY (UserID) REFERENCES Users(UserID)
                     );
                     """)

    def _set_connection_string(self):
        load_dotenv(Path(__file__).parent.parent.joinpath(".env"))

        sql_driver = "DRIVER={MySQL ODBC 8.0 ANSI Driver}"
        sql_server = "SERVER=localhost"
        sql_username = f"UID={os.getenv("SQL_USERNAME")}"
        sql_password = f"PWD={os.getenv("SQL_PASSWORD")}"

        arguments = [sql_driver, sql_server, sql_username, sql_password]

        self.connection_string = ";".join(arguments)

    def execute(self, statement):
        with self.db_connection.cursor() as cursor:
            cursor.execute(statement)

    # TODO: replace with df.to_sql
    def write_transactions(self, transaction_df: DataFrame):
        n_iter = transaction_df.shape[0]
        print("Writing transactions:")
        with self.db_connection.cursor() as cursor:
            for _, row in tqdm.tqdm(transaction_df.iterrows(), total=n_iter):
                cursor.execute(
                    "INSERT INTO Transactions (TransactionID,Date,UserID,MerchantID,Amount) values(?,?,?,?,?)",
                    row.id,
                    row.date,
                    row.client_id,
                    row.merchant_id,
                    row.amount,
                )

            cursor.commit()

    def write_users(self, users: list[User]):
        print("Writing users:")
        with self.db_connection.cursor() as cursor:
            for user in tqdm.tqdm(users):
                cursor.execute(
                    "INSERT INTO Users (UserID,Name,Gender,Birthdate,Country) values(?,?,?,?,?)",
                    user.id,
                    user.name,
                    user.gender,
                    user.birthdate,
                    user.country,
                )

            cursor.commit()


import csv, random
from connection import BaseConn, SnowflakeConn

def connect(cred: dict):
  return SnowflakeConn(cred)

def create_database(conn: BaseConn, name: str):
  "Create database in connection"
  conn.execute(f'create or replace database {name}')

def create_schema(conn: BaseConn, name: str):
  "Create Schema in connection"
  conn.execute(f'create or replace schema {name}')

def drop_database(conn: BaseConn, name: str):
  "Drop database with all schema/tables in it"
  conn.execute(f'drop database {name} cascade')


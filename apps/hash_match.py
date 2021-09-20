'''
PROCESS
- connect to snowflake
  - provide connection details from frontend? secrets file?
- input 2 database names
- click 'Setup', this will create the databases with a demo schema and an emails table with random emails (with some overlap)
  - select from both tables and attempt to join them using the encrypted key
- statistics: # emails left, #emails right, % overlap
- Button to tear down databases, drop cascade, reset state
'''

import streamlit as st
import pandas as pd
import csv, random, sys, os

sys.path.insert(0, os.path.dirname(__file__))

from helpers import helpers as h
from helpers.connection import BaseConn

def_schema = 'demo_schema'
def_database_prefix = 'TEST_DB_'

@st.cache(ttl=600, allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def get_connection():
	return h.connect(st.secrets["snowflake"])

def init_state():
  st.session_state.dbs_created = st.session_state.get('dbs_created', False)
  st.session_state.database1 = st.session_state.get('database1', def_database_prefix+'1')
  st.session_state.database2 = st.session_state.get('database2', def_database_prefix+'2')
  st.session_state.query_text = st.session_state.get('query_text', '')

def create_email_table(conn: BaseConn, name: str):
  "Create email table with ~100 random records"
  with open(os.path.join(os.path.dirname(__file__), 'dummy_records.csv')) as file:
    reader = csv.reader(file)
    rows = list(reader)[1:]
    emails = random.choices([row[0] for row in rows], k=100)
    emails = list(set(emails)) # remove dupes
    
  conn.execute(f'create or replace table {name} (email varchar, email_sha varchar)')

  values = [f"('{email}')" for email in emails[1:]]
  conn.execute(f'insert into {name} (email) values {", ".join(values)}')
  conn.execute(f'update {name} set email_sha = sha2(email)')

def setup(conn: BaseConn, *databases):
  "Sets up demo databases/schemas with a emails table"
  schema = def_schema
  for database in databases:
    h.create_database(conn, database)
    h.create_schema(conn, f'{database}.{schema}')
    create_email_table(conn, f'{database}.{schema}.emails')

def tear_down(conn: BaseConn, *databases):
  "Drop demo databases"
  for database in databases:
    h.drop_database(conn, database)


def query_email_sql(database1, database2):
  schema = def_schema
  table1 = f'{database1}.{schema}.emails'
  table2 = f'{database2}.{schema}.emails'
  sql = f'''
  select
    t1.email_sha email_sha_1,
    t2.email_sha email_sha_2
  from {table1} t1
  inner join {table2} t2
    on t1.email_sha = t2.email_sha
  '''
  return sql

def main():
  st.write("""
  This app demonstates the ability to join two different tables from different databases on hashed values. In this example, tables with hashed email values will attempt to join on limited matching data. Click **Setup** on the left sidebar to setup demo databases.
  """)
  conn = get_connection()
  status = st.empty()

  with st.sidebar.form("Database Names"):
      do_teardown = do_setup = False
      st.markdown("### Database Names")
      database1 = st.text_input('Database 1 Name', st.session_state.database1)
      database2 = st.text_input('Database 2 Name', st.session_state.database2)
      do_setup = st.form_submit_button('Setup' ,'setup demo databases')
      do_teardown = st.form_submit_button('Teardown', "destroy demo databases")

  if do_setup:
    status.text('Creating databases with email data')
    if database1 and database2:
      setup(conn, database1, database2)
      status.text('')
      st.session_state.dbs_created = True
      st.session_state.database1 = database1
      st.session_state.database2 = database2
      st.session_state.query_text = ''
    else:
      status.write('*Invalid database names*')

  if do_teardown:
    status.text('Destroying databases..')
    tear_down(conn, database1, database2)
    status.text('')
    st.session_state.dbs_created = False
    st.session_state.database1 = def_database_prefix+'1'
    st.session_state.database2 = def_database_prefix+'2'
    st.session_state.query_text = ''
  
  columns = [] 
  rows = []
  df = pd.DataFrame(rows, columns=columns)
  if st.button("Refresh") or do_setup:
    if st.session_state.query_text == '':
      st.write("These are matching emails from two different email tables")
      st.session_state.query_text = query_email_sql(st.session_state.database1, st.session_state.database2)
    sql = st.text_area("", st.session_state.query_text.strip(), height=200)
    df = conn.query(sql.strip(), dtype="dataframe")
  
  if do_teardown:
    df = pd.DataFrame(rows, columns=columns)
  
  st.dataframe(df, height=720)

def main_old():
  do_teardown = do_setup = False
  conn = get_connection()
  
  status = st.empty()
  col1, col2 = st.columns(2)

  with col1:
    form = st.form(key='input-form')
    if st.session_state.dbs_created:
      form.write("""## Existing Database Names""")
      database1 = form.text_input('Database 1 Name')
      database2 = form.text_input('Database 2 Name')
      do_teardown = form.form_submit_button('Teardown', "destroy demo databases")
    else:
      form.write("""## Input Database Names""")
      database1 = form.text_input('Database 1 Name')
      database2 = form.text_input('Database 2 Name')
      do_setup = form.form_submit_button('Setup' ,'setup demo databases')


  if do_setup:
    status.text('Creating databases with email data')
    if database1 and database2:
      setup(conn, database1, database2)
      status.text('')
      st.session_state.dbs_created = True
      st.session_state.database1 = database1
      st.session_state.database2 = database2
    else:
      status.write('*Invalid database names*')

  if do_teardown:
    status.text('Destroying databases..')
    tear_down(conn, database1, database2)
    status.text('')
    st.session_state.dbs_created = False
    st.session_state.database1 = ''
    st.session_state.database2 = ''
  
  with col2:
    columns = [] 
    rows = []
    df = pd.DataFrame(rows, columns=columns)
    if st.button("Refresh") or do_setup:
      st.write("These are matching emails from two different email tables")
      df = query_email_data(conn, st.session_state.database1, st.session_state.database2)
    if do_teardown:
      df = pd.DataFrame(rows, columns=columns)
    st.dataframe(df, height=720)

st.title("Snowflake hash match Demo")
init_state()
main()  
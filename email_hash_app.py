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
import helpers as h
import csv, random
from connection import BaseConn

def_schema = 'demo_schema'

@st.cache(ttl=600, allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def get_connection():
	return h.connect(st.secrets["snowflake"])

def init_state():
  st.session_state.dbs_created = st.session_state.get('dbs_created', False)

def create_email_table(conn: BaseConn, name: str):
  "Create email table with ~100 random records"
  with open('dummy_records.csv') as file:
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


def query_email_data(conn: BaseConn, database1, database2):
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
  return conn.query(sql, dtype='dataframe')

def main():
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
      st.session_state.db1 = database1
      st.session_state.db2 = database2
    else:
      status.write('*Invalid database names*')

  if do_teardown:
    status.text('Destroying databases..')
    tear_down(conn, database1, database2)
    status.text('')
    st.session_state.dbs_created = False
    st.session_state.db1 = ''
    st.session_state.db2 = ''
  
  with col2:
    columns = []
    rows = []
    df = pd.DataFrame(rows, columns=columns)
    if st.button("Refresh") or do_setup:
      st.write("These are matching emails from two different email tables")
      df = query_email_data(conn, st.session_state.db1, st.session_state.db2)
    if do_teardown:
      df = pd.DataFrame(rows, columns=columns)
    st.dataframe(df, height=720)

st.title("Snowflake Email hash Demo")
init_state()
main()
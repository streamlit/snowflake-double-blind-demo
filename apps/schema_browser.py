import streamlit as st
import pandas as pd
import snowflake.connector
import csv, random, sys, os

sys.path.insert(0, os.path.dirname(__file__))

from helpers import helpers as h
from helpers.connection import BaseConn


@st.cache(
    allow_output_mutation=True,
    hash_funcs={"_thread.RLock": lambda _: None},
)
def get_connector():
    """Returns the snowflake connector. Uses st.cache to only run once."""
    return snowflake.connector.connect(**st.secrets["snowflake"])


def_schema = "demo_schema"
state = st.session_state


@st.cache(
    ttl=600, allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None}
)
def get_connection():
    return h.connect(st.secrets["snowflake"])


def init_state():
    state.dbs_created = state.get("dbs_created", False)
    state.database = state.get("database", "SNOWFLAKE_SAMPLE_DATA")
    state.databases = state.get("databases", [])
    state.schema = state.get("schema", "")
    state.schemas = state.get("schemas", [])


def create_email_table(conn: BaseConn, name: str):
    "Create email table with ~100 random records"
    with open("dummy_records.csv") as file:
        reader = csv.reader(file)
        rows = list(reader)[1:]
        emails = random.choices([row[0] for row in rows], k=100)
        emails = list(set(emails))  # remove dupes

    conn.execute(f"create or replace table {name} (email varchar, email_sha varchar)")

    values = [f"('{email}')" for email in emails[1:]]
    conn.execute(f'insert into {name} (email) values {", ".join(values)}')
    conn.execute(f"update {name} set email_sha = sha2(email)")


def setup(conn: BaseConn, *databases):
    "Sets up demo databases/schemas with a emails table"
    schema = def_schema
    for database in databases:
        h.create_database(conn, database)
        h.create_schema(conn, f"{database}.{schema}")
        create_email_table(conn, f"{database}.{schema}.emails")


def tear_down(conn: BaseConn, *databases):
    "Drop demo databases"
    for database in databases:
        h.drop_database(conn, database)


def get_databases(conn: BaseConn):
    rows = conn.query("show databases")
    return [r.name for r in rows]


def get_schemas(conn: BaseConn):
    rows = conn.query(
        """
    select schema_name
    from information_schema.schemata
    order by schema_name
  """
    )
    return [r.schema_name for r in rows]


def get_tables_info(conn: BaseConn, schema: str):
    df = conn.query(
        f"""
    select *
    from information_schema.tables
    where table_schema = upper('{schema}')
    order by table_name
  """,
        dtype="dataframe",
    )
    return df


def main():
    # connect to snowflake
    try:
        conn = get_connection()
        st.sidebar.success("🎉 We have successfully loaded your Snowflake credentials")
    except Exception:
        snowflake_tutorial = (
            "https://docs.streamlit.io/en/latest/tutorial/snowflake.html"
        )
        st.sidebar.error(
            f"""
        Couldn't load your credentials.  
        Did you have a look at our 
        [tutorial on connecting to Snowflake]({snowflake_tutorial})?
        """
        )
        raise

    st.sidebar.write("---")

    refresh = st.sidebar.button("Refresh")

    # Show list of databases
    if not state.databases or refresh:
        with st.spinner(f"Collecting databases available in Snowflake..."):
            state.databases = get_databases(conn)
            state.schemas = []

    if state.database in state.databases:
        database_index = state.databases.index(state.database)
    else:
        database_index = 0
    database = st.sidebar.selectbox(
        "Choose a Database", state.databases, index=database_index
    )
    if database != state.database:
      state.schemas = []
      state.schema = ''
    state.database = database

    if not state.database:
        return

    # Show list of schemas
    if not state.schemas or refresh:
        with st.spinner(f"Getting schemas in '{state.database}'..."):
            state.schemas = get_schemas(conn)
            state.schema = ''

    if state.schema in state.schemas:
        schema_index = state.schemas.index(state.schema)
    else:
        schema_index = 0

    state.schema = st.sidebar.selectbox("Choose a Schema", state.schemas, index=schema_index)

    if not state.schema:
        return

    with st.spinner(f"Getting tables for schema {state.database}.{state.schema}..."):
      conn.execute(f"use {state.database}")
      df = get_tables_info(conn, state.schema)
      st.dataframe(df, height=720, width=1420)

st.title("Schema Browser")
init_state()
main()
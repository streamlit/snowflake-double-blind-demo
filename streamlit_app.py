from logging import exception
from numpy import number
import streamlit as st
from streamlit import caching
import pandas as pd
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pandas_tools import pd_writer, write_pandas
import yaml, csv, random, os
from typing import List, Tuple
import sqlalchemy as sa

from collections import OrderedDict, namedtuple

state = st.session_state


def _snowflake_cache(**cache_args):
    """Returns a specialized version of the st.cache decorator for Snowflake."""
    return st.cache(hash_funcs={"_thread.RLock": lambda _: None}, **cache_args)


@_snowflake_cache(show_spinner=False, allow_output_mutation=True)
def get_connector():
    """Returns the snowflake connector. Uses st.cache to only run once."""
    return snowflake.connector.connect(**st.secrets["snowflake"])


@_snowflake_cache(show_spinner=False, allow_output_mutation=True)
def get_engine(database):
    """Returns the snowflake connector engine. Uses st.cache to only run once."""
    cred = st.secrets["snowflake"]
    args = [
        "snowflake://",
        cred["user"],
        ":",
        cred["password"],
        "@",
        cred["account"],
        "/",
        database,
        "/",
        cred["schema"],
        "?warehouse=",
        cred["warehouse"],
        "&role=",
        cred.get("role", "ACCOUNTADMIN"),
    ]
    url = "".join(args)
    return sa.create_engine(url, echo=False)


# @st.cache(ttl=600, **SNOWFLAKE_CACHE_ARGS)
# @_snowflake_cache(ttl=600)
def run_query(query: str, as_df=False):
    """Perform query. Uses st.cache to only rerun when the query changes
    after 10 min."""
    conn = get_connector()
    with conn.cursor() as cur:
        try:
            cur.execute(query)
        except Exception as E:
            raise Exception(f"{E}\n\nError running SQL query:\n{query}")

        columns = [c[0].lower() for c in cur.description]
        Row = namedtuple(
            "Row", columns
        )  # namedtuples allow property and index reference
        rows = [Row(*row) for row in cur.fetchall()]
        if as_df:
            return pd.DataFrame(rows, columns=columns)
        return rows


def execute_query(query: str):
    """Executes non-SELECT query, without cache"""
    conn = get_connector()
    with conn.cursor() as cur:
        try:
            cur.execute(query)
        except Exception as E:
            raise Exception(f"{E}\n\nError running SQL query:\n{query}")


# @st.cache(ttl=600)
def get_databases() -> list:
    return [row.name for row in run_query("SHOW DATABASES")]


def get_table_names(database) -> List[str]:
    tables = run_query(f"SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES")
    return [f"{t.table_schema}.{t.table_name}" for t in tables]


def init_state():
    state.dbs_created = state.get("dbs_created", False)
    state.database = state.get("database", "STREAMLIT_DEMO_DB")
    state.databases = state.get("databases", [])
    state.schema = state.get("schema", "PUBLIC")
    state.schemas = state.get("schemas", [])
    state.table = state.get("table", "SAMPLE_CONTACTS")


def get_index(choices: list, value=None):
    if value in choices:
        return choices.index(value)
    return 0


####################### SYNTHETHIC DATA APP


@_snowflake_cache()
def load_names() -> Tuple[List[str], List[str]]:
    """Returns two lists (firstnames, lastnames) of example names."""
    with open("names.yaml") as name_file:
        return yaml.load(name_file)


def randomize_names(key: str):
    """Randomize either the list of firstnames or lastnames."""
    assert key in ("firstnames", "lastnames")
    names = load_names()
    random_names = random.sample(names[key], 10)
    setattr(state, key, random_names)


def sample_database_form():
    state.database = st.text_input("Sample Database Name", state.database)
    if state.database not in get_databases():
        st.error(f":warning: Missing database `{state.database}`. Create it?")
        if st.button(f"Create {state.database}"):
            st.warning(f"Creating `{state.database}...`")
            execute_query(f"CREATE DATABASE {state.database}")
            st.success(f"Created `{state.database}`")
            caching.clear_cache()
            st.success("The cache has been cleared.")
            st.button("Reload this page")
        return False

    st.success(f"Found {state.database}!")
    if st.button(f"Destroy {state.database}"):
        st.warning(f"Destroying `{state.database}...`")
        execute_query(f"DROP DATABASE {state.database} CASCADE")
        st.success(f"Destroyed `{state.database}`")
        caching.clear_cache()
        st.success("The cache has been cleared.")
        st.button("Reload this page")
        return False

    return True


def synthetic_data_app(conn: SnowflakeConnection):
    """Create some synthetic tables with which we can select data."""
    # # Let the user create a sample database

    if not sample_database_form():
        return  # if sample database did not exist or is destroyed

    st.write("## Create synthetic data")

    # Show select boxes for the first and last names.
    names = load_names()

    name_types = [("first name", "firstnames"), ("last name", "lastnames")]
    for name_type, key in name_types:
        if key not in state:
            randomize_names(key)
        st.multiselect(f"Select {name_type}s", names[key], key=key)
        st.button(f"Randomize {name_type}s", on_click=randomize_names, args=(key,))

    n_firstnames = len(getattr(state, "firstnames"))
    n_lastnames = len(getattr(state, "lastnames"))
    max_rows = n_firstnames * n_lastnames
    assert max_rows > 0, "Must have a least one first and last name."
    n_rows = st.slider("Number of rows", 1, max_rows, min(max_rows, 50))

    df = pd.DataFrame(
        random.sample(
            [
                {
                    "FIRSTNAME": firstname,
                    "LASTNAME": lastname,
                    "EMAIL": f"{firstname}.{lastname}@gmail.com",
                }
                for firstname in state.firstnames
                for lastname in state.lastnames
            ],
            n_rows,
        )
    )
    st.write(df)

    table_name = st.text_input("Table name", state.table)
    if not table_name:
        st.warning("Enter a table name above an press enter to create a new table.")
    else:
        state.table = (
            table_name if "." in table_name else f"PUBLIC.{table_name}"
        ).upper()
        if st.button(f'Create table "{state.table}"'):
            schema, table = state.table.split(".")
            execute_query(f"use database {state.database}")
            execute_query(f"create schema if not exists {schema}")
            execute_query(f"use schema {schema}")
            execute_query(f"drop table if exists {table}")
            st.warning(f"Creating `{table}` with len `{len(df)}`.")
            engine = get_engine(state.database)
            engine.execute(f"use database {state.database}")
            df.to_sql(
                table,
                engine,
                schema=schema,
                index=False,
                method=pd_writer,
            )
            st.success(f"Created table `{state.table}`!")

    # st.write("### firstnames", firstnames, "### lastnames", lastnames)


####################### INTRO APP
def intro_app(conn: SnowflakeConnection):
    st.sidebar.success("Select a mode above.")

    st.markdown(
        """
        Streamlit is an open-source app framework built specifically for
        Machine Learning and Data Science projects.

        **👈 Select a mode from the dropdown on the left** to see some examples
        of what Streamlit can do!
        ### Want to learn more?
        - Check out [streamlit.io](https://streamlit.io)
        - Jump into our [documentation](https://docs.streamlit.io)
        - Ask a question in our [community
          forums](https://discuss.streamlit.io)
        ### See more complex demos
        - Use a neural net to [analyze the Udacity Self-driving Car Image
          Dataset] (https://github.com/streamlit/demo-self-driving)
        - Explore a [New York City rideshare dataset]
          (https://github.com/streamlit/demo-uber-nyc-pickups)
    """
    )


####################### DOUBLE BLIND JOIN APP


def double_bind_join_app(conn: SnowflakeConnection):
    st.write(
        """
  This app demonstates the ability to join two different tables from different databases on hashed values. In this example, tables with hashed email values will attempt to join on limited matching data. Click **Setup** on the left sidebar to setup demo databases.
  """
    )

    st.markdown("### Table Names")

    tables = get_table_names(state.database)
    tables = [t for t in tables if not t.lower().startswith("information_schema")]
    t1_index = get_index(tables, state.table)
    table1 = st.sidebar.selectbox("Choose Table 1", tables, index=t1_index)
    t2_index = get_index(tables, state.table)
    table2 = st.sidebar.selectbox("Choose Table 2", tables, index=t2_index)

    df = pd.DataFrame([], columns=[])
    with st.spinner(f"Getting data..."):
        # TABLE1
        table1 = table1 if "." in table1 else f"{state.database}.public.{table1}"
        df1 = run_query(f"select sha2(email) as email_hash from {table1}", as_df=True)
        st.write(f"Got {len(df1)} records from Table 1: `{table1}`")

        # TABLE2
        table2 = table2 if "." in table2 else f"{state.database}.public.{table2}"
        df2 = run_query(f"select sha2(email) as email_hash from {table2}", as_df=True)
        st.write(f"Got {len(df2)} records from Table 2: `{table2}`")

    with st.spinner(f"Matching Hashes..."):
        df = df1.join(df2.set_index("email_hash"), on="email_hash", how="inner")
        st.write(f"Found **{len(df)} matching emails** from two provided tables!")

    st.dataframe(df, height=720)


def main():
    """Execution starts here."""
    # Get the snowflake connector. Display an error if anything went wrong.
    try:
        conn = get_connector()
    except:
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

    # # Show a browser for what functions they could run.
    modes = OrderedDict(
        [
            ("Intro", intro_app),
            ("Synthetic data generator", synthetic_data_app),
            ("Double-blind join", double_bind_join_app),
        ]
    )
    selected_mode_name = st.sidebar.selectbox(
        "Select mode", list(modes)  # type: ignore
    )
    selected_mode = modes[selected_mode_name]
    selected_mode(conn)


if __name__ == "__main__":
    init_state()
    main()

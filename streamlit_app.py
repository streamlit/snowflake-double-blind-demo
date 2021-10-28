import yaml, random, streamlit as st, pandas as pd, sqlalchemy as sa
import snowflake.connector

# from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pandas_tools import pd_writer
from typing import List, Tuple
from collections import namedtuple

# TODO: Just use st.session_state everywhere.
state = st.session_state


# TODO: Would be good to see if we could use st.experimental_memo and
# st.experimental_singleton if possible instead of st.cache.
# https://docs.streamlit.io/library/api-reference/performance
def _snowflake_cache(**cache_args):
    """A specialized version of the st.cache decorator for Snowflake."""
    return st.cache(hash_funcs={"_thread.RLock": lambda _: None}, **cache_args)


def _snowflake_singleton(**cache_args):
    """A specialized version of the st.cache decorator for singleton objects."""
    return _snowflake_cache(allow_output_mutation=True, ttl=600, **cache_args)


# TODO: Let's see if we can not use this. The goal is to minimize cognitive load.
@_snowflake_singleton()
def get_connector():
    """Returns the snowflake connector. Uses st.cache to only run once."""
    return snowflake.connector.connect(**st.secrets["snowflake"])

# TODO: Is there a way that we can get this down to either get_engine or get_connector,
# but not both. My guess is use this instead of get_connector. 
@_snowflake_singleton()
def get_engine(database):
    """Returns the snowflake connector engine. Uses st.cache to only run once."""
    cred = st.secrets["snowflake"]
    # TODO: This is a wordy way of creating this thing. Would be better with f-string
    # interpolation. 
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

# TODO: Can we figure out a way to merge these run_query() and execute_query()

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


def update_tables(database) -> List[str]:
    tables = run_query(f"SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES")
    return [
        t.table_name
        # f"{t.table_schema}.{t.table_name}"
        for t in tables
        if t.table_schema.lower() == "public"
    ]


# TODO: I'm going to work with to to slim down the session state.
# I *think* that only need state.databases. Therefore, I don't even think we need this
# method, but we can keep in maybe.
def init_state():
    # TODO: Not being used. Let's get rid of it. 
    state.dbs_created = state.get("dbs_created", False)

    # TODO: We don't need state.database, we can make it always STREAMLIT_DEMO_DB.
    state.database = state.get("database", "STREAMLIT_DEMO_DB")

    # TODO: I think it's fair for this to be session state. Set it every time a database
    # is created / if we nuke STREAMLIT_DEMO_DB
    state.databases = state.get("databases", [])

    # I don't think we need this because it can be hard-coded. 
    state.schema = state.get("schema", "PUBLIC")
    state.schemas = state.get("schemas", [])


# I don't think we need this either. I think there's a more elegant way to do this
# without this method and without  s
def get_index(choices: list, value=None):
    if value in choices:
        return choices.index(value)
    return 0


####################### SYNTHETHIC DATA APP


@_snowflake_cache()
def load_names() -> Tuple[List[str], List[str]]:
    """Returns two lists (firstnames, lastnames) of example names."""
    with open("names.yaml") as name_file:
        return yaml.safe_load(name_file)


def randomize_names(key: str):
    """Randomize either the list of firstnames or lastnames."""
    names = load_names()
    random_names = random.sample(names[key], 10)  # type: ignore
    setattr(state, key, random_names)


def sample_database_form():
    state.database = st.text_input("Sample Database Name", state.database)
    if state.database not in get_databases():
        st.error(f":warning: Missing database `{state.database}`. Create it?")
        if st.button(f"Create {state.database}"):
            st.warning(f"Creating `{state.database}...`")
            execute_query(f"CREATE DATABASE {state.database}")
            st.success(f"Created `{state.database}`")
            # caching.clear_cache()
            st.success("The cache has been cleared.")
            st.button("Reload this page")
        return False

    st.success(f"Found {state.database}!")
    if st.button(f"Destroy {state.database}"):
        st.warning(f"Destroying `{state.database}...`")
        execute_query(f"DROP DATABASE {state.database} CASCADE")
        st.success(f"Destroyed `{state.database}`")
        # caching.clear_cache()
        st.success("The cache has been cleared.")
        st.button("Reload this page")
        return False

    return True


def synthetic_data_page():
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

    raise RuntimeError("Todo: Fix this section.") 
    # table_names = update_tables(state.database)
    # st.write(
    # table_number = 1
    # default_table_name = f"SAMPLE_{table_number}"
    # while () in table_names:
    #     table_number += 1
    # st.write(f"default_table_name: `{default_table_name}`")
    return

    table_name = st.text_input("Table name", state_table)
    if not table_name:
        st.warning("Enter a table name above an press enter to create a new table.")
    else:
        state_table = f"PUBLIC.{table_name}".upper()
        if st.button(f'Create table "{state_table}"'):
            schema, table = state_table.split(".")
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
            st.success(f"Created table `{state_table}`!")

    # st.write("### firstnames", firstnames, "### lastnames", lastnames)


####################### INTRO APP
def intro_page():
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


def double_bind_join_page():
    st.markdown("### Table Names")

    tables = get_public_tables(state.database)
    if len(tables) < 2:
        st.warning(
            ":point_left: Must have a least two tables to compare."
            "**Please select *Synthetic table generator* to create data.**"
        )
        return
    st.write("raw tables", tables)
    tables = [t for t in tables if not t.lower().startswith("information_schema")]
    st.write("final tables", tables)
    # t1_index = get_index(tables, state_table)
    t
    table1 = st.sidebar.selectbox("Choose Table 1", tables)
    # t2_index = get_index(tables, state_table)
    table2 = st.sidebar.selectbox("Choose Table 2", tables)

    tables = st.multiselect("Select tables to compare", tables, default=tables[:2])

    st.write("state.database", state.database)
    st.write("tables", tables)
    df1 = run_query(f"select * from {state.database}.public.{table1}", as_df=True)
    st.write(df1)

    df = pd.DataFrame([], columns=[])
    with st.spinner(f"Getting data..."):
        # TABLE1
        table1 = f"{state.database}.public.{table1}"
        st.write("table1", table1)
        df1 = run_query(
            f"select (sha2(email || 'abc')) as email_hash from {table2}", as_df=True
        )
        st.write(f"Got {len(df1)} records from Table 1: `{table1}`")

        # TABLE2
        table2 = table2 if "." in table2 else f"{state.database}.public.{table2}"
        st.write("table2", table2)
        df2 = run_query(
            f"select sha2(email || 'abc') as email_hash from {table2}", as_df=True
        )
        st.write(f"Got {len(df2)} records from Table 2: `{table2}`")

    st.write("## df1", df1)
    st.write("## df2", df2)
    with st.spinner(f"Matching Hashes..."):
        df = df1.join(df2.set_index("email_hash"), on="email_hash", how="inner")  # type: ignore
        st.write(f"Found **{len(df)} matching emails** from two provided tables!")

    st.dataframe(df, height=720)


def main():
    """Execution starts here."""
    # Get the snowflake connector. Display an error if anything went wrong.
    try:
        get_connector()
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

    # Show a browser for what functions they could run.
    modes = {
        "Double-blind join": double_bind_join_page,
        "Intro": intro_page,
        "Synthetic data generator": synthetic_data_page,
    }
    selected_mode_name = st.sidebar.selectbox("Select mode", list(modes))  # type: ignore
    selected_mode = modes[selected_mode_name]
    selected_mode()


if __name__ == "__main__":
    init_state()
    main()

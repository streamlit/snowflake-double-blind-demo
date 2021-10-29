import yaml, random, streamlit as st, pandas as pd, sqlalchemy as sa
import snowflake.connector

# from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pandas_tools import pd_writer
from typing import List, Tuple
from collections import namedtuple


# TODO: Would be good to see if we could use st.experimental_memo and
# st.experimental_singleton if possible instead of st.cache.
# https://docs.streamlit.io/library/api-reference/performance
def _snowflake_cache(**cache_args):
    """A specialized version of the st.cache decorator for Snowflake."""
    return st.cache(hash_funcs={"_thread.RLock": lambda _: None}, **cache_args)


def _snowflake_singleton(**cache_args):
    """A specialized version of the st.cache decorator for singleton objects."""
    return _snowflake_cache(allow_output_mutation=True, ttl=600, **cache_args)


@_snowflake_singleton()
def get_engine():
    """Returns the snowflake connector engine. Uses st.cache to only run once."""
    url_template = 'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'
    return sa.create_engine(url_template.format(**st.secrets["snowflake"]), echo=False)

# @st.cache(ttl=600, **SNOWFLAKE_CACHE_ARGS)
# @_snowflake_cache(ttl=600)
def run_query(query: str, as_df=False):
    """Perform query. Uses st.cache to only rerun when the query changes
    after 10 min."""
    conn = get_engine()
    try:
        result = conn.execute(query)
    except Exception as E:
        raise Exception(f"{E}\n\nError running SQL query:\n{query}")
        
    columns = list(result.keys())
    Row = namedtuple(
        "Row", columns
    )  # namedtuples allow property and index reference
    rows = [Row(*row) for row in result.fetchall()]

    if as_df:
        return pd.DataFrame(rows, columns=columns)
    return rows

# TODO: We may eventually want to get rid of this.
# def update_tables(database) -> List[str]:
#     tables = run_query(f"SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES")
#     return [
#         t.table_name
#         # f"{t.table_schema}.{t.table_name}"
#         for t in tables
#         if t.table_schema.lower() == "public"
#     ]


####################### SYNTHETHIC DATA APP


@_snowflake_cache()
def load_names() -> Tuple[List[str], List[str]]:
    """Returns two lists (firstnames, lastnames) of example names."""
    with open("names.yaml") as name_file:
        return yaml.safe_load(name_file)

# not important TODO: give key a type of enum.Enum
# see: https://docs.python.org/3/library/enum.html
def randomize_names(key: str):
    """Randomize either the list of firstnames or lastnames."""
    names = load_names()
    random_names = random.sample(names[key], 10)  # type: ignore
    setattr(st.session_state, key, random_names)


# TODO step 3: We need to look at this together when the app's running again.
def sample_database_form():
    databases = [row.name for row in run_query("SHOW DATABASES")]
    "STREAMLIT_DEMO_DB" = st.text_input("Sample Database Name", "STREAMLIT_DEMO_DB")
    if "STREAMLIT_DEMO_DB" not in databases:
        st.error(f":warning: Missing database `STREAMLIT_DEMO_DB`. Create it?")
        if st.button(f"Create STREAMLIT_DEMO_DB"):
            st.warning(f"Creating `STREAMLIT_DEMO_DB...`")
            run_query(f"CREATE DATABASE STREAMLIT_DEMO_DB")
            st.success(f"Created `STREAMLIT_DEMO_DB`")
            # caching.clear_cache()
            st.success("The cache has been cleared.")
            st.button("Reload this page")
        return False

    st.success(f"Found STREAMLIT_DEMO_DB!")
    if st.button(f"Destroy STREAMLIT_DEMO_DB"):
        st.warning(f"Destroying `STREAMLIT_DEMO_DB...`")
        run_query(f"DROP DATABASE STREAMLIT_DEMO_DB CASCADE")
        st.success(f"Destroyed `STREAMLIT_DEMO_DB`")
        # caching.clear_cache()
        st.success("The cache has been cleared.")
        st.button("Reload this page")
        return False

    return True


# TODO step 3: We need to look at this together when the app's running again.
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
        if key not in st.session_state:
            randomize_names(key)
        st.multiselect(f"Select {name_type}s", names[key], key=key)
        st.button(f"Randomize {name_type}s", on_click=randomize_names, args=(key,))

    n_firstnames = len(getattr(st.session_state, "firstnames"))
    n_lastnames = len(getattr(st.session_state, "lastnames"))
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
                for firstname in st.session_state.firstnames
                for lastname in st.session_state.lastnames
            ],
            n_rows,
        )
    )
    st.write(df)

    # TODO step 2: Once you get the app to this point, let's look at it together.
    raise RuntimeError("Todo step 2: Fix this section.") 
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
            run_query(f"use database {state.database}")
            run_query(f"create schema if not exists {schema}")
            run_query(f"use schema {schema}")
            run_query(f"drop table if exists {table}")
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

    tables = get_public_tables("STREAMLIT_DEMO_DB")
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
    table1 = st.sidebar.selectbox("Choose Table 1", tables)
    # t2_index = get_index(tables, state_table)
    table2 = st.sidebar.selectbox("Choose Table 2", tables)

    tables = st.multiselect("Select tables to compare", tables, default=tables[:2])

    st.write("state.database", "STREAMLIT_DEMO_DB")
    st.write("tables", tables)
    df1 = run_query(f"select * from STREAMLIT_DEMO_DB.public.{table1}", as_df=True)
    st.write(df1)

    df = pd.DataFrame([], columns=[])
    with st.spinner(f"Getting data..."):
        # TABLE1
        table1 = f"STREAMLIT_DEMO_DB.public.{table1}"
        st.write("table1", table1)
        df1 = run_query(
            f"select (sha2(email || 'abc')) as email_hash from {table2}", as_df=True
        )
        st.write(f"Got {len(df1)} records from Table 1: `{table1}`")

        # TABLE2
        table2 = table2 if "." in table2 else f"STREAMLIT_DEMO_DB.public.{table2}"
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
        get_engine()
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
    
    # initiate state
    st.session_state.databases = st.session_state.get("databases", [])

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
    main()

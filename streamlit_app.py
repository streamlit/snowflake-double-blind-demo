import yaml, random, streamlit as st, pandas as pd, sqlalchemy as sa
from enum import Enum

# from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pandas_tools import pd_writer
from typing import List, Tuple, Dict
from collections import namedtuple

DEMO_DB = "STREAMLIT_DEMO_DB"

# TODO: Would be good to see if we could use st.experimental_memo and
# st.experimental_singleton if possible instead of st.cache.
# https://docs.streamlit.io/library/api-reference/performance
def _snowflake_cache(**cache_args):
    """A specialized version of the st.cache decorator for Snowflake."""
    return st.cache(
        hash_funcs={
            "_thread.RLock": lambda _: None,
            "builtins.weakref": lambda _: None,
        },
        **cache_args,
    )


def _snowflake_singleton(**cache_args):
    """A specialized version of the st.cache decorator for singleton objects."""
    return _snowflake_cache(allow_output_mutation=True, ttl=600, **cache_args)


@_snowflake_singleton()
def get_engine(snowflake_creds: Dict[str, str]):
    """Returns the snowflake connector engine. Uses st.cache to only run once."""
    url_template = "snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    return sa.create_engine(url_template.format(**snowflake_creds), echo=False)


# @st.cache(ttl=600, **SNOWFLAKE_CACHE_ARGS)
# @_snowflake_cache(ttl=600)
def run_query(query: str, as_df=False):
    """Perform query. Uses st.cache to only rerun when the query changes
    after 10 min."""
    conn = get_engine(st.secrets["snowflake"])
    try:
        result = conn.execute(query)
    except Exception as E:
        raise Exception(f"{E}\n\nError running SQL query:\n{query}")

    # namedtuples allow property and index reference
    columns = list(result.keys())  # type: ignore
    Row = namedtuple("Row", columns)
    rows = [Row(*row) for row in result.fetchall()]  # type: ignore

    if as_df:
        return pd.DataFrame(rows, columns=columns)
    return rows


def get_tables(database=DEMO_DB, schema="PUBLIC") -> pd.DataFrame:
    """Returns the list of table names from the demo database."""
    tables = run_query(f"SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES")
    tables = [t for t in tables if t.table_schema.lower() == schema.lower()]
    return pd.DataFrame(tables)[["table_name", "row_count"]]


####################### SYNTHETHIC DATA APP


@_snowflake_cache()
def load_names() -> Tuple[List[str], List[str]]:
    """Returns two lists (firstnames, lastnames) of example names."""
    with open("names.yaml") as name_file:
        return yaml.safe_load(name_file)


class Key(Enum):
    FIRST_NAMES = "firstnames"
    LAST_NAMES = "lastnames"


def randomize_names(key: Key):
    """Randomize either the list of firstnames or lastnames."""
    names = load_names()
    random_names = random.sample(names[key.value], 10)  # type: ignore
    setattr(st.session_state, key.value, random_names)


# TODO step 3:
#  - This tests should be done
def database_form():
    global table1, table2
    databases = [row.name for row in run_query("SHOW DATABASES")]
    with st.sidebar:
        if "STREAMLIT_DEMO_DB" not in databases:
            st.error(f":warning: Missing database `STREAMLIT_DEMO_DB`. Create it?")
            if st.button(f"Create STREAMLIT_DEMO_DB"):
                st.warning(f"Creating `STREAMLIT_DEMO_DB...`")
                run_query(f"CREATE DATABASE STREAMLIT_DEMO_DB")
                st.success(f"Created `STREAMLIT_DEMO_DB`")
                # caching.clear_cache()
                st.success("The cache has been cleared.")
                st.button("Reload this page")
            return

        # show tables
        tables = get_tables()
        table1 = st.sidebar.selectbox("Choose Table 1", tables)
        table2 = st.sidebar.selectbox("Choose Table 2", tables)

        # advanced form, destroy database
        with st.expander("Advanced"):
            if st.button(f"DROP PUBLIC Tables"):
                st.warning(f"Dropping all tables in schema `PUBLIC`")
                engine = get_engine(st.secrets["snowflake"])
                for table in tables:
                    run_query(f"DROP TABLE STREAMLIT_DEMO_DB.PUBLIC.{table}", engine)
                if st.button("Reload this page"):
                    # st.caching.clear_cache()
                    st.success("DELETE THIS: Just clicked the button.")

            if st.button(f"Nuke STREAMLIT_DEMO_DB"):
                st.warning(f"Destroying `STREAMLIT_DEMO_DB...`")
                run_query(f"DROP DATABASE STREAMLIT_DEMO_DB CASCADE")
                st.success(f"Destroyed `STREAMLIT_DEMO_DB`")
                if st.button("Reload this page"):
                    # st.caching.clear_cache()
                    st.success("DELETE THIS: Just clicked the button.")


def synthetic_data_page():
    """Create some synthetic tables with which we can select data."""
    st.write("## Create synthetic data")

    # Show select boxes for the first and last names.
    names = load_names()

    name_types = [("first name", Key.FIRST_NAMES), ("last name", Key.LAST_NAMES)]
    for name_type, key in name_types:
        if key not in st.session_state:
            randomize_names(key)
        st.multiselect(f"Select {name_type}s", names[key.value], key=key.value)
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

    # TODO: determine max number of SAMPLE tables? now is 10
    def make_table_name():
        tables = get_tables()
        print(tables)
        table_name = f"ERROR"
        for i in range(10):
            table_name = f"SAMPLE_{i}"
            if table_name not in tables:
                break
            if i == 9:
                raise Exception(
                    f"""
            Reached maximum number of SAMPLE tables.
            Please clear database or drop table {table_name}.
          """
                )
        return table_name

    table = make_table_name()
    if st.button(f'Create table "{table}"'):
        st.warning(f"Creating `{table}` with len `{len(df)}`.")
        engine = get_engine(st.secrets["snowflake"])
        run_query(f"use database STREAMLIT_DEMO_DB", engine)
        run_query(f"use schema PUBLIC", engine)
        run_query(f"drop table if exists PUBLIC.{table}", engine)
        df.to_sql(
            table,
            engine,
            schema="PUBLIC",
            index=False,
            method=pd_writer,
        )
        st.success(f"Created table `{table}`!")
        table = make_table_name()


def intro_page():
    """Show the text the user first sees when they run the app."""
    with open("README.md") as readme:
        st.markdown(readme.read())


def double_bind_join_page():
    global table1, table2
    st.markdown("## Double Bind Join")

    tables = get_tables()
    if len(tables) < 2:
        st.warning(
            ":point_left: Must have a least two tables to compare."
            "**Please select *Synthetic table generator* to create data.**"
        )
        return

    # tables = st.multiselect("Select tables to compare", tables, default=tables[:2])

    if not (table1 or table2):
        return

    df = pd.DataFrame([], columns=[])
    with st.spinner(f"Getting data..."):
        # TABLE1
        df1 = run_query(
            f"select (sha2(email || 'abc')) as email_hash from STREAMLIT_DEMO_DB.PUBLIC.{table1}",
            as_df=True,
        )
        st.write(f"Got {len(df1)} records from Table 1: `{table1}`")

        # TABLE2
        df2 = run_query(
            f"select sha2(email || 'abc') as email_hash from STREAMLIT_DEMO_DB.PUBLIC.{table2}",
            as_df=True,
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
    # try:
    #     get_engine(st.secrets["snowflake"])
    # except:
    #     snowflake_tutorial = (
    #         "https://docs.streamlit.io/en/latest/tutorial/snowflake.html"
    #     )
    #     st.sidebar.error(
    #         f"""
    #         Couldn't load your credentials.
    #         Did you have a look at our
    #         [tutorial on connecting to Snowflake]({snowflake_tutorial})?
    #         """
    #     )
    #     # raise

    # # initiate state
    # st.session_state.databases = st.session_state.get("databases", [])

    # Get the database tables created by the user.
    tables = get_tables()

    # Show a browser for what functions they could run.
    st.sidebar.success("Select a mode below.")
    modes = {
        "Intro": intro_page,
        "Synthetic data generator": synthetic_data_page,
        "Double-blind join": double_bind_join_page,
        "Show the source code": None,
    }
    selected_mode_name = st.sidebar.selectbox("Select mode", list(modes))  # type: ignore

    # Show the tables
    st.sidebar.write("---")
    st.sidebar.subheader("Tables of Contacts")
    st.sidebar.table(tables)

    # Run the selected mode
    selected_mode = modes[selected_mode_name]
    selected_mode()


if __name__ == "__main__":
    main()

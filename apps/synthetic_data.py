# from numpy import number
import sqlalchemy as sa
import streamlit as st

# from streamlit import caching
import pandas as pd
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.pandas_tools import pd_writer
import yaml
from typing import List, Tuple
import random

# INFORMATION_SCHEMA_TABLES_COLUMNS = [
#     "TABLE_CATALOG",
#     "TABLE_SCHEMA",
#     "TABLE_NAME",
#     "TABLE_OWNER",
#     "TABLE_TYPE",
#     "IS_TRANSIENT",
#     "CLUSTERING_KEY",
#     "ROW_COUNT",
#     "BYTES",
#     "RETENTION_TIME",
#     "SELF_REFERENCING_COLUMN_NAME",
#     "REFERENCE_GENERATION",
#     "USER_DEFINED_TYPE_CATALOG",
#     "USER_DEFINED_TYPE_SCHEMA",
#     "USER_DEFINED_TYPE_NAME",
#     "IS_INSERTABLE_INTO",
#     "IS_TYPED",
#     "COMMIT_ACTION",
#     "CREATED",
#     "LAST_ALTERED",
#     "AUTO_CLUSTERING_ON",
#     "COMMENT",
# ]


SAMPLE_DATABASE = "SAMPLE_EMAILS"


def _snowflake_cache(**cache_args):
    """Returns a specialized version of the st.cache decorator for Snowflake."""
    return st.cache(hash_funcs={"_thread.RLock": lambda _: None}, **cache_args)


@_snowflake_cache(show_spinner=False, allow_output_mutation=True)
def get_connector():
    """Returns the snowflake connector. Uses st.cache to only run once."""
    return snowflake.connector.connect(**st.secrets["snowflake"])


# @st.cache(ttl=600, **SNOWFLAKE_CACHE_ARGS)
@_snowflake_cache(ttl=600)
def run_query(query: str):
    """Perform query. Uses st.cache to only rerun when the query changes
    after 10 min."""
    conn = get_connector()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


# @st.cache(ttl=600)
def get_databases() -> List[str]:
    return [row[1] for row in run_query("SHOW DATABASES;")]


# @st.cache(ttl=600)
# def get_tables(database) -> pd.DataFrame:
#     tables = pd.DataFrame(
#         run_query(f"SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES LIMIT 10;")
#     )

#     tables.columns = INFORMATION_SCHEMA_TABLES_COLUMNS
#     return tables


@_snowflake_cache()
def load_names() -> Tuple[List[str], List[str]]:
    """Returns two lists (firstnames, lastnames) of example names."""
    with open("names.yaml") as name_file:
        return yaml.load(name_file)


def empty_function(conn: SnowflakeConnection):
    """TODO: Delete this function."""
    st.success("This is an empty function.")


def randomize_names(key: str):
    """Randomize either the list of firstnames or lastnames."""
    assert key in ("firstnames", "lastnames")
    names = load_names()
    random_names = random.sample(names[key], 10)
    setattr(st.session_state, key, random_names)


def create_table(table_name: str, df: pd.DataFrame):
    """Called when the user creates a new table of email addresses."""
    st.warning(f"Creating `{SAMPLE_DATABASE}.{table_name}` with len `{len(df)}`.")
    st.text(pd_writer)
    # st.write(sa.create_engine)
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine

    engine = sa.create_engine(**st.secrets["snowflake"])
    st.text(type(url))
    st.text(url)
    conn2 = engine.connect()
    try:
        # conn2.execute(<SQL>)
        df.to_sql(
            f"{SAMPLE_DATABASE}.{table_name}",
            conn,
            if_exists="replace",
            index=False,
            method=pd_writer,
        )
        st.success(f"Created `{table_name}` with len `{len(df)}`.")
    finally:
        conn2.close()
        engine.dispose()
    # account = 'myorganization-myaccount',
    # user = 'testuser1',
    # password = '0123456',
    # database = 'testdb',
    # schema = 'public',
    # warehouse = 'testwh',
    # role='myrole',
    # ))



# pandas.core.generic.to_sql(name: 'str', con, schema=None, if_exists: 'str' = 'fail', index: 'bool_t' = True, index_label=None, chunksize=None, dtype: 'DtypeArg | None' = None, method=None) -> 'None'
# Write records stored in a DataFrame to a SQL database.

# Databases supported by SQLAlchemy [1]_ are supported. Tables can be
# newly created, appended to, or overwritten.

# Parameters
# ----------
# name : str
#     Name of SQL table.
# con : sqlalchemy.engine.(Engine or Connection) or sqlite3.Connection
#     Using SQLAlchemy makes it possible to use any DB supported by that
#     library. Legacy support is provided for sqlite3.Connection objects. The user
#     is responsible for engine disposal and connection closure for the SQLAlchemy
#     connectable See `here                 <https://docs.sqlalchemy.org/en/13/core/connections.html>`_.

# schema : str, optional
#     Specify the schema (if database flavor supports this). If None, use
#     default schema.
# if_exists : {'fail', 'replace', 'append'}, default 'fail'
#     How to behave if the table already exists.

#     * fail: Raise a ValueError.
#     * replace: Drop the table before inserting new values.
#     * append: Insert new values to the existing table.

# index : bool, default True
#     Write DataFrame index as a column. Uses `index_label` as the column
#     name in the table.
# index_label : str or sequence, default None
#     Column label for index column(s). If None is given (default) and
#     `index` is True, then the index names are used.
#     A sequence should be given if the DataFrame uses MultiIndex.
# chunksize : int, optional
#     Specify the number of rows in each batch to be written at a time.
#     By default, all rows will be written at once.
# dtype : dict or scalar, optional
#     Specifying the datatype for columns. If a dictionary is used, the
#     keys should be the column names and the values should be the
#     SQLAlchemy types or strings for the sqlite3 legacy mode. If a
#     scalar is provided, it will be applied to all columns.
# method : {None, 'multi', callable}, optional
#     Controls the SQL insertion clause used:

#     * None : Uses standard SQL ``INSERT`` clause (one per row).
#     * 'multi': Pass multiple values in a single ``INSERT`` clause.
#     * callable with signature ``(pd_table, conn, keys, data_iter)``.

#     Details and a sample callable implementation can be found in the
#     section :ref:`insert method <io.sql.method>`.


def create_synthetic_data(conn: SnowflakeConnection):
    """Create some synthetic tables with which we can select data."""
    # # Let the user create a sample database
    # if SAMPLE_DATABASE not in get_databases():
    #     st.error(f":warning: Missing database `{SAMPLE_DATABASE}`. Create it?")
    #     if st.button(f"Create {SAMPLE_DATABASE}"):
    #         st.warning(f"Creating `{SAMPLE_DATABASE}...`")
    #         conn.cursor().execute(f"CREATE DATABASE {SAMPLE_DATABASE};")
    #         st.success(f"Created `{SAMPLE_DATABASE}`")
    #         caching.clear_cache()
    #         st.success("The cache has been cleared.")
    #         st.button("Reload this page")
    #     return

    # # Show the tables
    # # tables = get_tables(SAMPLE_DATABASE)
    # # run_query(f"SELECT * FROM {SAMPLE_DATABASE}.INFORMATION_SCHEMA.TABLES;")
    # tables = run_query(f"SHOW TABLES IN DATABASE {SAMPLE_DATABASE};")
    # st.write("tables", tables)

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
                    "firstname": firstname,
                    "lastname": lastname,
                    "email": f"{firstname}.{lastname}@gmail.com",
                }
                for firstname in st.session_state.firstnames
                for lastname in st.session_state.lastnames
            ],
            n_rows,
        )
    )
    st.write(df)
    table_name = st.text_input("Table name").strip()
    if not table_name:
        st.warning("Enter a table name above an press enter to create a new table.")
    else:
        st.button(
            f'Create "{table_name}"', on_click=create_table, args=(table_name, df)
        )
    # st.write(st.text_input)
    # st.write(df.to_sql)

    # st.write(f"n_rows: `{n_rows}`")
    # st.write(f"n_firstnames: `{n_firstnames}`")
    # st.write(f"n_lastnames: `{n_lastnames}`")

    # table_name = st.text_input("Table name")
    # if st.button("Insert a table"):
    #     st.write(f"Inserting `{table_name}`")

    # st.write("### firstnames", firstnames, "### lastnames", lastnames)


def simple_table_browser(conn: SnowflakeConnection):
    """Minimalistic browswer of your tables in Snowflake."""
    with st.spinner(f"Collecting databases available in Snowflake..."):
        databases = get_databases()

    if "SNOWFLAKE_SAMPLE_DATA" in databases:
        index = databases.index("SNOWFLAKE_SAMPLE_DATA")
    else:
        index = 0

    database = st.sidebar.selectbox("Choose a DB", databases, index=index)

    st.write(f"## ❄️ Snowflake dashboard")
    st.write(f"Database: `{database}`")
    tables = get_tables(database)

    st.write("### Table view")

    st.write(
        f"Below you'll find the 10 largest tables in your Snowflake database `{database}`:"
    )

    st.table(
        tables[["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "BYTES"]]
        .set_index("TABLE_CATALOG")  # type: ignore
        .sort_values(by="BYTES", ascending=False)  # type: ignore
        .head(10)  # type: ignore
    )


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

    # # Show a browser for what functions they could run.
    # modes = {
    #     "Introduction": empty_function,
    #     "Create Synthetic Data": create_synthetic_data,
    #     "Simple Table Browser": simple_table_browser,
    # }
    # selected_mode_name = st.sidebar.selectbox(
    #     "Select mode", modes.keys()  # type: ignore
    # )
    # selected_mode = modes[selected_mode_name]
    # selected_mode(conn)
    create_synthetic_data(conn)


main()

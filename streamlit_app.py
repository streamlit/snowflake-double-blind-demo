import streamlit as st
from streamlit import caching
import pandas as pd
import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection

INFORMATION_SCHEMA_TABLES_COLUMNS = [
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "TABLE_OWNER",
    "TABLE_TYPE",
    "IS_TRANSIENT",
    "CLUSTERING_KEY",
    "ROW_COUNT",
    "BYTES",
    "RETENTION_TIME",
    "SELF_REFERENCING_COLUMN_NAME",
    "REFERENCE_GENERATION",
    "USER_DEFINED_TYPE_CATALOG",
    "USER_DEFINED_TYPE_SCHEMA",
    "USER_DEFINED_TYPE_NAME",
    "IS_INSERTABLE_INTO",
    "IS_TYPED",
    "COMMIT_ACTION",
    "CREATED",
    "LAST_ALTERED",
    "AUTO_CLUSTERING_ON",
    "COMMENT",
]


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
def get_databases() -> list:
    return [row[1] for row in run_query("SHOW DATABASES;")]


# @st.cache(ttl=600)
def get_tables(database) -> pd.DataFrame:
    tables = pd.DataFrame(
        run_query(f"SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES LIMIT 10;")
    )

    tables.columns = INFORMATION_SCHEMA_TABLES_COLUMNS
    return tables


def empty_function(conn: SnowflakeConnection):
    """TODO: Delete this function."""
    st.success("This is an empty function.")


def create_synthetic_data(conn: SnowflakeConnection):
    """Create some synthetic tables with which we can select data."""
    # Let the user create a sample database
    if SAMPLE_DATABASE not in get_databases():
        st.error(f":warning: Missing database `{SAMPLE_DATABASE}`. Create it?")
        if st.button(f"Create {SAMPLE_DATABASE}"):
            st.warning(f"Creating `{SAMPLE_DATABASE}...`")
            conn.cursor().execute(f"CREATE DATABASE {SAMPLE_DATABASE};")
            st.success(f"Created `{SAMPLE_DATABASE}`")
            caching.clear_cache()
            st.success("The cache has been cleared.")
            st.button("Reload this page")
        return

    # Show the tables
    # tables = get_tables(SAMPLE_DATABASE)
    # run_query(f"SELECT * FROM {SAMPLE_DATABASE}.INFORMATION_SCHEMA.TABLES;")
    tables = run_query(f"SHOW TABLES IN DATABASE {SAMPLE_DATABASE};")
    st.write("tables", tables)

    table_name = st.text_input("Table name")
    if st.button("Insert a table"):
        st.write(f"Inserting `{table_name}`")


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

    # Show a browser for what functions they could run.
    modes = {
        "Introduction": empty_function,
        "Create Synthetic Data": create_synthetic_data,
        "Simple Table Browser": simple_table_browser,
    }
    selected_mode_name = st.sidebar.selectbox(
        "Select mode", modes.keys()  # type: ignore
    )
    selected_mode = modes[selected_mode_name]
    selected_mode(conn)


if __name__ == "__main__":
    main()

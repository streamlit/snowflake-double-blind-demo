import streamlit as st
import pandas as pd
import snowflake.connector

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


@st.cache(
    allow_output_mutation=True,
    hash_funcs={"_thread.RLock": lambda _: None},
)
def get_connector():
    """Returns the snowflake connector. Uses st.cache to only run once."""
    return snowflake.connector.connect(**st.secrets["snowflake"], client_session_keep_alive=True)


def main():
    # Get the snowflake connector. Display an error if anything went wrong.
    try:
        conn = get_connector()
        st.sidebar.success("🎉 We have successfully loaded your Snowflake credentials")
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

    # Perform query.
    # Uses st.cache to only rerun when the query changes or after 10 min.
    @st.cache(ttl=600, show_spinner=False)
    def run_query(query):
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
        f"Below you'll find the 10 heaviest tables in your Snowflake database `{database}`:"
    )

    st.table(
        tables[["TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "BYTES"]]
        .set_index("TABLE_CATALOG")
        .sort_values(by="BYTES", ascending=False)
        .head(10)
    )


if __name__ == "__main__":
    main()

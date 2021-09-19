import streamlit as st
import pandas as pd
import snowflake.connector

# from streamlit_agraph import agraph, TripleStore, Config, Node, Edge

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


def get_graph_data(tables, max_schemas=5, max_tables=15):

    # Add schemas
    schema_subset = tables.TABLE_SCHEMA.unique()[:max_schemas]
    nodes = [Node(id=schema, size=200, color="red") for schema in schema_subset]

    # Add tables
    # TODO: Better filtering of max_tables. Should be max_tables per schema. `groupby().sample()` etc.
    tables_subset = tables.TABLE_NAME.unique()[:max_tables]
    nodes += [Node(id=table, size=100, color="blue") for table in tables_subset]

    # Add schema -> table
    edges = [
        Edge(source=schema, target=table)
        for schema, table in tables[["TABLE_SCHEMA", "TABLE_NAME"]]
        .drop_duplicates()
        .values
        if schema in schema_subset and table in tables_subset
    ]

    config = Config(
        height=600,
        width=600,
        nodeHighlightBehavior=True,
        highlightColor="#F7A7A6",
        directed=True,
        collapsible=True,
        initialZoom=1.5,
    )

    return config, nodes, edges


def main():

    # Initialize connection.
    # Uses st.cache to only run once.
    @st.cache(
        allow_output_mutation=True,
        hash_funcs={"_thread.RLock": lambda _: None},
        show_spinner=False,
    )
    def init_connection():
        return snowflake.connector.connect(**st.secrets["snowflake"])

    try:
        conn = init_connection()
        st.sidebar.success("🎉 We have successfully loaded your Snowflake credentials")
    except Exception as e:
        st.sidebar.error(
            """Couldn't load your credentials.  
            Did you have a look at our [tutorial on connecting to Snowflake](https://docs.streamlit.io/en/latest/tutorial/snowflake.html)?
            """
        )

        with st.sidebar.expander("👇 Read more about the error"):
            st.write(e)

        return ""

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

    # st.write("### Graph view")

    # with st.spinner(f"Converting to graph data..."):
    #     config, nodes, edges = get_graph_data(tables, max_tables=10)

    # agraph(nodes, edges, config)

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

    return ""


if __name__ == "__main__":
    main()

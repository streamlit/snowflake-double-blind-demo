import yaml, random, re
import streamlit as st
import pandas as pd
import sqlalchemy as sa
from enum import Enum
from snowflake.connector.pandas_tools import pd_writer
from typing import List, Tuple, Dict, Set
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


def get_tables() -> pd.DataFrame:
    """Returns the list of table names from the demo database."""
    if "tables" not in st.session_state:
        tables = run_query(f"SELECT * FROM {DEMO_DB}.INFORMATION_SCHEMA.TABLES")
        tables = [t for t in tables if t.table_schema.lower() == "public"]
        st.session_state.tables = pd.DataFrame(tables)[["table_name", "row_count"]]
    return st.session_state.tables


def add_table(name: str, table: pd.DataFrame) -> None:
    """Add a new table of contacts."""
    with st.spinner(f"Creating `{name}` with len `{len(table)}`."):
        engine = get_engine(st.secrets["snowflake"])
        run_query(f"use database STREAMLIT_DEMO_DB", engine)
        run_query(f"use schema PUBLIC", engine)
        run_query(f"drop table if exists PUBLIC.{name}", engine)
        table.to_sql(name, engine, schema="PUBLIC", index=False, method=pd_writer)
        for attr in ("firstnames", "lastnames", "tables"):
            if hasattr(st.session_state, attr):
                delattr(st.session_state, attr)


@st.experimental_memo(show_spinner=False)
def create_unique_table_name(tables: Set[str]) -> str:
    """Creates a table name not in the set of existing table names."""
    suffix = 0
    while (table_name := f"CONTACT_TABLE_{suffix}") in tables:
        suffix += 1
    return table_name


####################### SYNTHETHIC DATA APP


@st.experimental_memo(max_entries=1, show_spinner=False)
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
    st.write("### :level_slider: Select names")
    names = load_names()
    name_types = [("first name", Key.FIRST_NAMES), ("last name", Key.LAST_NAMES)]
    for name_type, key in name_types:
        if key.value not in st.session_state:
            randomize_names(key)
        st.multiselect(f"Select {name_type}s", names[key.value], key=key.value)
        st.button(f"Randomize {name_type}s", on_click=randomize_names, args=(key,))

    # Show a slider for the number of names
    n_firstnames = len(getattr(st.session_state, "firstnames"))
    n_lastnames = len(getattr(st.session_state, "lastnames"))
    max_rows = n_firstnames * n_lastnames
    assert max_rows > 0, "Must have a least one first and last name."
    n_rows = st.slider("Number of rows", 1, max_rows, min(max_rows, 50))

    # Show a preview of the synthetic contacts
    st.write("### :sleuth_or_spy: Data preview")
    synthetic_contacts = pd.DataFrame(
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
    st.write(synthetic_contacts)

    # Show a button to let the user create a new table
    existing_tables = set(get_tables().table_name)
    table_name = create_unique_table_name(existing_tables)
    st.button(
        f'Create table "{table_name}"',
        on_click=add_table,
        args=(table_name, synthetic_contacts),
    )


def intro_page():
    """Show the text the user first sees when they run the app."""
    with open("README.md") as readme:
        st.markdown(readme.read())

    with st.expander("See the session state"):
        st.write(st.session_state)
        if "tables" in st.session_state:
            st.write(st.session_state.tables)


def double_bind_join_page():
    # global table1, table2
    st.markdown("## Double-blind Join")

    tables = get_tables()
    if len(tables) < 2:
        st.warning(
            ":point_left: Must have a least two tables to compare."
            "**Please select *Synthetic table generator* to create data.**"
        )
        return

    hashed_emails = [None, None]
    for i, col in enumerate(st.columns(2)):
        selected_table = col.selectbox(f"Table {i}", tables)
        hash = col.text_input(f"Hash {i}", "abc")
        assert re.match("^[a-z]+$", hash), "Hash must contain only lowercase letters."
        hashed_emails[i] = run_query(  # type: ignore
            f"select concat(email, '{hash}') as whole_email, "
            f"(sha2(concat(email, '{hash}'))) as email_hash from "
            f"STREAMLIT_DEMO_DB.PUBLIC.{selected_table}",
            as_df=True,
        )
        col.write(hashed_emails[i])
        col.caption(f"`{len(hashed_emails[i])}` records from `{selected_table}`")  # type: ignore

    df1, df2 = hashed_emails
    matching_hashes = len(set(df1.email_hash).intersection(df2.email_hash))  # type: ignore
    st.write("Matching hashes", matching_hashes)


def main():
    """Execution starts here."""
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

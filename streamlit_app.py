import yaml, random, re
import streamlit as st
import pandas as pd
import sqlalchemy as sa
from snowflake.connector.pandas_tools import pd_writer
from typing import Dict, Set
from collections import namedtuple

DEMO_DB = "STREAMLIT_DEMO_DB"


@st.experimental_singleton()
def get_engine(snowflake_creds: Dict[str, str]):
    """Returns the snowflake connector engine. Uses st.cache to only run once."""
    url_template = "snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    return sa.create_engine(url_template.format(**snowflake_creds), echo=False)


def run_query(query: str, as_df=False):
    """Perform query."""
    conn = get_engine(st.secrets["snowflake"])
    result = conn.execute(query)
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


def clear_state() -> None:
    """Remove state variables."""
    for attr in ("firstnames", "lastnames", "tables"):
        if hasattr(st.session_state, attr):
            delattr(st.session_state, attr)


def add_table(name: str, table: pd.DataFrame) -> None:
    """Add a new table of contacts."""
    with st.spinner(f"Creating `{name}` with len `{len(table)}`."):
        engine = get_engine(st.secrets["snowflake"])
        run_query(f"use database {DEMO_DB}", engine)
        run_query(f"use schema PUBLIC", engine)
        run_query(f"drop table if exists PUBLIC.{name}", engine)
        table.to_sql(name, engine, schema="PUBLIC", index=False, method=pd_writer)
        clear_state()


@st.experimental_memo(show_spinner=False)
def create_unique_table_name(tables: Set[str]) -> str:
    """Creates a table name not in the set of existing table names."""
    suffix = 0
    table_name = f"CONTACTS_{suffix}"
    while table_name in tables:
        suffix += 1
        table_name = f"CONTACTS_{suffix}"
    return table_name


@st.experimental_memo(max_entries=1, show_spinner=False)
def load_names():
    """Returns two lists (firstnames, lastnames) of example names."""
    with open("names.yaml") as name_file:
        return yaml.safe_load(name_file)


def randomize_names(key: str):
    """Randomize either the list of firstnames or lastnames."""
    names = load_names()
    random_names = random.sample(names[key], 10)  # type: ignore
    setattr(st.session_state, key, random_names)


def intro_page():
    """Show the text the user first sees when they run the app."""
    with open("README.md") as readme:
        st.markdown(readme.read())
    st.write("## :gear: Example use")
    st.image("example.gif")


def synthetic_data_page():
    """Create some synthetic tables with which we can select data."""
    st.write("## :robot_face: Create synthetic data")

    st.success(
        "Feel free to **add and delete names**, and **play with the length slider!** "
        "When you're ready, **click the *create table* button below.**"
    )

    # Show select boxes for the first and last names.
    st.write("### Select names")
    names = load_names()
    name_types = [("first name", "firstnames"), ("last name", "lastnames")]
    for name_type, key in name_types:
        if key not in st.session_state:
            randomize_names(key)
        st.multiselect(f"Select {name_type}s", names[key], key=key)
        st.button(f"Randomize {name_type}s", on_click=randomize_names, args=(key,))

    # Show a slider for the number of names
    n_firstnames = len(getattr(st.session_state, "firstnames"))
    n_lastnames = len(getattr(st.session_state, "lastnames"))
    max_rows = n_firstnames * n_lastnames
    assert max_rows > 0, "Must have a least one first and last name."
    n_rows = st.slider("Number of rows", 1, max_rows, min(max_rows, 50))

    # Show a preview of the synthetic contacts
    st.write("### Data preview")
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


def double_bind_join_page():
    st.markdown("## :see_no_evil: Double-blind Join")

    st.success(
        "Select to tables to compaire. You can also change the hash salt. "
        "**Matching hashes will automatically be show below.**"
    )

    tables = get_tables()
    assert len(tables) >= 2, "Must have a least two tables to compare."

    hashed_emails = [None, None]
    for i, col in enumerate(st.columns(2)):
        selected_table = col.selectbox(f"Table {i}", tables)
        salt = col.text_input(f"Salt {i}", "abc")
        assert re.match("^[a-z]+$", salt), "Hash must contain only lowercase letters."
        hashed_emails[i] = run_query(  # type: ignore
            f"select email, "
            f"(sha2(concat(email, '{salt}'))) as email_hash from "
            f"{DEMO_DB}.PUBLIC.{selected_table}",
            as_df=True,
        )
        col.write(hashed_emails[i])
        col.caption(f"`{len(hashed_emails[i])}` records from `{selected_table}`")  # type: ignore

    df1, df2 = hashed_emails
    matching_hashes = set(df1.email_hash).intersection(df2.email_hash)  # type: ignore
    if len(matching_hashes) == 0:
        st.error("No matching hashes.")
    else:
        st.info(f"Matching hashes: `{len(matching_hashes)}`")
        st.json(list(sorted(matching_hashes)))


def main():
    """Execution starts here."""
    # Get the database tables created by the user.
    tables = get_tables()

    # Show a browser for what functions they could run.
    st.sidebar.success("Select a mode below.")
    modes = {
        "🌟 Intro": intro_page,
        "🤖 Synthetic data generator": synthetic_data_page,
        "🙈 Double-blind join": double_bind_join_page,
    }
    selected_mode_name = st.sidebar.selectbox("Select mode", list(modes))  # type: ignore

    # Show the tables
    st.sidebar.write("---")
    st.sidebar.subheader("Contacts Tables")
    st.sidebar.table(tables)

    # Run the selected mode
    selected_mode = modes[selected_mode_name]
    selected_mode()


if __name__ == "__main__":
    main()

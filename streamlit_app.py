import streamlit as st
import sys, importlib
from collections import OrderedDict

PAGES = OrderedDict(
    [
        ("Intro", "apps/intro.py"),
        ("Synthetic data generator", "apps/synthetic_data.py"),
        ("Double-blind join", "apps/double_bind_join.py"),
        ("Schema browser", "apps/schema_browser.py"),
    ]
)


def main():

    # Basic app information
    APP_TITLE = "Snowflake Demo"
    # st.set_page_config(
    #     page_title=APP_TITLE, layout="wide", page_icon=":snowflake:"
    # )

    # Display the snowflake logo nicely.
    col1, col2 = st.sidebar.columns((1, 9))
    col1.image("snowflake-logo-transparent.png", width=25)
    col2.markdown(f"**{APP_TITLE}**")

    # Infer selected page from query params.
    query_params = st.experimental_get_query_params()
    if "page" in query_params:
        page_url = query_params["page"][0]  # type: ignore
        if page_url in list(PAGES):
            st.session_state["page_selector"] = page_url

    # If viewer clicks on page selector: Update query params to point to this page.
    def change_page_url():
        """Update query params to reflect the selected page."""
        st.experimental_set_query_params(page=st.session_state["page_selector"])

    # Show page selector in sidebar.
    selected_page = st.sidebar.radio(
        "Page",
        list(PAGES),
        key="page_selector",
        on_change=change_page_url,
    )
    st.sidebar.write("---")

    # Import (and therefore show) the selected page. Streamlit doesn't rerun imports, so if
    # the page was already imported before, reload it.
    module_name = PAGES[selected_page].replace("/", ".").replace(".py", "")  # type: ignore
    is_first_import = module_name not in sys.modules
    selected_page_module = importlib.import_module(module_name)
    if not is_first_import:
        importlib.reload(selected_page_module)


if __name__ == "__main__":
    main()

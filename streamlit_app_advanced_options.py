# def advanced_options_page():
#     """advanced form, destroy database"""
#     st.warning("These have not been properly debugged.")
#     return
#     if st.button(f"DROP PUBLIC Tables"):
#         st.warning(f"Dropping all tables in schema `PUBLIC`")
#         engine = get_engine(st.secrets["snowflake"])
#         for table in tables:
#             run_query(f"DROP TABLE STREAMLIT_DEMO_DB.PUBLIC.{table}", engine)
#         st.button("Reload this page")

#     if st.button(f"Nuke STREAMLIT_DEMO_DB"):
#         st.warning(f"Destroying `STREAMLIT_DEMO_DB...`")
#         run_query(f"DROP DATABASE STREAMLIT_DEMO_DB CASCADE")
#         st.success(f"Destroyed `STREAMLIT_DEMO_DB`")
#         st.button("Reload this page")

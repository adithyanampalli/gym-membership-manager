import streamlit as st
import requests
from snowflake.snowpark.functions import col
import pandas as pd

# Set up Streamlit app title
st.title("Gym Membership Manager")
st.write("Manage gym memberships, track attendance, and generate reports.")

# Initialize Snowflake session
cnx = st.connection("snowflake")
session = cnx.session()


# Sidebar menu
menu = ["Manage Members", "Attendance", "Reports"]
choice = st.sidebar.selectbox("Select Option", menu)

# Manage Members
if choice == "Manage Members":
    st.subheader("Manage Gym Members")
    
    # View members
    members_df = session.table("GYM_MANAGEMENT.PUBLIC.GYM_MEMBERS").to_pandas()
    st.dataframe(members_df)

    # Add new member
    with st.form("add_member"):
        st.write("Add New Member")
        name = st.text_input("Name")
        membership_type = st.selectbox("Membership Type", ["Monthly", "Yearly"])
        join_date = st.date_input("Join Date")
        status = st.selectbox("Status", ["Active", "Inactive"])
        
        submit_add = st.form_submit_button("Add Member")
        if submit_add:
            insert_query = f"""
            INSERT INTO GYM_MANAGEMENT.PUBLIC.GYM_MEMBERS (NAME, MEMBERSHIP_TYPE, JOIN_DATE, STATUS)
            VALUES ('{name}', '{membership_type}', '{join_date}', '{status}')
            """
            session.sql(insert_query).collect()
            st.success("New member added!")

# Attendance tracking
elif choice == "Attendance":
    st.subheader("Track Attendance")
    
    members_df = session.table("GYM_MANAGEMENT.PUBLIC.GYM_MEMBERS").filter(col("STATUS") == 'Active').select(col("MEMBER_ID"), col("NAME")).to_pandas()
    st.write("Select member to mark attendance:")
    
    selected_member = st.selectbox("Select Member", members_df['NAME'])
    attendance_date = st.date_input("Attendance Date")
    
    if st.button("Mark Attendance"):
        member_id = members_df[members_df['NAME'] == selected_member]['MEMBER_ID'].values[0]
        insert_attendance_query = f"""
        INSERT INTO GYM_MANAGEMENT.PUBLIC.GYM_ATTENDANCE (MEMBER_ID, ATTENDANCE_DATE)
        VALUES ({member_id}, '{attendance_date}')
        """
        session.sql(insert_attendance_query).collect()
        st.success(f"Attendance marked for {selected_member} on {attendance_date}")

# Reports
elif choice == "Reports":
    st.subheader("Reports")
    
    # Report on active members
    st.write("Active Members")
    active_members_df = session.table("GYM_MANAGEMENT.PUBLIC.GYM_MEMBERS").filter(col("STATUS") == "Active").to_pandas()
    st.dataframe(active_members_df)
    
    # Attendance report
    st.write("Attendance Report")
    attendance_report = session.sql("""
    SELECT GYM_MEMBERS.NAME, COUNT(GYM_ATTENDANCE.ATTENDANCE_ID) AS DAYS_ATTENDED
    FROM GYM_MANAGEMENT.PUBLIC.GYM_ATTENDANCE
    JOIN GYM_MANAGEMENT.PUBLIC.GYM_MEMBERS ON GYM_ATTENDANCE.MEMBER_ID = GYM_MEMBERS.MEMBER_ID
    GROUP BY GYM_MEMBERS.NAME
    """).to_pandas()
    st.dataframe(attendance_report)

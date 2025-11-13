import streamlit as st
import pandas as pd
import pymysql
from datetime import datetime

# ------------------- Streamlit PAGE SETUP -------------------
st.set_page_config(page_title="SECURECHECK POLICE DASHBOARD", layout="wide")
st.header("👮🏻‍♂ DIGITAL LEDGER FOR POLICE POST LOGS")
st.write("""
Welcome to the SecureCheck Police Dashboard!  
We are gonna dive into traffic stop data, advance insights and the Outcome summaries based on Vehicle & Inputs. 
""")

# -------------------  LOAD & CLEAN DATA -------------------
@st.cache_data
def load_data():
    df = pd.read_excel("C:/Traffic Stops/traffic_stops/traffic_stops.xlsx")
    df = df.drop(['violation_raw', 'driver_age_raw'], axis=1, errors='ignore')
    df = df.fillna("Unknown")

    # Clean date & time
    df['stop_date'] = pd.to_datetime(df['stop_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['stop_time'] = pd.to_datetime(df['stop_time'], format='%H:%M:%S', errors='coerce').dt.time
    return df

df = load_data()

# ------------------- MYSQL CONNECTION -------------------
def create_connection():
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="deepi",          
        database="traffic_stops"   
    )
    return connection

# -------------------  RUN MYSQL Query -------------------
def run_query(query):
    try:
        conn = create_connection()
        cursor = conn.cursor()
        
        cursor.execute(query)

        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        return pd.DataFrame(rows, columns=cols)

    except Exception as e:
        st.error(f"Error running query: {e}")
        return pd.DataFrame()  
    
# --- Full Table ---
st.header("🧾 Digital Police Logs Table")
st.dataframe(df.head(65538), width="stretch")

# ------------------- 📊 KEY METRICS -------------------
st.subheader("📈 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Police Stops", df.shape[0])
col2.metric("Total Arrests", df[df['stop_outcome'].str.contains("arrest", case=False, na=False)].shape[0])
col3.metric("Total Warnings", df[df['stop_outcome'].str.contains("warning", case=False, na=False)].shape[0])
col4.metric("Drug Related Stops", df[df['drugs_related_stop'] == 1].shape[0])


# ------------------- 💡 ADVANCED INSIGHTS -------------------
st.subheader("💡 Advanced Insights")

query_mapping = {
        "top 10 vehicle_Number involved in drug-related stops":"""select  vehicle_number , count(*) as stops from police_post_log where drugs_related_stop= True group by vehicle_number order by stops desc limit 10;""",
         "vehicles were most frequently searched":"""SELECT vehicle_number, COUNT(*) AS search_count FROM  police_post_log WHERE search_conducted = True GROUP BY vehicle_number ORDER BY search_count DESC LIMIT 10;""",
         "driver age group had the highest arrest rate":"""SELECT CASE when driver_age between 18 and 25 then '18-25'when driver_age between 26 and 35 then '26-35'when driver_age between 36 and 45 then '36-45'when driver_age between 46 and 60 then '46-60'else '60+' end as age_group,count(*) as total_stops,count(case when is_arrested =TRUE then 1 end) as arrests,
         round(count(case when is_arrested =TRUE then 1 end)/count(*)*100.0,2) as arrest_rate from police_post_log group by age_group order by arrest_rate desc;""",
         "gender distribution of drivers stopped in each country":"""SELECT country_name,driver_gender,COUNT(*) AS total_stops FROM  police_post_log GROUP BY country_name, driver_gender ORDER BY country_name, total_stops DESC;""",
         "race and gender combination has the highest search rate":"""select driver_race,driver_gender,round(count(case when search_conducted =1 then 1 end)/count(*)*100.0, 2) as search_rate from police_post_log group by driver_race, driver_gender order by search_rate DESC;""",
         "time of day sees the most traffic stops":"""SELECT HOUR(stop_time) AS hour_of_the_day, COUNT(*) AS traffic_stops FROM  police_post_log GROUP BY hour_of_the_day ORDER BY traffic_stops DESC LIMIT 10;""",
         "average stop duration for different violations":"""SELECT violation,ROUND(AVG(stop_duration), 2) AS avg_duration FROM  police_post_log GROUP BY violation ORDER BY avg_duration DESC;""",
         "stops during the night more likely to lead to arrests":"""select case when hour(str_to_date(stop_time,'%H:%i'))>=18 or hour(str_to_date(stop_time,'%H:%i'))<6 then 'NIGHT'else 'DAY'end as stop_period,count(*) as total_stops,count(case when is_arrested=TRUE then 1 end) as arrests,round(count(case when is_arrested=TRUE then 1 end)/count(*)*100.0,2) as arrest_rate_percent from  police_post_log group by stop_period;""",
         "violations are most associated with searches or arrests":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 'Yes' THEN 1 ELSE 0 END) AS total_searched,SUM(CASE WHEN is_arrested = 'Yes' THEN 1 ELSE 0 END) AS total_arrests FROM  police_post_log GROUP BY violation ORDER BY total_searched DESC, total_arrests DESC;""",
         "violations are most common among younger drivers (<25)":""" SELECT violation,COUNT(*) AS count_violation FROM  police_post_log  WHERE driver_age < 25 GROUP BY violation ORDER BY count_violation DESC;""",
         "violation that rarely results in search or arrest":"""SELECT violation,count(case when search_conducted=TRUE then 1 end) as count_of_the_search,count(case when is_arrested=TRUE then 1 end) as count_of_the_arrest,ROUND(SUM(search_conducted) / COUNT(*) * 100, 2) AS search_rate_percent,ROUND(SUM(is_arrested) / COUNT(*) * 100, 2) AS arrest_rate_percent FROM police_post_log GROUP BY violation ORDER BY search_rate_percent ASC, arrest_rate_percent ASC;""",
         "countries report the highest rate of drug-related stops":"""select country_name,count(*) as total_counts,round(count(case when drugs_related_stop=TRUE then 1 end)/count(*)*100.0,2) as drugs_related_stop_rates from police_post_log group by country_name order by drugs_related_stop_rates desc;""",
         "arrest rate by country and violation":"""select country_name,violation,count(*) as total_count,round(count(case when is_arrested=TRUE then 1 end)/count(*)*100.0,2) as arrest_rate from police_post_log group by country_name,violation;""",
         "country has the most stops with search conducted":"""SELECT country_name,COUNT(*) AS search_count FROM  police_post_log WHERE search_conducted = TRUE GROUP BY country_name ORDER BY search_count DESC;""",
         "Yearly Breakdown of Stops and Arrests by Country":"""SELECT country_name,year,total_stops,total_arrests,arrest_rate,RANK() OVER (PARTITION BY year ORDER BY total_arrests DESC) AS rank_in_year FROM (SELECT country_name,YEAR(stop_date) AS year,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate FROM police_post_log GROUP BY country_name, YEAR(stop_date)) AS yearly_summary ORDER BY year ASC, rank_in_year ASC;""",
         "Driver Violation Trends Based on Age and Race":"""SELECT t.driver_race,t.age_group,v.violation,COUNT(t.violation) AS violation_count FROM (SELECT *,CASE WHEN driver_age < 18 THEN 'Under 18'WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'ELSE '60+'END AS age_group FROM police_post_log)as t JOIN (SELECT DISTINCT violation FROM police_post_log)as v ON t.violation = v.violation GROUP BY t.driver_race, t.age_group, v.violation ORDER BY violation_count,age_group DESC;""",
         "Time Period Analysis of Stops Number of Stops by Year,Month, Hour of the Day":"""SELECT EXTRACT(YEAR FROM stop_date) AS year,EXTRACT(MONTH FROM stop_date) AS month,EXTRACT(HOUR FROM stop_time) AS hour,COUNT(*) AS total_stops FROM  police_post_log GROUP BY EXTRACT(YEAR FROM stop_date), EXTRACT(MONTH FROM stop_date), EXTRACT(HOUR FROM stop_time) ORDER BY year, month, hour;""",
         "Violations with High Search and Arrest Rates":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) AS total_searches,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate,RANK() OVER (ORDER BY SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) DESC) AS rank_by_arrest FROM  police_post_log GROUP BY violation;""",
         "Driver Demographics by Country":"""SELECT country_name,driver_gender,driver_race,CASE WHEN driver_age < 18 THEN 'Under 18'WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'ELSE '60+'END AS age_group,COUNT(*) AS total_drivers FROM  police_post_log GROUP BY country_name, driver_gender, driver_race, age_group ORDER BY country_name, total_drivers, age_group DESC;""",
         "Top 5 Violations with Highest Arrest Rates":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN is_arrested =  1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate FROM  police_post_log GROUP BY violation ORDER BY arrest_rate DESC;""",
}

query_choice = st.selectbox("📌 Select Query", list(query_mapping.keys()))

if st.button("▶ Run Query"):

    result = run_query(query_mapping[query_choice])

    st.dataframe(result, use_container_width=True)

# -------------------  VEHICLE LOOKUP  -------------------
st.subheader("🔍 Vehicle Stop Summary(Based on vehicle number)")

vehicle_number = st.text_input("Enter Vehicle Number")

if vehicle_number:
    record = df[df['vehicle_number'].astype(str).str.lower() == vehicle_number.lower()]

    if not record.empty:
        row = record.iloc[0]
        pronoun = "he" if row["driver_gender"].lower() == "male" else "she"
        drug_txt = "YES-drug-related" if row["drugs_related_stop"] in [1, True, "True"] else "NO -not drug-related"

        st.markdown(f"""
        🚗 **Vehicle:** {vehicle_number}  
        👤 **Driver:** {row['driver_age']} years old, {row['driver_gender']}  
        ⚠️ **Violation:** {row['violation']}  
        🕒 **Time & Date:** {row['stop_time']} on {row['stop_date']}  
        🎯 **Outcome:** {row['stop_outcome']}  
        🧾 **Stop Duration:** {row['stop_duration']}  
        💊 **Drug Related:** {drug_txt}
        """)

    else:
        st.warning("No record found for this vehicle number.")


# -------------------  STOP OUTCOME SUMMARY -------------------
st.subheader("🚗 Stop Outcome Summary (Based on Your Inputs)")


with st.form("prediction_form"):
    driver_gender = st.selectbox("Driver Gender", ["female", "male"])
    driver_age = st.number_input("Driver Age", min_value=18, max_value=80)
    violation = st.selectbox("Violation Type", ["Seatbelt", "Speeding", "Signal", "DUI", "Other"])
    search_conducted = st.selectbox("Search Conducted", ["False", "True"])
    drugs_related_stop = st.selectbox("Drug Related Stop", ["False", "True"])
    stop_duration = st.selectbox("Stop Duration", df['stop_duration'].dropna().unique())

    submit = st.form_submit_button("Show Stop Outcome input")

if submit:
    searching = "A search was conducted" if search_conducted == "True" else "No search was conducted"
    drug_txt = "was drug-related" if drugs_related_stop == "True" else "was not drug-related"
    pronoun = "he" if driver_gender == "male" else "she"

    st.markdown(f"""
    🚗: A {driver_age}-year-old {driver_gender} driver was stopped for **{violation}**.  
    {searching}, and {pronoun} received an outcome based on data patterns.  
    The stop lasted *{stop_duration}* and it {drug_txt}.
    """)

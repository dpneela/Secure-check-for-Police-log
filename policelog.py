import pymysql
import streamlit as st
import pandas as pd
from datetime import datetime

# ------------------- Load & Clean Data -------------------
df = pd.read_excel("C:/Traffic Stops/traffic_stops/traffic_stops.xlsx")

# Drop unnecessary columns and fill missing values
df = df.drop(['violation_raw','driver_age_raw'], axis=1)
df = df.fillna("Unknown")

# date and time values
df['stop_date'] = pd.to_datetime(df['stop_date'], errors='coerce').dt.strftime('%Y-%m-%d')
df['stop_time'] = pd.to_datetime(df['stop_time'], format='%H:%M:%S', errors='coerce').dt.time

# cleaned Excel
df.to_excel("cleaned_police_log.xlsx", index=False)

# ------------------- MySQL Connection -------------------
def create_connection():
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="deepi",   
            database="traffic_stops"
        )
        connection.autocommit(True)
        return connection
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

connection = create_connection()

# ------------------- Fetch Data -------------------
def fetch_data(query):
    connection = create_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                return pd.DataFrame(result, columns=cols)
        finally:
            connection.close()
    return pd.DataFrame()

# -------------------Streamlit -------------------
st.set_page_config(page_title="SECURECHECK POLICE DASHBOARD", layout="wide")
st.header("👮🏻‍♂ DIGITAL LEDGER FOR POLICE POST LOGS")
st.write("""
Welcome to the SecureCheck Police Dashboard!  
We are gonna dive into traffic stop data, advance insights and the predict outcomes. 
""")

# --- Full Table ---
st.header("🧾 Digital Police Logs Table")
st.dataframe(df.head(65538), width="stretch")

# --- Key Metrics ---
st.header("📊 Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("TOTAL POLICE STOPS", df.shape[0])
col2.metric("TOTAL ARRESTS", df[df['stop_outcome'].str.contains("arrest", case=False, na=False)].shape[0])
col3.metric("TOTAL WARNINGS", df[df['stop_outcome'].str.contains("warning", case=False, na=False)].shape[0])
col4.metric("DRUG RELATED STOPS", df[df['drugs_related_stop'] == 1].shape[0])

# --- Advanced Insights ---
st.header("💡 Advanced Insights")
selecting_the_query = st.selectbox(
    "Select the query to run",
    [
        "top 10 vehicle_Number involved in drug-related stops",
        "vehicles were most frequently searched",
        "driver age group had the highest arrest rate",
        "gender distribution of drivers stopped in each country",
        "race and gender combination has the highest search rate",
        "time of day sees the most traffic stops",
        "average stop duration for different violations",
        "stops during the night more likely to lead to arrests",
        "violations are most associated with searches or arrests",
        "violations are most common among younger drivers (<25)",
        "violation that rarely results in search or arrest",
        "countries report the highest rate of drug-related stops",
        "arrest rate by country and violation",
        "country has the most stops with search conducted",
        "Yearly Breakdown of Stops and Arrests by Country",
        "Driver Violation Trends Based on Age and Race",
        "Time Period Analysis of Stops Number of Stops by Year,Month, Hour of the Day",
        "Violations with High Search and Arrest Rates",
        "Driver Demographics by Country",
        "Top 5 Violations with Highest Arrest Rates"
    ]
)

# Mapping of query names to SQL queries
mapping_of_query={
         "top 10 vehicle_Number involved in drug-related stops":"""select  vehicle_number , count(*) as stops from police_post_log where drugs_related_stop= True group by vehicle_number order by stops desc limit 10;""",
         "vehicles were most frequently searched":"""SELECT vehicle_number, COUNT(*) AS search_count FROM  police_post_log WHERE search_conducted = True GROUP BY vehicle_number ORDER BY search_count DESC LIMIT 10;""",
         "driver age group had the highest arrest rate":"""SELECT CASE when driver_age between 18 and 25 then '18-25'when driver_age between 26 and 35 then '26-35'when driver_age between 36 and 45 then '36-45'when driver_age between 46 and 60 then '46-60'else '60+' end as age_group,count(*) as total_stops,count(case when is_arrested =TRUE then 1 end) as arrests,
         round(count(case when is_arrested =TRUE then 1 end)/count(*)*100.0,2) as arrest_rate from police_post_log group by age_group order by arrest_rate desc;""",
         "gender distribution of drivers stopped in each country":"""SELECT country_name,driver_gender,COUNT(*) AS total_stops FROM  police_post_log GROUP BY country_name, driver_gender ORDER BY country_name, total_stops DESC;""",
         "race and gender combination has the highest search rate":"""select driver_race,driver_gender,round(count(case when search_conducted =TRUE then 1 end)/count(*)*100.0, 2) as search_rate from police_post_log group by driver_race, driver_gender order by search_rate DESC;""",
         "time of day sees the most traffic stops":"""SELECT HOUR(stop_time) AS hour_of_the_day, COUNT(*) AS traffic_stops FROM  police_post_log GROUP BY hour_of_the_day ORDER BY traffic_stops DESC LIMIT 10;""",
         "average stop duration for different violations":"""SELECT violation,ROUND(AVG(stop_duration), 2) AS avg_duration FROM  police_post_log GROUP BY violation ORDER BY avg_duration DESC;""",
         "stops during the night more likely to lead to arrests":"""select case when hour(str_to_date(stop_time,'%H:%i'))>=18 or hour(str_to_date(stop_time,'%H:%i'))<6 then 'NIGHT'else 'DAY'end as stop_period,count(*) as total_stops,count(case when is_arrested=TRUE then 1 end) as arrests,round(count(case when is_arrested=TRUE then 1 end)/count(*)*100.0,2) as arrest_rate_percent from  police_post_log group by stop_period;""",
         "violations are most associated with searches or arrests":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 'Yes' OR  1 THEN 1 ELSE 0 END) AS total_searched,SUM(CASE WHEN is_arrested = 'Yes' OR  1 THEN 1 ELSE 0 END) AS total_arrests FROM  police_post_log GROUP BY violation ORDER BY total_searched DESC, total_arrests DESC;""",
         "violations are most common among younger drivers (<25)":""" SELECT violation,COUNT(*) AS count_violation FROM  police_post_log  WHERE driver_age < 25 GROUP BY violation ORDER BY count_violation DESC;""",
         "violation that rarely results in search or arrest":"""SELECT violation,count(case when search_conducted=TRUE then 1 end) as count_of_the_search,count(case when is_arrested=TRUE then 1 end) as count_of_the_arrest,ROUND(SUM(search_conducted) / COUNT(*) * 100, 2) AS search_rate_percent,ROUND(SUM(is_arrested) / COUNT(*) * 100, 2) AS arrest_rate_percent FROM police_post_log GROUP BY violation ORDER BY search_rate_percent ASC, arrest_rate_percent ASC;""",
         "countries report the highest rate of drug-related stops":"""select country_name,count(*) as total_counts,round(count(case when drugs_related_stop=TRUE then 1 end)/count(*)*100.0,2) as drugs_related_stop_rates from police_post_log group by country_name order by drugs_related_stop_rates desc;""",
         "arrest rate by country and violation":"""select country_name,violation,count(*) as total_count,round(count(case when is_arrested=TRUE then 1 end)/count(*)*100.0,2) as arrest_rate from police_post_log group by country_name,violation;""",
         "country has the most stops with search conducted":"""SELECT country_name,COUNT(*) AS search_count FROM  police_post_log WHERE search_conducted = 1 GROUP BY country_name ORDER BY search_count DESC;""",
         "Yearly Breakdown of Stops and Arrests by Country":"""SELECT country_name,year,total_stops,total_arrests,arrest_rate,RANK() OVER (PARTITION BY year ORDER BY total_arrests DESC) AS rank_in_year FROM (SELECT country_name,YEAR(stop_date) AS year,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate FROM police_post_log GROUP BY country_name, YEAR(stop_date)) AS yearly_summary ORDER BY year ASC, rank_in_year ASC;""",
         "Driver Violation Trends Based on Age and Race":"""SELECT t.driver_race,t.age_group,v.violation,COUNT(t.violation) AS violation_count FROM (SELECT *,CASE WHEN driver_age < 18 THEN 'Under 18'WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'ELSE '60+'END AS age_group FROM police_post_log) t JOIN (SELECT DISTINCT violation FROM police_post_log) v ON t.violation = v.violation GROUP BY t.driver_race, t.age_group, v.violation ORDER BY violation_count DESC;""",
         "Time Period Analysis of Stops Number of Stops by Year,Month, Hour of the Day":"""SELECT EXTRACT(YEAR FROM stop_date) AS year,EXTRACT(MONTH FROM stop_date) AS month,EXTRACT(HOUR FROM stop_time) AS hour,COUNT(*) AS total_stops FROM  police_post_log GROUP BY EXTRACT(YEAR FROM stop_date), EXTRACT(MONTH FROM stop_date), EXTRACT(HOUR FROM stop_time) ORDER BY year, month, hour;""",
         "Violations with High Search and Arrest Rates":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) AS total_searches,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS search_rate,ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate,RANK() OVER (ORDER BY SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) DESC) AS rank_by_arrest FROM  police_post_log GROUP BY violation;""",
         "Driver Demographics by Country":"""SELECT country_name,driver_gender,driver_race,CASE WHEN driver_age < 18 THEN 'Under 18'WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'ELSE '60+'END AS age_group,COUNT(*) AS total_drivers FROM  police_post_log GROUP BY country_name, driver_gender, driver_race, age_group ORDER BY country_name, total_drivers DESC;""",
         "Top 5 Violations with Highest Arrest Rates":"""SELECT violation,COUNT(*) AS total_stops,SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS total_arrests,ROUND(SUM(CASE WHEN is_arrested =  1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS arrest_rate FROM  police_post_log GROUP BY violation ORDER BY arrest_rate DESC;""",
    }    

if st.button("Run the query "):
    output = fetch_data(mapping_of_query.get(selecting_the_query, "SELECT 1"))
    if not output.empty:
        st.write(output)
    else:
        st.warning("NO RESULTS FOUND ")

# --- Predict Outcome and Violation ---
st.header("🔆 Predict the Outcome and Violation")
with st.form("prediction_form"):
    stop_date = st.date_input("STOP DATE")
    time_str = st.text_input("STOP TIME (HH:MM:SS)", value="12:00:00")
    try:
        stop_time = datetime.strptime(time_str, "%H:%M:%S").time()
    except ValueError:
        st.error("Invalid time format. Use HH:MM:SS")
        stop_time = None

    driver_gender = st.selectbox("DRIVER GENDER", ["female", "male"])
    driver_age = st.number_input("DRIVER AGE", min_value=18, max_value=80)
    violation = st.selectbox("VIOLATION", ["Seatbelt", "Speeding", "Signal", "DUI", "Other"])
    search_conducted = st.selectbox("SEARCH CONDUCTED", ["0", "1"])
    stop_outcome = st.selectbox("STOP OUTCOME", ["Ticket", "Arrest", "Warning"])
    stop_duration = st.selectbox("STOP DURATION", df['stop_duration'].dropna().unique())
    drugs_related_stop = st.selectbox("DRUG RELATED", ["0", "1"])
    vehicle_number = st.text_input("VEHICLE NUMBER")
    
    submit = st.form_submit_button("PREDICT THE STOP OUTCOME AND VIOLATION 🌟")

if submit:
    filter_data = df[
        (df['driver_gender'] == driver_gender) &
        (df['driver_age'] == driver_age) &
        (df['search_conducted'] == int(search_conducted)) &
        (df['stop_duration'] == stop_duration) &
        (df['drugs_related_stop'] == drugs_related_stop) &
        (df['violation'] == violation) &
        (df['stop_outcome'] == stop_outcome)
    ]
    
    searching = "A search was conducted" if int(search_conducted) else "No search was conducted"
    drug_txt = "was drugs related" if int(drugs_related_stop) else "was not drug related"
    pronoun = "he" if driver_gender == "male" else "she"
    
    st.markdown(f"""
    🚗 : A {driver_age}-year-old {driver_gender} driver was stopped for **{violation}* at *{stop_time.strftime('%I:%M %p')}*.  
    {searching}, and {pronoun} received a *{stop_outcome}*.  
    The stop lasted *{stop_duration}* and {drug_txt}.
    """)
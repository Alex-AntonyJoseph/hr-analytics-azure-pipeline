import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# Page config
st.set_page_config(page_title="HR Analytics Dashboard", layout="wide", page_icon="📊")

# Database connection
@st.cache_resource
def get_connection():
    server = "hr-analytics-server.database.windows.net"
    database = "hr-analytics-db"
    username = "hradmin"
    password = st.secrets["sql_password"]
    
    connection_string = f"mssql+pymssql://{username}:{password}@{server}:1433/{database}"
    engine = create_engine(connection_string)
    return engine

engine = get_connection()

# Load data
@st.cache_data(ttl=3600)
def load_data():
    fact = pd.read_sql("SELECT * FROM fact_employee", engine)
    dept = pd.read_sql("SELECT * FROM dim_dept_attrition", engine)
    age = pd.read_sql("SELECT * FROM dim_age_attrition", engine)
    jobrole = pd.read_sql("SELECT * FROM dim_jobrole_income", engine)
    return fact, dept, age, jobrole

fact, dept, age, jobrole = load_data()

# ------ HEADER ------
st.title("📊 HR Analytics Dashboard")
st.caption("Employee attrition and compensation insights — built on Azure Data Lake, Databricks, and Azure SQL")

# ------ KPI CARDS ------
col1, col2, col3, col4 = st.columns(4)

total_employees = len(fact)
attrition_rate = round((fact["AttritionFlag"].sum() / total_employees) * 100, 2)
avg_income = round(fact["MonthlyIncome"].mean(), 0)
avg_tenure = round(fact["YearsAtCompany"].mean(), 1)

col1.metric("Total Employees", f"{total_employees:,}")
col2.metric("Attrition Rate", f"{attrition_rate}%")
col3.metric("Avg Monthly Income", f"${avg_income:,.0f}")
col4.metric("Avg Tenure", f"{avg_tenure} yrs")

st.divider()

# ------ CHARTS ------
col1, col2 = st.columns(2)

with col1:
    st.subheader("Attrition Rate by Department")
    fig1 = px.bar(
        dept.sort_values("AttritionRate", ascending=False),
        x="Department", y="AttritionRate",
        color="AttritionRate",
        color_continuous_scale="Reds",
        text="AttritionRate"
    )
    fig1.update_traces(texttemplate='%{text}%', textposition='outside')
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Attrition Rate by Age Group")
    age_order = ["Under 25", "25-34", "35-44", "45-54", "55+"]
    age["AgeGroup"] = pd.Categorical(age["AgeGroup"], categories=age_order, ordered=True)
    age_sorted = age.sort_values("AgeGroup")
    fig2 = px.bar(
        age_sorted,
        x="AgeGroup", y="AttritionRate",
        color="AttritionRate",
        color_continuous_scale="Oranges",
        text="AttritionRate"
    )
    fig2.update_traces(texttemplate='%{text}%', textposition='outside')
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("Average Monthly Income by Job Role")
jobrole_sorted = jobrole.sort_values("AvgMonthlyIncome", ascending=True)
fig3 = px.bar(
    jobrole_sorted,
    x="AvgMonthlyIncome", y="JobRole",
    orientation="h",
    color="AvgMonthlyIncome",
    color_continuous_scale="Blues",
    text="AvgMonthlyIncome"
)
fig3.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
st.plotly_chart(fig3, use_container_width=True)

st.divider()
st.caption("Data pipeline: ADLS2 → Databricks (PySpark) → Azure SQL → Streamlit | Orchestrated via Azure Data Factory")
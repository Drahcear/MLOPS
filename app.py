import streamlit as st
from utils import *
from datetime import date
import plotly.express as px

df = get_dataframe()

st.title("Analytics")
start = st.date_input(
    "Start date",
    date(2022,12,7))

end = st.date_input(
    "End date",
    date(2022,12,8))

total_count, dates, true_count, false_count = from_range(df, str(start), str(end))
st.write('Total transaction: ', total_count)

fig = px.bar(x=dates, y=[true_count, false_count], barmode="group", labels={'x':'Date', 'value':'Count'}, title="Transaction by day")
newnames = {"wide_variable_0":"True", "wide_variable_1":"False"}
fig.for_each_trace(lambda t: t.update(name = newnames[t.name]))
st.plotly_chart(fig)
st.plotly_chart(plot_per_coutry(df, ""))

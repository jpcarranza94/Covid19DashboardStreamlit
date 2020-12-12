import streamlit as st
import pandas as pd
#import sqlalchemy

## importing 'mysql.connector' as mysql for convenient
import mysql.connector as mysql

import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import matplotlib.pyplot as plt

import numpy as np

import pydeck as pdk 
import altair as alt 
from datetime import datetime
from datetime import timedelta
import time 

st.title("Product Development: Project")


## connecting to the database using 'connect()' method
## it takes 3 required parameters 'host', 'user', 'passwd'
db = mysql.connect(
    host = "db",
    user = "test",
    passwd = "test123",
    database = "covid"
)

print(db) # it will print a connection object if everything is fine

cursor = db.cursor()

## defining the Query
query = "select lat as latitude, `long` as longitude, c_cases as Confirmed, r_cases as Recovered, d_cases as Deaths, concat(country_region,' ',province_state) as State from cases_data, (select max(event_date) as fechaMaxima from cases_data)A where event_date=A.fechaMaxima order by c_cases desc"

## getting records from the table
cursor.execute(query)

## fetching all records from the 'cursor' object
df = pd.DataFrame(cursor.fetchall(), columns=['latitude', 'longitude', 'Confirmed', 'Recovered', 'Deaths', 'State'])

## cases_data
query2 = "select lat as latitude, `long` as longitude, c_cases as Confirmed, r_cases as Recovered, d_cases as Deaths, concat(country_region,' ',province_state) as State, event_date as fecha from cases_data"

## getting records from the table
cursor.execute(query2)

## fetching all records from the 'cursor' object
df2 = pd.DataFrame(cursor.fetchall(), columns=['latitude', 'longitude', 'Confirmados', 'Recuperados', 'Muertes', 'State', 'Fecha'])

query3 = "select lat as latitude, `long` as longitude, mortality_rate as 'tasa_de_letalidad', recovery_rate as 'tasa_de_recuperados',concat(country_region,' ',province_state) as State, event_date as fecha from cases_data"

cursor.execute(query3)

df3 = pd.DataFrame(cursor.fetchall(), columns=['latitude', 'longitude', 'tasa_de_letalidad', 'tasa_de_recuperados', 'State', 'Fecha'])

df['latitude']=pd.to_numeric(df['latitude'])#df['latitude'].astype(int)#
df['longitude']=pd.to_numeric(df['longitude'])#df['longitude'].astype(int)#
df.round(8)
#df2.round(8)
df2['Fecha']=pd.to_datetime(df2['Fecha']).dt.strftime('%Y-%m-%d')
df3['Fecha']=pd.to_datetime(df3['Fecha']).dt.strftime('%Y-%m-%d')

#df2['Fecha']=pd.to_datetime(df2['Fecha'])

#df_sql_data = pd.DataFrame(cur.fetchall())

## Showing the data
#for record in records:
   # print(record)
    # record


st.markdown('Este dashboard permitirá visualizar la situación del COVID-19')
st.markdown('En esta primera tabla podrá visualizar los casos a la fecha máxima cargada')
df[['State', 'latitude', 'longitude', 'Confirmed', 'Recovered', 'Deaths']]

st.markdown('El coronavirus o COVID-19, es una enfermedad infecciosa causada por un coronavirus descubierto recientemente. La mayoría de personas afectadas por el virus experimentarán síntomas moderados. Sin embargo, la enfermedad puede presentar formas graves en pacientes con comorbilidades o edades avanzadas.')
st.sidebar.title("Selector de Visualización")
st.sidebar.markdown("Selecciona los Charts/Plots según corresponda:")
select = st.sidebar.selectbox('Tipo de Visualización', ['Barras', 'Pie'], key='1')
if not st.sidebar.checkbox("Ocultar top 5", True, key='1'):
    if select == 'Pie':
        st.title("Top 5 de países con mas casos confirmados")
        fig = px.pie(df, values=df['Confirmed'][:5], names=df['State'][:5], title='Casos Confirmados Totales')
        st.plotly_chart(fig)

        st.title("Top 5 de países con menos casos confirmados")
        fig2 = px.pie(df, values=df['Confirmed'][-5:], names=df['State'][-5:], title='Casos Confirmados Totales')
        st.plotly_chart(fig2)

    if select=='Barras':
        st.title("Top 5 de países con mas casos confirmados")
        fig = go.Figure(data=[
        go.Bar(name='Confirmed', x=df['State'][:5], y=df['Confirmed'][:5]),
        go.Bar(name='Recovered', x=df['State'][:5], y=df['Recovered'][:5]),
        go.Bar(name='Active', x=df['State'][:5], y=df['Deaths'][:5])])
        st.plotly_chart(fig)

        st.title("Top 5 de países con menos casos confirmados")
        fig2 = go.Figure(data=[
        go.Bar(name='Confirmed', x=df['State'][-5:], y=df['Confirmed'][-5:]),
        go.Bar(name='Recovered', x=df['State'][-5:], y=df['Recovered'][-5:]),
        go.Bar(name='Active', x=df['State'][-5:], y=df['Deaths'][-5:])])
        st.plotly_chart(fig2)


st.subheader('Mapa de casos mundiales a la última fecha en sistema')
map_data = pd.DataFrame(
    df.loc[: ,'latitude':'longitude'],
    columns=['lat', 'lon'])

st.map(df)


# Filters UI
subset_data = df2
subset_data_2 = df3
country_name_input = st.sidebar.multiselect(
'Nombre País:',
df2.groupby('State').count().reset_index()['State'].tolist())
# by country name
if len(country_name_input) > 0:
    subset_data = df2[df2['State'].isin(country_name_input)]
    subset_data_2 = df3[df3['State'].isin(country_name_input)]    

metrics =['Confirmados','Recuperados','Muertes']
cols = st.sidebar.selectbox('Métrica a visualizar', metrics)
# let's ask the user which column should be used as Index
if cols in metrics:   
    metric_to_show_in_covid_Layer = cols


filter_data = subset_data[subset_data['Fecha'] >='2020-01-01'].set_index("Fecha") 
#st.markdown(str(','.join(country_name_input)) + " casos de "+cols.lower()+" desde el 1 de Enero del 2020")
# bar chart 
#st.bar_chart(filter_data[cols])

## linechart



st.subheader('Comparación de tasa de crecimiento')

filter_data.reset_index(inplace = True)

initial_date = filter_data.Fecha.min()
final_date = filter_data.Fecha.max()

initial_date = datetime.strptime(initial_date,'%Y-%m-%d').date()
final_date = datetime.strptime(final_date,'%Y-%m-%d').date()

start_date = st.date_input('Fecha inicial', initial_date)
end_date = st.date_input('Fecha final', final_date)

subset_data['Fecha']=pd.to_datetime(subset_data['Fecha'], format = '%Y-%m-%d').dt.date

subset_data = subset_data[((subset_data['Fecha']>= start_date)&(subset_data['Fecha'] <= end_date))]


total_cases_graph  =alt.Chart(subset_data).mark_line().encode(
    x=alt.X('Fecha', type='nominal', title='Fecha'),
    y=alt.Y('sum(Confirmados):Q',  title='Casos confirmados'),
    color='State',
    tooltip = 'sum(Confirmados)',
).properties(
    width=800,
    height=600
).configure_axis(
    labelFontSize=17,
    titleFontSize=20,
    labelOverlap="greedy",
    labelSeparation=1
)

st.altair_chart(total_cases_graph)


subset_data_2['Fecha']=pd.to_datetime(subset_data_2['Fecha'], format = '%Y-%m-%d').dt.date

subset_data_2 = subset_data_2[((subset_data_2['Fecha']>= start_date)&(subset_data_2['Fecha'] <= end_date))]


subset_data_2 = subset_data_2.fillna(0)

#subset_data_2.tasa_de_letalidad * 100 = subset_data_2.tasa_de_letalidad

st.subheader('Comparacion de tasa de letalidad')

letality_graph  =alt.Chart(subset_data_2).mark_line().encode(
    x=alt.X('Fecha', type='nominal', title='Fecha'),
    y=alt.Y('tasa_de_letalidad',  title='Tasa de letalidad'),
    color='State',
    tooltip = alt.Tooltip(['State', 'tasa_de_letalidad']),
).properties(
    width=800,
    height=600
).configure_axis(
    labelFontSize=17,
    titleFontSize=20,
    labelOverlap="greedy",
    labelSeparation=1
)

st.altair_chart(letality_graph)

st.subheader('Comparacion de tasa de recuperación')

recovery_graph  =alt.Chart(subset_data_2).mark_line().encode(
    x=alt.X('Fecha', type='nominal', title='Fecha'),
    y=alt.Y('tasa_de_recuperados',  title='Tasa de recuperados'),
    color='State',
    tooltip = alt.Tooltip(['State', 'tasa_de_recuperados']),
).properties(
    width=800,
    height=600
).configure_axis(
    labelFontSize=17,
    titleFontSize=20,
    labelOverlap="greedy",
    labelSeparation=1
)

st.altair_chart(recovery_graph)


 
## MAP

# Variable for date picker, default to Jan 1st 2020
#date = datetime.date(2020,1,1)

# Set viewport for the deckgl map
view = pdk.ViewState(latitude=0, longitude=0, zoom=0.2)
# Create the scatter plot layer


# df = pd.DataFrame(
#    np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
#    columns=['lat', 'lon'])


filter_data['latitude']=pd.to_numeric(filter_data['latitude'])#df['latitude'].astype(int)#
filter_data['longitude']=pd.to_numeric(filter_data['longitude'])

dictionary_radius = {'Confirmados': [0.25,0.1,100,[254,186,41],[255,98,1]], 'Recuperados': [0.75, 0.1, 100,[174,218,73],[0,255,0]],'Muertes': [5, 2.5, 100,[255,0,0],[187,0,59]]}

args_list_radius = dictionary_radius.get(metric_to_show_in_covid_Layer)



covidLayer = pdk.Layer(
    "ScatterplotLayer",
    data=filter_data,
    pickable=False,
    opacity=0.1,
    stroked=True,
    filled=True,
    radius_scale = args_list_radius[0],
    radius_min_pixels=args_list_radius[1],
    radius_max_pixels=args_list_radius[2],
    line_width_min_pixels=1,
    get_position=["longitude", "latitude"],
    get_radius=metric_to_show_in_covid_Layer,
    get_fill_color=args_list_radius[3],
    get_line_color=args_list_radius[4],
    tooltip = True
)

# Create the deck.gl map
r = pdk.Deck(
    layers=covidLayer,
    initial_view_state=view,
    map_style="mapbox://styles/mapbox/light-v10"
)
# Create a subheading to display current date
subheading = st.subheader("")
# Render the deck.gl map in the Streamlit app as a Pydeck chart 
map = st.pydeck_chart(r)
# Update the maps and the subheading each day for 90 days



date_str = '01-01-2020'

date = datetime.strptime(date_str, '%m-%d-%Y').date()


initial_date = filter_data.Fecha.min()
final_date = filter_data.Fecha.max()

initial_date = datetime.strptime(initial_date,'%Y-%m-%d').date()
final_date = datetime.strptime(final_date,'%Y-%m-%d').date()

days = (final_date - initial_date).days


for i in range(0, days, 1):
    # Increment day by 1
    initial_date += timedelta(days=1)
    # Update data in map layers
    covidLayer.data = filter_data[filter_data['Fecha'] == initial_date.isoformat()]
    # Update the deck.gl map
    r.update()
    # Render the map
    map.pydeck_chart(r)
    # Update the heading with current date
    subheading.subheader("%s on : %s" % (metric_to_show_in_covid_Layer, initial_date.strftime("%B %d, %Y")))
    
# wait 0.1 second before go onto next day
    time.sleep(0.05)

st.sidebar.subheader("Integrantes del grupo")
st.sidebar.markdown("José Sebastián Rodríguez Velásquez")
st.sidebar.markdown("Diego Fernando Valle Morales")
st.sidebar.markdown("Juan Pablo Carranza Hurtado")
st.sidebar.markdown("José Alberto Ligorría Taracena")
from dash import Dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from dash.dependencies import Input, Output
from datetime import timedelta
from flask import Flask
from sqlalchemy import create_engine


# Initialize the app
server = Flask(__name__)
app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
app.config.suppress_callback_exceptions = True

# Read data

# dialect = 'mysql+pymysql://root:Bigdata2122@192.168.193.133:3306/scrapping'
dialect = 'mysql+pymysql://root@localhost:3306/scrapping_teatre'

sqlEngine = create_engine(dialect)
df_obres = pd.read_sql('ines_obres', con=sqlEngine)
df_cat = pd.read_sql('ines_categories', con=sqlEngine)
df_preus = pd.read_sql('ines_preus', con=sqlEngine)


# Layout components

navbar = dbc.NavbarSimple(
    brand=("Teatre Principal Inca"),
    color="#0c618f",
    dark=True,
    style={'marginBottom': '20px'}
)

footer = dbc.NavbarSimple(
    children=[html.Div("© Copyright Ines", style={'color': 'white'})],
    brand="Ines Arrom",
    color="#0c618f",
    dark=True,
    style={'marginTop': '20px'}
)

df_merge = pd.merge(df_obres, df_cat, on=["titol"])
table = dbc.Table.from_dataframe(
    df_merge, striped=True, bordered=True, hover=True)

# Graph
labels = df_cat['categoria'].unique()
values = df_cat.groupby('categoria').categoria.count()

fig = {
    'data': [go.Pie(labels=labels, values=values)],
    'layout': go.Layout(
        title="Tipus d'obres",
        colorway=["#5E0DAC", '#FF4F00', '#375CB1',
                  '#FF7400', '#FFF400', '#FF0056'],
        plot_bgcolor='rgba(100, 100, 100, 100)',
    )
}


# App Layout
app.layout = html.Div(children=[
    html.Div(navbar),
    html.Div(dcc.Graph(id='graph_types', figure=fig)),
    html.Div(table, style={'margin': '20px'}),
    html.Div(footer),
])


if __name__ == '__main__':
    app.run_server(debug=True)

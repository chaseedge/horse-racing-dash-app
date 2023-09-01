import dash, dash_table
import pandas as pd
from dash import html, dcc
from utils.database import Database
import dash_bootstrap_components as dbc

# this registers that page that's accessible on 
dash.register_page(__name__, path='/races')

db = Database()

def get_races(tracks=None):
    params = {}
    sql = """SELECT * FROM tvg.races"""
    if tracks:
        sql += " WHERE track_id=ANY(%(tracks)s)"
        params['tracks'] = tracks
    data = db.query(sql, params, as_df=False)
        
    return pd.DataFrame(data)

races = get_races()
cols = db.get_cols('races')

layout = html.Div(children=[
    html.H1(children='Races for today'),

    html.Div(children=[
        dbc.Table.from_dataframe(races, striped=True, bordered=True, hover=True)
        ]),
])
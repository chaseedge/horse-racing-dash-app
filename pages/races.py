import dash, dash_table
from dash import html, dcc
from utils.database import Database

dash.register_page(__name__, path='/races')

db = Database()

def get_races(tracks=None):
    params = {}
    sql = """SELECT * FROM races"""
    if tracks:
        sql += " WHERE track_id=ANY(%(tracks)s)"
        params['tracks'] = tracks
    data = db.query(sql, params, as_df=False)
        
    return data

races = get_races()
cols = db.get_cols('races')

layout = html.Div(children=[
    html.H1(children='Races for today'),

    html.Div(children=[
        dash_table.DataTable(races, [{"name": c, "id": c} for c in cols])
        ]),
])
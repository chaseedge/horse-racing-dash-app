import pprint
import pandas as pd
import plotly.express as px
import dash, dash_table
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, callback
from utils.database import Database


dash.register_page(__name__, path='/stable')

APP_NAME = 'stable'
_id = lambda x: f"{APP_NAME}-{x}"

db = Database()

def get_horses(tracks=None):
    params = {}
    sql = """SELECT horse_id, horse_name, foaling_date, sex FROM tvg.horses"""
    if tracks:
        sql += " WHERE track_id=ANY(%(tracks)s)"
        params['tracks'] = tracks
    data = db.query(sql, params, as_df=False)
        
    return data

horses = get_horses()
cols = ["horse_id", "horse_name", "foaling_date", "sex"]
# cols = db.get_cols('horses')

horse_table_id = _id('horses')
sex_pie_chart_id = _id('sex-pie-chart')
add_horse_id = _id("add-horse")

cnts = pd.DataFrame(horses).sex.value_counts().to_frame().reset_index(drop=False)

# tabs
layout = html.Div([
    dcc.Tabs([
        dcc.Tab(label='Stable', children=[
                html.Div(children=[
                    html.Div([dbc.Button("Primary", color="primary", className="me-1", id=add_horse_id, style={'maxWidth': 100}),], className='row'),
                    html.Div(dash_table.DataTable(horses,
                                            [{"name": c, "id": c} for c in cols], 
                                            editable=True,
                                            id=horse_table_id),
                                className="table table-striped table-hover"),
                    dcc.Graph(figure=px.pie(cnts, values='count', names='sex', title='Gender Breakdown'), id=sex_pie_chart_id)
    ])])] + [
        dcc.Tab(label=h['horse_name'], children=[
            # just dump out the horse dict
            html.Div(pprint.pformat(h, indent=4), style={"white-space": "pre-wrap", "margin": "15px 20%"})
        ]) for h in horses])
])
    

@callback(
    Output(sex_pie_chart_id, 'figure'),
    Input(horse_table_id, 'data'),
    Input(horse_table_id, 'columns'))
def display_output(rows, columns):
    df = pd.DataFrame(rows, columns=[c['name'] for c in columns])
    db.upsert_df(df,  'tvg.horses')
    df = df.sex.value_counts().to_frame().reset_index(drop=False)
    return px.pie(df, values='count', names='sex', title='Gender Breakdown')
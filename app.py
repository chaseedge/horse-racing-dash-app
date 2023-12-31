import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc


app = Dash(__name__,
           use_pages=True, 
           external_stylesheets=[dbc.themes.BOOTSTRAP])

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink(page['name'], href=page["relative_path"]))
        for page in dash.page_registry.values()
    ],
    brand="Big Haus Racing",
    color="primary",
    dark=True,
    brand_href="/"
)

app.layout = html.Div([
    navbar,
    dash.page_container
])

if __name__ == '__main__':
	app.run(debug=True)
import dash
import dash_bootstrap_components as dbc

navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink(page['name'], href=page["relative_path"]))
        for page in dash.page_registry.values()
    ],
    brand="Big Haus Racing",
    color="primary",
    dark=True,
)
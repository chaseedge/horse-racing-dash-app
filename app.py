import os
import time
import datetime, pytz
import flask
import dash
from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from etl.get_races import update_scheduled_races

server = flask.Flask(__name__)

app = Dash(__name__,
           server=server,
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

def check_if_not_debug_thread():
    """dash/flask runs on two threads for debug b/c of hot reload
    so this checks if it's not debug or if it's the main thread.
    """
    return not app.server.debug or os.environ.get('WERKZEUG_RUN_MAIN')=='true'

def my_cron_job():
    # debug will run the scheduler twice, so you need to check if it's debug mode so it doesn't run the functions twice
    if check_if_not_debug_thread(): 
        now = datetime.datetime.utcnow().astimezone(pytz.timezone("EST"))
        print(f"running {now}")
        # Code to be executed by the cron job
        with open("test-job.txt", "a") as f:
            # just write the current date to a file so we know it's working
            f.write(f"{now}\n")
     
    
if __name__ == '__main__':    
    
    scheduler = BackgroundScheduler() 
    
    # https://apscheduler.readthedocs.io/en/3.x/modules/triggers/cron.html
    # Schedule the cron job to run every minute a the 15 seconds
    scheduler.add_job(func=my_cron_job, 
                        trigger=CronTrigger(second="0,15,30,45"),
                        max_instances=1)
    
    # Schedule the cron job to run at 9, 12 , 15, 18 at eastern timezon
    # scheduler.add_job(func=my_cron_job, 
    #                     trigger=CronTrigger(hour="9,12,15,18", timezone=pytz.timezone("EST")),
    #                     max_instances=1)
    
    # Start the scheduler
    scheduler.start() 
        
    app.run(debug=True)
    


import sys
import requests
from prefect import flow
from prefect import flow, get_run_logger, task
from prefect.deployments.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

try:
    from utils.database import Database
except ModuleNotFoundError:    
    sys.path.append("..")
    from utils.database import Database



@task
def get_tvg_race_schedule():
    """get race schedule from tvg"""
    
    headers = {
        'authority': 'service.tvg.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.5',
        'content-type': 'application/json',
        'dnt': '1',
        'origin': 'https://www.tvg.com',
        'referer': 'https://www.tvg.com/',
        'sec-ch-ua': '"Not/A)Brand";v="99", "Brave";v="115", "Chromium";v="115"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'sec-gpc': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    }

    json_data = {
        'query': 'query getFullScheduleRaces($wagerProfile: String, $sortBy: RaceListSort, $filterBy: RaceListFilter, $pagination: Pagination) {\n  races(sort: $sortBy, filter: $filterBy, profile: $wagerProfile, page: $pagination) {\n    number\n    distance\n    numRunners\n    postTime\n    mtp\n    isGreyhound\n    track {\n      code\n      name\n      featured\n      perfAbbr\n      location {\n        country\n        __typename\n      }\n      __typename\n    }\n    raceClass {\n      id\n      name\n      __typename\n    }\n    surface {\n      id\n      code\n      name\n      __typename\n    }\n    video {\n      onTvg\n      onTvg2\n      liveStreaming\n      hasReplay\n      streams\n      replays\n      __typename\n    }\n    __typename\n  }\n}\n',
        'variables': {
            'wagerProfile': 'PORT-NY',
            'filterBy': {
                'hasMTP': True,
                'isOpen': True,
            },
            'sortBy': {
                'byPostTime': 'ASC',
            },
        },
        'operationName': 'getFullScheduleRaces',
    }
    logger = get_run_logger()
    logger.info("Fetching races from tvg")
    r = requests.post('https://service.tvg.com/graph/v2/query', headers=headers, json=json_data)
    r.raise_for_status()
    return r.json()



@task
def parse_tvg_race_schedule(data):
    logger = get_run_logger()
    races = []
    for d in data.get('data',{}).get('races',[]):
        r = {'race_number': d['number'],
             'distance': d['distance'],
             'num_runners': d['numRunners'],
             'race_date': d['postTime'][:10],
             'post_time': d['postTime'],
             'track_id': f"{d['track']['location']['country']}_{d['track']['code']}",
             "surface": d['surface']['name'].lower(),
             "race_class": d['raceClass']['name'].lower()}
        r['race_id'] = f"{r['track_id']}_{r['race_date']}_{r['race_number']}"
        races.append(r)
    logger.info(f"{len(races)} races parsed")
    return races
    

@flow(retries=3, retry_delay_seconds=60)
def update_scheduled_races():
    races = get_tvg_race_schedule()
    races = parse_tvg_race_schedule(races)
    db = Database()
    db.upsert(races, table="tvg.races", pkeys=['race_id'])
   

if __name__ == "__main__":
    
    update_scheduled_races()
    # Deployment.build_from_flow(
    #     update_scheduled_races,
    #     schedule=(CronSchedule(cron="0 0 * * *", timezone="America/New_York"))
    # )
    





import requests
import pandas as pd
import numpy as np
import os
from functools import wraps
from pydantic import BaseModel
import time

class PlayDF(BaseModel):
    """
    Represents the DataFrame structure for play-by-play data.
    
    """
    game_id: int
    play_id: int
    home_id: int
    home_team: object
    home_rank: float
    away_id: int
    away_team: object
    away_rank: float
    season_type: object
    week: int
    posteam: object
    posteam_type: object
    defteam: object
    yardline_100: float
    year: int
    quarter: float
    clock: object
    game_seconds_remaining: float
    half: object
    down: float
    ydstogo: float
    play_type: object
    yds_gained: float
    description: object
    home_timeouts: float
    away_timeouts: float
    posteam_tos: float
    defteam_tos: float

def with_default_values(func):
    """
    Decorator that provides default values for specific parameters in the decorated function.
    
    Args:
        func (callible): The original function to be decorated.
    
    Returns:
        callible: The decorated function with default values for certain parameters.
    
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        defaults = {'_home_team': None, '_hid': None, '_away_team': None, '_aid': None, 'h_color' : None, 'a_color' : None, 'h_rank' : None, 'a_rank' : None, 'h_logo' : None, 'a_logo' : None, 'h_location' : None, 'a_location' : None}
        defaults.update(kwargs)
        return func(*args, **defaults)
    return wrapper
        
@with_default_values
def get_home_away(_json, _home_team, _hid, _away_team, _aid, h_color, a_color, h_rank, a_rank, h_logo, a_logo, h_location, a_location):
    """ 
    Assigns the home and away team based on information passed in JSON input.
    The values default to None with the decorator with_default_values.
    
    Args:
        _json (dict): JSON object containing information about the sports event.
        _home_team (object): Default value for the home team abbreviation.
        _hid (int): Default value for the home team ID.
        _away_team (object): Default value for the away team abbreviation.
        _aid (int): Default value for the away team ID.
        h_color (object): Default value for the home team color.
        a_color (object): Default value for the away team color.
        h_rank (int): Default value for the home team rank.
        a_rank (int): Default value for the away team rank.
        h_logo (object): Default value for the home team logo URL.
        a_logo (object): Default value for the away team logo URL.
        h_location (object): Default value for the home team location.
        a_location (object): Default value for the away team location.

    Returns:
        tuple: Tuple containing values assigned to the provided parameters based on 
        the JSON data.
    
    Raises:
        Exception: This is usually a NonType. This is used to skip the exctraction.
    
    """
    try:
        for i in range(2):
            if _json[i].get('homeAway') == 'home':
                _home_team = _json[i].get('team')['abbreviation']
                _hid = _json[i].get('team')['id']
                h_color = _json[i].get('team').get('color') 
                h_rank = _json[i]['rank'] if 'rank' in _json[i] else np.nan
                h_logo = _json[i].get('team').get('logos')[0].get('href')
                h_location = _json[i].get('team')['location']
            else:
                _away_team = _json[i].get('team')['abbreviation']
                _aid = _json[i].get('team')['id']
                a_color = _json[i].get('team').get('color')
                a_rank = _json[i]['rank'] if 'rank' in _json[i] else np.nan
                a_logo = _json[i].get('team').get('logos')[0].get('href')
                a_location = _json[i].get('team')['location']
    
    except Exception as e:
        print(f'Error: {e}')

    return _home_team, _hid, _away_team, _aid, h_color, a_color, h_rank, a_rank, h_logo, a_logo, h_location, a_location

# Helper funcitons
def convert_clock_to_seconds(clock, quarter) -> float:
    """
    Converts the clock time to seconds based each quarter (e.g., 15:00 in Q1 to 3600).
    
    Args:
        clock (object): The Clock time format MM:SS.
        quarter (float): Quarter number in the game.
    
    Returns:
        float: The total elapsed seconds in the game.
    
    """
    if clock is None:
        return None
    minutes, seconds = map(int, clock.split(':'))
    quarter_seconds = minutes * 60 + seconds
    total_seconds = (4 - quarter if quarter <= 4 else 0) * 15 * 60
    return quarter_seconds + total_seconds

def timeout_decrementor(row, df, location) -> pd.Series:
    """
    Manually adjusts the timeout counts based on the play information and game state.
    
    Args:
        row (pandas.Series): DataFrame row containing play information.
        df (pandas.DataFrame): DataFrame containing timeout information.
        location (object): Used to help compare the timeout calling team.
    
    Returns:
        pandas.Series: Updated home and away timeouts.
    
    """
    if row['play_type'] == 'Timeout' and row['description'] is not None:
        if location in row['description'] or location.upper() in row['description']:
            df['home_timeouts'] -= 1
        else:
            df['away_timeouts'] -= 1
    if row['game_seconds_remaining'] == 1800:
        df['home_timeouts'] = 3
        df['away_timeouts'] = 3
    
    if row['game_seconds_remaining'] == 0 and 'OT' in row['half']:
        df['home_timeouts'] = 1
        df['away_timeouts'] = 1
        if row['play_type'] == 'Timeout':
            if location in row['description'] or location.upper() in row['description'] and row['description'] is not None:
                df['home_timeouts'] -= 1
            else:
                df['away_timeouts'] -= 1

    return pd.Series({
        'home_timeouts': df['home_timeouts'],
        'away_timeouts': df['away_timeouts'],
    })



def get_play_by_play(game_id) -> pd.DataFrame:
    """
    Retrieves the play-by-play data for a game given game ID. Uses a 1 second sleep to 
    give endpoint time to load.
    
    Args:
        game_id (int): Identifier for the event.
    
    Returns:
        pandas.DataFrame: DataFrame containing play-by-play data.
    
    Raises:
        Attribute Error: If data cannot be found for specific game id, it is skipped.
    
    """
    url = f'http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={game_id}'
    response = requests.get(url)
    json = response.json()

    
    time.sleep(1)
    
    try:
        home_team, home_id, away_team, away_id, home_color, away_color, home_rank, away_rank, home_logo, away_logo, h_location, a_location = get_home_away(json.get('header').get('competitions')[0].get('competitors'))
    except AttributeError:
        print('Skip this. No data available.')
    
    game_df = pd.DataFrame()

    play_ids = []
    home_score = []
    away_score = []
    desc = []
    posteam = []
    play_type = []
    quarter = []
    clock = []
    down = []
    ydstogo = []
    yardline_100 = []
    posteam = []
    ball_location = []
    yards_gained = []


    # Gets the main play by play data
    # Some games don't have play by play so they will be skipped
    try: 
        for drive in json.get('drives').get('previous'):
            for play in drive.get('plays'):
                play_ids.append(play['id'])
                home_score.append(play['homeScore'])
                away_score.append(play['awayScore'])
                desc.append(play['text'] if 'text' in play else 'N/A')
                play_type.append(play.get('type').get('text') if play.get('type') is not None else 'N/A')
                quarter.append(play['period'].get('number'))
                clock.append(play['clock'].get('displayValue'))
                down.append(play['start'].get('down'))
                ydstogo.append(play['start'].get('distance'))
                yardline_100.append(play['start'].get('yardsToEndzone'))
                posteam.append(play['start'].get('team').get('id'))
                ball_location.append(play['start'].get('possessionText'))
                yards_gained.append(play['statYardage'])

        game_id = json.get('header').get('id')
        year = json.get('header').get('season').get('year')
        season_type = json.get('header').get('season').get('type')
        is_neutral = json.get('header').get('competitions')[0]['neutralSite']
        is_conference_play = json.get('header').get('competitions')[0]['conferenceCompetition']
        week = json.get('header').get('week')


        # Time to make a dataframe:
        game_df['game_id'] = [game_id] * len(play_ids)
        game_df['play_id'] = play_ids
        game_df['home_id'] = home_id
        game_df['home_team'] = home_team
        game_df['home_rank'] = home_rank
        game_df['away_id'] = away_id
        game_df['away_team'] = away_team
        game_df['away_rank'] = away_rank
        game_df['season_type'] = 'regular' if season_type == 2 else 'bowl'
        game_df['week'] = week
        game_df['posteam'] = posteam
        game_df['posteam_type'] = game_df.apply(lambda row: 'home' if row['posteam'] == row['home_id'] else 'away' if row['posteam'] == row['away_id'] else 'neutral', axis=1)
        game_df['defteam'] = game_df.apply(lambda row: away_id if row['posteam'] == row['home_id'] else home_id, axis=1)
        game_df['yardline_100'] = yardline_100
        game_df['year'] = year
        game_df['quarter'] = quarter
        game_df['clock'] = clock
        game_df['game_seconds_remaining'] = game_df.apply(lambda row: convert_clock_to_seconds(row['clock'], row['quarter']), axis=1)

        game_df['half'] = game_df['quarter'].apply(lambda x: 'Half1' if x in [1, 2] else 'Half2' if x in [3, 4] else f'OT{x}')
        game_df['down'] = [down if down != 0 else np.nan for down in down]
        game_df['ydstogo'] = ydstogo
        game_df['play_type'] = play_type
        game_df['yds_gained'] = yards_gained
        game_df['description'] = desc

        timeout_state = {'home_timeouts': 3,'away_timeouts': 3,} 
        game_df[['home_timeouts', 'away_timeouts']] = game_df.apply(lambda row: timeout_decrementor(row, timeout_state, h_location), axis=1)
        game_df['posteam_tos'] = game_df.apply(lambda row: row['home_timeouts'] if row['posteam'] == row['home_id'] else row['away_timeouts'], axis=1)
        game_df['defteam_tos'] = game_df.apply(lambda row: row['home_timeouts'] if row['defteam'] == row['home_id'] else row['away_timeouts'], axis=1)

    except AttributeError:
        print('No play by play for game')
    
    return game_df

def get_teams(game_id) -> pd.DataFrame:
    """
    Retrieves team information for a given game ID.

    Args:
        game_id (int): Identifier for the sports event.

    Returns:
        pandas.DataFrame: DataFrame containing team information for the specified game.
    
    """
    url = f'http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={game_id}'
    response = requests.get(url)
    json = response.json()
    
    try:
        home_team, home_id, away_team, away_id, home_color, away_color, home_rank, away_rank, home_logo, away_logo, h_location, a_location = get_home_away(json.get('header').get('competitions')[0].get('competitors'))
    except AttributeError:
        print('Skip this. No data available.')
    
    teams = pd.DataFrame(columns=['team_name', 'team_id', 'color', 'logo'])

    if home_team not in teams['team_name'].values:
        teams = pd.concat([teams, pd.DataFrame({'team_location' : [h_location], 'team_name': [home_team], 'team_id': [home_id], 'color': [f'#{home_color}'], 'logo': [home_logo]})], ignore_index=True)

    if away_team not in teams['team_name'].values:
        teams = pd.concat([teams, pd.DataFrame({'team_location' : [a_location], 'team_name': [away_team], 'team_id': [away_id], 'color': [f'#{away_color}'], 'logo': [away_logo]})], ignore_index=True)
        
    return teams

def extract_season(game_ids, start_date, current_dates, end_date):
    """
    Extracts play-by-play for each season.
    
    Args:
        game_ids (list): List of the game_ids.
        start_date (int): Start date of extraction year.
        current_dates (list): List of dates corresponding to the game IDs.
        end_date (int): End date of extraction year.
        
    Returns:
        None
    
    """
    pbp = pd.DataFrame()
    teams = pd.DataFrame()
    counter = 0
    for game, date in zip(game_ids, current_dates):
        if start_date <= date <= end_date:
            counter += 1
            print(f'Getting Game: http://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event={game}')
            print(f'{counter}')
            try:
                pbp = pbp._append(get_play_by_play(game), ignore_index = True)
                teams = teams._append(get_teams(game), ignore_index = True)
            except TypeError:
                print('Gonna skip this one since it probably doesn\'t need to be there.')
    
    # Remove unecessary rows
    pbp = pbp[~pbp['description'].str.contains('End')]
    
    year = str(start_date)[:4]
    
    pbp.to_csv(f'{year}play_by_play.csv', index=False)

    teams.to_csv('teams.csv', index=False)

csv = pd.read_csv('game_ids_since_2014.csv') 
game_ids = list(csv['GameID'])
dates = list(csv['Date'])

start_date = 20230801  # Adjust as needed
end_date = 20240131    # Adjust as needed

extract_season(game_ids, start_date, dates, end_date)

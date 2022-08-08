import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import numpy as np
from datetime import date, timedelta
import psycopg2 as ps
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import config

pd.options.mode.chained_assignment = None

# saving webpage to file
url = "https://www.baseball-reference.com/leagues/daily.fcgi?type=p"
r = requests.get(url)
with open('webpages/baseball.html', "w+", encoding='utf-8') as f:
    f.write(r.text)
with open('webpages/baseball.html', encoding='utf_8') as f:
    baseball = f.read()

# using BeautifulSoup to parse through webpage to get to table containing yesterday's pitchers
soup = BeautifulSoup(baseball, "html.parser")
first = soup.find('div', id="div_daily")
soup2 = BeautifulSoup(str(first), 'html.parser')
tbody = soup2.find('tbody')
rows = tbody.find_all('tr')

# creating date for yesterday (when games were played) so I can add it with the rest of the values
today = date.today()
yesterday = today - timedelta(days=1)

# need to loop through all of the rows in the table and individually take out values and add them to a DataFrame
irow = 0
columns = ['name', 'team', 'league', 'opp', 'away', 'gs', 'w', 'l',
           'sv', 'ip', 'h', 'r', 'er', 'bb', 'k', 'hr', 'hbp', 'bf', 'pit']
yesterday_pitchers = pd.DataFrame(columns=columns)
for row in rows:
    cols = row.find_all('td')
    if (len(cols) >= 1):
        yesterday_pitchers.loc[irow] = [
            cols[0].text.strip(),  # name
            cols[5].text.strip(),  # team
            cols[4].text.strip()[-2:],  # league
            cols[7].text.strip(),  # opponent
            cols[6].text,  # away
            cols[8].text.strip(),  # game started
            cols[9].text.strip(),  # win
            cols[10].text.strip(),  # loss
            cols[11].text.strip(),  # save
            cols[12].text.strip(),  # innings pitched
            cols[13].text.strip(),  # hits
            cols[14].text.strip(),  # runs
            cols[15].text.strip(),  # earned runs
            cols[16].text.strip(),  # base on balls
            cols[17].text.strip(),  # strikeouts
            cols[18].text.strip(),  # homeruns
            cols[19].text.strip(),  # hit by pitch
            cols[30].text.strip(),  # batters faced
            cols[31].text.strip(),  # pitches
        ]
    irow = irow+1

# dictionaries to distinguish teams based on 'team' and 'league'. pairs with the 'make_abbvs()' function.
nl_abbreviation = {
    'Milwaukee': 'MIL',
    'Atlanta': 'ATL',
    'San Diego': 'SD',
    'San Francisco': 'SF',
    'Pittsburgh': 'PIT',
    'New York': 'NYM',
    'St. Louis': 'STL',
    'Arizona': 'ARI',
    'Miami': 'MIA',
    'Chicago': 'CHC',
    'Washington': 'WSH',
    'Los Angeles': 'LAD',
    'Cincinnati': 'CIN',
    'Philadelphia': 'PHI',
    'Colorado': 'COL'
}

al_abbreviation = {
    'Toronto': 'TOR',
    'Chicago': 'CWS',
    'Kansas City': 'KC',
    'Seattle': 'SEA',
    'Los Angeles': 'LAA',
    'Tampa Bay': 'TB',
    'Oakland': 'OAK',
    'Boston': 'BOS',
    'Minnesota': 'MIN',
    'Houston': 'HOU',
    'New York': 'NYY',
    'Detroit': 'DET',
    'Texas': 'TEX',
    'Cleveland': 'CLE',
    'Baltimore': 'BAL'
}

# Below are a few functions to clean up the data


def make_abbvs(a):
    if a['league'] == 'NL':
        return nl_abbreviation[a['team']]
    else:
        return al_abbreviation[a['team']]


def make_opp_abbvs(a):
    try:
        if a['league'] == 'NL':
            return nl_abbreviation[a['opp']]
        else:
            return al_abbreviation[a['opp']]
    except:
        if a['league'] == 'NL':
            return al_abbreviation[a['opp']]
        else:
            return nl_abbreviation[a['opp']]


def define_bools(away):
    if away == '@' or away == '1':
        away = True
    else:
        away = False
    return away


# innings to outs sorts out the .1 and .2 innings so calculations are easier
def innings_to_outs(number):
    innings = int(str(number)[0])
    partial_innings = int(str(number)[-1])
    outs = innings*3 + partial_innings
    return outs


yesterday_pitchers = yesterday_pitchers.reset_index(drop=True)
yesterday_pitchers['outs'] = yesterday_pitchers['ip'].apply(innings_to_outs)
yesterday_pitchers['opp'] = yesterday_pitchers.apply(lambda x: make_opp_abbvs(x), axis=1)
yesterday_pitchers['gs'] = yesterday_pitchers['gs'].apply(define_bools)
yesterday_pitchers = yesterday_pitchers.replace('---', np.nan)
yesterday_pitchers = yesterday_pitchers.apply(pd.to_numeric, errors='ignore')
yesterday_pitchers['w'] = yesterday_pitchers['w'].map({1.0: 'W, ', np.nan: ''})
yesterday_pitchers['l'] = yesterday_pitchers['l'].map({1.0: 'L, ', np.nan: ''})
yesterday_pitchers['sv'] = yesterday_pitchers['sv'].map({1.0: 'S, ', np.nan: ''})
yesterday_pitchers['team'] = yesterday_pitchers.apply(lambda x: make_abbvs(x), axis=1)
yesterday_pitchers['whip'] = round(
    (yesterday_pitchers['bb'] + yesterday_pitchers['h']) / (yesterday_pitchers['outs']/3), 2)
yesterday_pitchers['ppi'] = round(
    (yesterday_pitchers['pit'] / (yesterday_pitchers['outs']/3)), 2)
yesterday_pitchers['kpbb'] = round(
    yesterday_pitchers['k']/yesterday_pitchers['bb'], 2)
yesterday_pitchers['fip'] = ((13*yesterday_pitchers['hr'] + 3*(yesterday_pitchers['bb'] +
                             yesterday_pitchers['hbp']) - 2*yesterday_pitchers['k'])/(yesterday_pitchers['outs']/3))+3.130
yesterday_pitchers['date'] = yesterday
yesterday_pitchers = yesterday_pitchers.replace(np.inf, np.nan)

# starter and relievers are separated to get averages
yesterday_relievers = yesterday_pitchers[yesterday_pitchers['gs'] == False]
yesterday_starters = yesterday_pitchers[yesterday_pitchers['gs'] == True]

# to deal with NaN and inf values created when a pitcher didn't record an out, I just gave the pitcher 1 out
yesterday_pitchers['kpbb'].fillna(value=round(
    yesterday_pitchers['k'], 2), inplace=True)
yesterday_pitchers['ppi'].fillna(value=round(
    (yesterday_pitchers['pit'] / 1)*3, 2), inplace=True)
yesterday_pitchers['whip'].fillna(value=round(
    (yesterday_pitchers['bb'] + yesterday_pitchers['h']) * 3, 2), inplace=True)
yesterday_pitchers['fip'].fillna(value=round(((13*yesterday_pitchers['hr'] + 3*(
    yesterday_pitchers['bb'] + yesterday_pitchers['hbp']) - 2*yesterday_pitchers['k'])/(.3))+3.130, 2), inplace=True)


# Creating the a dataframe with just mets pitching. In the future I can potentially have all teams  
mets_pitching = yesterday_pitchers[yesterday_pitchers['team'] == 'NYM'].copy()


# Dataframes created for the database, around line 375
viz_history = mets_pitching[['team', 'date', 'name', 'ppi', 'fip', 'whip', 'kpbb']].copy()

viz_info = mets_pitching[['team', 'date', 'name', 'ppi', 'fip', 'whip', 'kpbb']].copy()

# I scraped starting and relief pitching stats and only took their Pitch Per Inning (PPI) stats.  
# I can always get more information if necessary in the future
sp_url = 'https://www.baseball-reference.com/leagues/majors/2022-starter-pitching.shtml'
sp_r = requests.get(sp_url)
with open('webpages/sp_baseball.html', "w+", encoding='utf-8') as sp_f:
    sp_f.write(sp_r.text)
with open('webpages/sp_baseball.html', encoding='utf_8') as sp_f:
    sp_baseball = sp_f.read()

sp_soup = BeautifulSoup(sp_baseball, "html.parser")
sp_first = sp_soup.find('table', id="teams_starter_pitching")
sp_soup2 = BeautifulSoup(str(sp_first), 'html.parser')
sp_tbody = sp_soup2.find('tbody')
sp_row = sp_tbody.find_all('tr')

sp_colls = []
sp_irow = 0
sp_columns = ['team', 'ip', 'pit']
sp_averages = pd.DataFrame(columns=sp_columns)
for row in sp_row:
    team = row.find_all('th')
    cols = row.find_all('td')
    sp_averages.loc[sp_irow] = [
        team[0].text.strip(),  # team
        cols[25].text.strip(),  # innings pitched per game started
        cols[26].text.strip(),  # pitches per game started
    ]
    sp_irow = sp_irow+1

sp_averages['outs'] = sp_averages['ip'].apply(innings_to_outs)
sp_averages = sp_averages.apply(pd.to_numeric, errors='ignore')
sp_averages['ppi'] = sp_averages['pit'] / sp_averages['outs']*3
sp_averages['date'] = yesterday
sp_averages = sp_averages[sp_averages['team'] == 'League Average']
sp_averages['name'] = 'Starter Average'
sp_averages['gs'] = True
sp_averages = sp_averages.reset_index(drop=True)

# Relief Pitching Stats
rp_url = 'https://www.baseball-reference.com/leagues/majors/2022-reliever-pitching.shtml'
rp_r = requests.get(rp_url)
with open('webpages/rp_baseball.html', "w+", encoding='utf-8') as rp_f:
    rp_f.write(rp_r.text)
with open('webpages/rp_baseball.html', encoding='utf_8') as rp_f:
    rp_baseball = rp_f.read()

rp_soup = BeautifulSoup(rp_baseball, "html.parser")
rp_first = rp_soup.find('table', id="teams_reliever_pitching")
rp_soup2 = BeautifulSoup(str(rp_first), 'html.parser')
rp_tbody = rp_soup2.find('tbody')
rp_row = rp_tbody.find_all('tr')

rp_colls = []
rp_irow = 0
# rp_columns = ['team', 'pit', 'outs']
rp_columns = ['team', 'ip', 'pit', 'outs']
rp_averages = pd.DataFrame(columns=rp_columns)
for row in rp_row:
    team = row.find_all('th')
    cols = row.find_all('td')
    rp_averages.loc[rp_irow] = [
        team[0].text.strip(),  # team
        # average outs recorded games in relief
        float(cols[28].text.strip())/3,
        cols[29].text.strip(),  # pitches per game in relief
        cols[28].text.strip(),  # average outs recorded games in relief
    ]
    rp_irow = rp_irow+1

rp_averages = rp_averages.apply(pd.to_numeric, errors='ignore')
rp_averages['ppi'] = round((rp_averages['pit'] / rp_averages['outs'])*3, 1)
rp_averages['date'] = yesterday
rp_averages = rp_averages[rp_averages['team'] == 'League Average']
rp_averages['name'] = 'Reliever Average'
rp_averages['gs'] = False
rp_averages['ip'] = format(float(rp_averages['ip']), ".3f")
rp_averages = rp_averages.reset_index(drop=True)

# League Pitching Averages to get FIP and WHIP info
league_url = 'https://www.baseball-reference.com/leagues/majors/2022.shtml#all_teams_standard_pitching'
league_r = requests.get(league_url)
with open('webpages/league_baseball.html', "w+", encoding='utf-8') as league_f:
    league_f.write(league_r.text)
with open('webpages/league_baseball.html', encoding='utf_8') as league_f:
    league_baseball = league_f.read()

soup = BeautifulSoup(league_baseball, "html.parser")

# The pitching tables are commented out in the source code, so the below code is to parse out the table info from any comments
comments = soup.find_all(string=lambda text: isinstance(text, Comment))
league_soup2 = BeautifulSoup(str(comments), "html.parser")
league_second = league_soup2.find('table', id="teams_standard_pitching")
league_tbody = league_soup2.find('tbody')
league_row = league_tbody.find_all('tr')


league_colls = []
league_irow = 0
league_columns = ['team', 'fip', 'whip', 'kpbb', 'date']
league_pitching = pd.DataFrame(columns=league_columns)
for row in league_row:
    team = row.find_all('th')

    cols = row.find_all('td')
    league_pitching.loc[irow] = [
        team[0].text.strip(),  # team
        cols[27].text.strip(),  # FIP
        cols[28].text.strip(),  # WHIP
        cols[33].text.strip(),  # strikeout per walk
        yesterday  # date
    ]
    irow = irow+1

league_pitching = league_pitching.apply(pd.to_numeric, errors='ignore')

# after averages for teams and the league are imported, isolate the 'League Average' line for fip, whip, and kpbb
league_average = league_pitching[league_pitching['team'] == 'League Average']
league_average = league_average.reset_index(drop=True)


# add ppi for both relief and starters, and make it a dataframe we can append to the final viz at the end
r_avg = league_average.copy().reset_index(drop=True)
r_avg['ppi'] = rp_averages['ppi'].mean()
r_avg['name'] = rp_averages.loc[0, 'name']


s_avg = league_average.copy().reset_index(drop=True)
s_avg['ppi'] = sp_averages['ppi'].mean()
s_avg['name'] = sp_averages.loc[0, 'name']


#combine starters and relievers, and add 'inverted' tables for visualization
# ...so better than average can be on the right instead of on the left
league_averages = pd.concat([r_avg, s_avg])
league_averages['ppi_invtert'] = league_averages['ppi'] * -1
league_averages['whip_invtert'] = league_averages['whip'] * -1
league_averages['fip_invtert'] = league_averages['fip'] * -1


# METS STATS so I can add season records and era to the viz
met_url = "https://www.baseball-reference.com/teams/NYM/2022.shtml"
met_r = requests.get(met_url)
with open('webpages/mets_stats.html', "w+", encoding='utf-8') as met_f:
    met_f.write(met_r.text)
with open('webpages/mets_stats.html', encoding='utf_8') as met_f:
    met_baseball = met_f.read()

# using BeautifulSoup to parse through webpage to get to table containing yesterday's pitchers
met_soup = BeautifulSoup(met_baseball, "html.parser")
met_first = met_soup.find('div', id="div_team_pitching")
met_soup2 = BeautifulSoup(str(met_first), 'html.parser')
met_tbody = met_soup2.find('tbody')
met_rows = met_tbody.find_all('tr')

# need to loop through all of the rows in the table and individually take out values and add them to a DataFrame
met_irow = 0
columns = ['name', 'season_w', 'season_l', 'season_era', 'season_saves']

mets_year_pitching = pd.DataFrame(columns=columns)
for row in met_rows:
    cols = row.find_all('td')
    if (len(cols) >= 1):
        mets_year_pitching.loc[met_irow] = [
            cols[1].text.strip(),  # name
            cols[3].text.strip(),  # win
            cols[4].text.strip(),  # loss
            cols[6].text.strip(),  # era
            cols[12].text.strip(),  # saves
        ]
    met_irow = met_irow+1

mets_year_pitching = mets_year_pitching.reset_index(drop=True)


# the website had symbols next to players' names, so this function cleans it up
def clean_name(name):
    name = name.split()
    just_name = name[0] + ' ' + name[1]
    just_name = just_name.replace('*', '')
    return just_name


mets_year_pitching['name'] = mets_year_pitching['name'].apply(
    lambda x: clean_name(x))
met_pitchers_join = mets_pitching.join(
    mets_year_pitching.set_index('name'), on='name', how='left')
mets_pitching = met_pitchers_join.round(decimals=2)

mets_pitching = mets_pitching.drop('date', axis=1)
mets_pitching['date'] = yesterday
# To make the viz easier, below are the headers for starters and closers
mets_pitching['pitch_header'] = mets_pitching['name'] + ' (' + mets_pitching['w'] + mets_pitching['l'] + mets_pitching['season_w'] + '-' + mets_pitching['season_l'] + ')'
mets_pitching['save_header'] =  mets_pitching['name'] + ' (' + mets_pitching['sv'] + mets_pitching['season_saves'] + ')'



# ****************************************************DATABASE INFORMATION***************************************************************




# IMPORTANT TABLES AND THEIR USES

# ' mets_to_db' IS THE TABLE THAT WILL BE ADDED TO THE 'METS_PITCHING_HISTORY' TABLE TO KEEP TRACK OF PAST PERFORMANCES
# 'viz_info' IS THE INITIAL VISUALIZATION TABLE.  IT WILL NEED TO BE RECALLED AND CLEANED
# 'mets_pitching_today' IS THE TABLE THAT I WILL USE TO PRINT THE PITCHER'S STAT LINE
# 'final_viz' IS THE TABLE THAT WILL BE USED TO MAKE VIZUALS.  IT IS AN EDITED VERSION OF viz_info


# connect and add to database
conn = config.connect
quoted = quote_plus(conn)
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))

# viz_history.to_sql('mets_pitching_history', engine, index=True, if_exists='append')
viz_info.to_sql('visualization_info', engine, index=True, if_exists='replace')
mets_pitching.to_sql('mets_pitching_today', engine,
                     index=True, if_exists='replace')

mets_yesterday_history = pd.read_sql("SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY name order by date desc) as name_by_date FROM [dbo].[mets_pitching_history] WHERE name in (SELECT name FROM [dbo].[mets_pitching_today])) ranks WHERE name_by_date <= 5;", engine, parse_dates=["dates"])

mets_yesterday_history['name'] = mets_yesterday_history['name'] + ' ' + (mets_yesterday_history['name_by_date'] * -1 + 6).astype(str)
mets_yesterday_history = mets_yesterday_history.drop(columns=['index', 'name_by_date'])


adjusted_averages = league_averages.copy()
final_viz = pd.concat([mets_yesterday_history, adjusted_averages])
final_viz['ppi_invtert'] = final_viz['ppi'] * -1
final_viz['whip_invtert'] = final_viz['whip'] * -1
final_viz['fip_invtert'] = final_viz['fip'] * -1


final_viz.to_sql('final_viz', engine, index=True, if_exists='replace')


# # The code below recalls all tables from the database
# mets_history = pd.read_sql("SELECT * FROM mets_pitching_history ORDER BY date desc", engine, index_col="index", parse_dates=["dates"])
# mets_yesterday = pd.read_sql("SELECT team, ppi, date, name, fip, whip, kpbb FROM visualization_info", engine, parse_dates=["dates"])
# mets_yesterday_history = pd.read_sql("SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY name order by date desc) as name_by_date FROM [dbo].[mets_pitching_history] WHERE name in (SELECT name FROM [dbo].[mets_pitching_today])) ranks WHERE name_by_date <= 10;", engine, parse_dates=["dates"])

engine.dispose()
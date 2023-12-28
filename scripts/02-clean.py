import pandas as pd
import glob
import os

# Clean Up the Teams CSV to show distinct teams
teams = pd.read_csv('teams.csv').drop_duplicates('team_id').reset_index()
teams.to_csv('teams.csv')

# For some reason the play_ids for some games are negative so those will be converted to positive
def positive(column):
    return abs(column)


csv_files = glob.glob('years/' + '*.csv')
combined_data = pd.DataFrame()

for file in csv_files:
    curr = pd.read_csv(file)
    curr['play_id'] = curr['play_id'].apply(positive)
    curr.to_csv(file, index = False)
    combined_data = pd.concat([combined_data, curr], ignore_index = True)
    
# Combined data set starts from 2023 to 2014.
combined_data.to_csv('years/combined.csv', index = False)
import os
import json
import csv
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# KOVAAKs LEADERBOARD IDs
Leaderboard_ID = [
    387,337,573,1581,
    781,782,607,5093,
    655,609,572,5137,
    571,355,5191,5536,
    15485,738,739,5159,
    740,611,610,5054,

    387,337,573,1581,
    526,583,777,5053,
    4686,5151,419,5102,
    430,9824,5075,3696,
    15485,527,528,5133,
    105, 360,552,5042,
]


# S1 RANK REQUIREMENTS
RankReq = [
    [0, 80, 85, 90, 100, 110, 115, 120, 125, 132, 137, 145, 150],
    [0, 70, 75, 85, 95, 105, 115, 120, 125, 130, 135, 140, 145],
    [0, 110, 115, 125, 135, 145, 155, 160, 165, 170, 175, 185, 195],
    [0, 80, 90, 100, 110, 120, 130, 140, 150, 155, 160, 170, 180],
    [0, 50, 55, 62, 68, 76, 85, 70, 80, 85, 90, 97, 103],
    [0, 90, 94, 100, 108, 115, 125, 108, 115, 122, 127, 137, 145],
    [0, 80, 120, 160, 200, 250, 290, 200, 250, 290, 320, 360, 390],
    [0, 45, 52, 56, 60, 64, 68, 55, 60, 65, 75, 85, 91],
    [0, 850, 1000, 1150, 1350, 1550, 1800, 1250, 1400, 1600, 1800, 2000, 2300],
    [0, 2100, 2400, 2700, 3100, 3600, 4000, 3600, 3900, 4100, 4400, 4700, 5200],
    [0, 880, 885, 890, 900, 905, 915, 898, 906, 910, 915, 921, 927],
    [0, 7200, 7800, 8400, 9000, 9500, 10000, 7000, 7600, 8200, 8800, 9250, 10800],
    [0, 7000, 8000, 9000, 10000, 11000, 12000, 8000, 9000, 9400, 9900, 10800, 11700],
    [0, 872, 882, 892, 902, 912, 918, 886, 896, 900, 907, 914, 922],
    [0, 10300, 10700, 11200, 11700, 12700, 13700, 10500, 11500, 12200, 12800, 13450, 14000],
    [0, 5700, 6400, 7200, 7600, 8300, 9000, 5500, 6100, 6400, 6800, 7350, 7900],
    [0, 4800, 5200, 5600, 6200, 6600, 7000, 7300, 7500, 7800, 8300, 8800, 9200],
    [0, 90, 100, 110, 120, 130, 140, 132, 138, 143, 148, 153, 158],
    [0, 71, 77, 83, 90, 95, 100, 96, 100, 104, 108, 112, 117],
    [0, 470, 500, 530, 560, 590, 630, 570, 600, 630, 650, 690, 720],
    [0, 45, 50, 55, 60, 65, 70, 64, 68, 72, 77, 84, 90],
    [0, 16000, 17000, 18000, 19000, 20000, 21000, 19000, 20500, 21500, 22500, 23700, 24600],
    [0, 48, 52, 56, 60, 64, 68, 65, 70, 75, 78, 84, 90],
    [0, 36, 42, 48, 54, 64, 68, 52, 56, 60, 65, 72, 76],
]

# S1 RANKS
Ranks = ["Unranked", "Bronze", "Silver", "Gold", "Platinum", "Ruby", "Emerald", "Diamond", "Master", "Grandmaster", "Immortal", "Archon", "Divine"]

# FUNCTION TO PROCESS EACH PAGE OF EACH LEADERBOARD (FUNCTION CALLED VIA THREADING)
def process_leaderboard(leaderboard_id, page, session, itera, Count, score_lock, Score_Dic, RankReq):
    result = []

    # API DATA PULL
    try:
        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={leaderboard_id}&page={page}&max=100").json()
        print(f"Leaderboard {leaderboard_id}. Page: {page} data pull.")

        # ITERATE THROUGH ALL DATA ROWS (100 LEADERBOARD ENTRIES) IN THE API PULL
        for Data in r['data']:
            try:
                Steam_Name = Data['steamAccountName']
                Steam_ID = Data['steamId']
                Score = Data['score']

                # LOCK
                with score_lock:

                    # IF STEAM ID WAS NOT YET SEEN CREATE KEY AND SET VOLTS TO ZERO
                    if Steam_ID not in Score_Dic:
                        Score_Dic[Steam_ID] = [-2] * (29)
                        Score_Dic[Steam_ID][25] = Steam_Name
                        Score_Dic[Steam_ID][26] = 0

                    # FOR EASY LEADERBOARDS
                    if itera == 1:

                        # ITERATE THROUGH RANKS
                        for iii in range(0, 7):
                            if RankReq[Count][iii] <= Score:
                                #Score_Dic[Steam_ID][19 + Count] = iii - 1
                                Score_Dic[Steam_ID][Count] = iii
                        #        Volts = iii
                        #Score_Dic[Steam_ID][26] += Volts

                    # FOR NORMAL LEADERBOARD
                    elif itera == 2:

                        # ITERATE THROUGH RANKS
                        for iii in range(7, 13):
                            if RankReq[Count][iii] <= Score:
                                #Score_Dic[Steam_ID][Count] = iii-1
                                Score_Dic[Steam_ID][Count] = iii
                        #        Volts = iii
                        #Score_Dic[Steam_ID][26] += Volts

            except KeyError:
                continue
    except Exception as e:
        print(f"Error processing leaderboard {leaderboard_id} page {page}: {e}")
    return result

# Main code with threading and lock protection
Score_Dic = {}
score_lock = Lock()  # Create a lock for protecting shared resources

# START THREADER
with ThreadPoolExecutor(max_workers=20) as executor:
    Count = 0
    itera = 1
    futures = []
    session = requests.Session()

    # ITERATE THROUGH ALL LEADERBOARDS
    for i in range(len(Leaderboard_ID)):
        r = session.get(f"https://kovaaks.com/webapp-backend/leaderboard/scores/global?leaderboardId={Leaderboard_ID[i]}&page=0&max=100").json()
        Max_Page = r['total'] // 100
        #Max_Page = 2

        # ITERATE THROUGH ALL LEADERBOARD PAGES AND SEND TO FUNCTION
        for ii in range(Max_Page + 1):
            futures.append(executor.submit(process_leaderboard, Leaderboard_ID[i], ii, session, itera, Count, score_lock, Score_Dic, RankReq))

        # LOCK CRITERIA (NEEDED)
        with score_lock:
            Count += 1
            if Count >= 24:
                Count = 0
                itera = 2

    # PROCESS RESULTS
    for future in as_completed(futures):
        future.result()  # No need to handle this since the processing is done within the function

    session.close()

# FIGURE OUT VOLTS
for key, values in Score_Dic.items():
    values[26] = sum(v for v in values[0:24] if v > -2)

# SORT Volts
Score_Dic_S = dict(sorted(Score_Dic.items(), key=lambda item: (item[1][26]), reverse=True))

# ITERATE THROUGH ALL KEYS IN DICTIONARY
Count = 0
for key, values in Score_Dic_S.items():
    RankL = values[0:24]

    # CALCULATE RANKS
    for i in range(0, 13):
        if max(RankL[0:4]) >= i and max(RankL[4:8]) >= i and max(RankL[8:12]) >= i and max(RankL[12:16]) >= i and max(RankL[16:20]) >= i and max(RankL[20:24]) >= i:
            values[24] = Ranks[i]
        if min(RankL) >= i and i >= 0:
            values[24] = Ranks[i] + " Complete"

    # COUNT OF RELEVANT ENTRIES
    if values[24] != -2:
        Count += 1
        values[27] = Count

    # CONVERT RANKL TO ACTUAL RANKS (NUMBERS TO NAMES)
    for i in range(len(RankL)):
        RankL[i] = Ranks[RankL[i]]

    values[0:24] = RankL

# GOOGLE SHEETS API
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# JSON CREDENTIAL FILE PATH
creds_dict = json.loads(os.getenv('GSPREAD_CREDENTIALS'))
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

# AUTHORIZE THE CLIENT
client = gspread.authorize(creds)

# OPEN GOOGLE SHEET
sheet = client.open('S1_RA').sheet1
sheet1 = client.open('S1_RA_Old').sheet1

sheet1.clear()
data = sheet.get_all_values()
sheet1.update('A1', data)

# CLEAR EXISTING DATA IN GOOGLE SHEET
sheet.clear()

# SHEET HEADERS
header = ['PlayerID',  '1w4ts reload','Wide Wall 3 Targets','voxTS Static Click rAim','1w6t NQS Raspberry',
          'Bounce 180 rAim','Pasu Reload Goated', 'Popcorn Goated rAim', 'ToonsClick rAim',
          'PGTI rAim', 'Smoothbot rAim', 'Air Angelic', 'Controlsphere rAim',
          'fuglaaXY Reactive rAim', 'Air Small 3478 rAim', 'MFSI rAim', 'Smooth Thin Strafes Raspberry',
          'PatCircleSwitch rAim', 'Pokeball Wide rAim', 'voxTS rAim', 'devTS Goated rAim',
          'Bounce 180 Tracking Small', 'kinTS rAim', 'Pasu Track Smaller rAim', 'ToonsTS rAim',
          'Rank', 'Player', 'Points', 'Number', 'Percentage']


# WRITE HEADERS TO FIRST ROW
sheet.append_row(header)

# SEND DATA FROM DICTIONARY TO ARRAY
Per = 0
rows_to_update = []
for key, values in Score_Dic_S.items():
    if values[24] != -2:
        values[28] = round(1 - Per / Count, 6)
        if values[25] is not None:
            values[25] = values[25].encode('ascii', 'ignore').decode('ascii')
        else:
            values[25] = ''
        # Add the row to the list
        rows_to_update.append([key] + values)
        Per += 1

# UPDATE GOOGLE SHEET WITH ALL ARRAY DATA
start_cell = 'A2'
sheet.update(rows_to_update, start_cell)

#!/usr/bin/env python

"""
This code parses NBA play-by-play data from one github account into a csv.  It currently only lists freethrow events.
"""

# data from
#    https://github.com/octonion/basketball/tree/master/nba/csv

# other useful resoruces: 
#    https://www.nbastuffer.com/access-exportable-nba-stats/
#    https://github.com/octonion/basketball
#    https://github.com/gmf05/nba
#    https://github.com/neilmj/BasketballData
#    https://github.com/abresler/NBA-Data-Stuff

import io
import gzip
import csv
import json
import re

### helpers
def detectEventType(description):
    #descriptions catch substittuions, violation, timeout, turnovers, fouls, rebound, period end and beginning, freethrows,  jump ball, shots, 
    d = description
    o = "ERROR"
    if re.search("Substitution", d):
        o = "substitution"
    elif d == "Start Period" or d == "End Period":
        o = "game_edge"
    elif re.search("Ruling", d):
        o = "ruling"
    elif re.search("Ejection", d):
        o = "ejection"
    elif re.search("Violation", d):
        o = "violation"
    elif re.search("Technical", d):
        o = "technical"
    elif re.search("Foul", d):
        o = "foul"
    elif re.search("Timeout", d):
        o = "timeout"
    elif re.search("Turnover", d):
        o = "turnover"
    elif re.search("Rebound", d):
        o = "rebound"
    elif re.search("Free Throw", d):
        o = "free_throw"
    elif re.search("Jump Ball", d):
        o = "jump_ball"
    elif re.search("Shot", d, flags=re.IGNORECASE):
        if re.search("Made", d):
            o = "shot_made"
        elif re.search("Missed", d):
            o = "shot_missed"
        else:
            o = "MISC"
            print( "from detectEventType:", "SHOT:" + d)
    else:
        print("from detectEventType:", d)
        o = "MISC"
    return( o)

def parsePeriodClock(sClock):
    if sClock != "":
        aClock = sClock.split(":")
        iClock = int(aClock[0])*60 + int(aClock[1])
    else:
        iClock = 60*12
    return( iClock)

def estimatePosessingTeam(teams, eventOriginator, prevPossessingTeam, eventType):
    possessingTeam = prevPossessingTeam
    verb = ["gaining", "losing", "continuing"]
    if eventOriginator != "":
        nonEventOriginator = [t for t in teams if t != eventOriginator][0]
        if eventType in ["free_throw", "shot_made", "shot_missed"]:
            possessingTeam = eventOriginator
            verb = "losing"
        elif eventType in ["rebound"]:
            possessingTeam = eventOriginator
            verb = "gaining"
        elif eventType in ["turnover", "timeout"]:
            possessingTeam = nonEventOriginator
            verb = "gaining"
        elif eventType in ["foul", "ruling", "game_edge", "technical", "violation", "ejection"]:
            possessingTeam = prevPossessingTeam
            verb = "continuing"
        elif eventType in ["substitution"]:
            possessingTeam = prevPossessingTeam
            verb = "continuing"
        elif eventType in ["jump_shot"]:
            ### this depends on metadata: who in the original description text gainas possession?
            pass
        else:
            possessingTeam = prevPossessingTeam
            #print("from estimatePosessingTeam:", eventType)
    return(possessingTeam)

def gameClockAnnotations(injsonfilename, outcsvfilename):
    eventDist = {}
    # year is year as int, date is data as YYYYMMDD string, season_phase is text of what part of the season, iseason_phase is integer 1-4 representation of that, game_id is a unique ID for each NBA game across seasons, home and visit are the 3 digit codes for each team.  string uids for games usually put home team first. period is the period of the game, clock_game is game wide clock time, descending (gives unique time for whole game), clock_period is each period's clock, score_* is the current score for each time at that point in the game, event_team lists the event that occurred that prompted an entry in the play-by-play data. possible events are substittuions, violation, timeout, turnovers, fouls, rebound, period end and beginning, freethrows,  jump ball, shots.
    header = ["year", "date", "season_phase", "iseason_phase", "game_id", "home", "visit", "period", "clock_game", "clock_period", "event", "score_h", "score_v", "event_team", "possess"]
    # https://stackoverflow.com/questions/39450065/python-3-read-write-compressed-json-objects-from-to-gzip-file
    with io.TextIOWrapper(gzip.open(injsonfilename, 'r')) as fin:        # 4. gzip
        reader = csv.reader(fin)
        with open(outcsvfilename, 'w') as fout:        # 4. gzip
            writer = csv.writer(fout)
            writer.writerow(header)
            for i, l in enumerate(reader):
                if "resultSets" in json.loads(l[5].replace("=>", ":")):
                    print("2015 season can't get chugged yet: it's in a different format that 2014")
                    break
                #if i > 2: break
                #if i < 100: continue
                #print(l[5])
                #print(row)
                #json_bytes = fin.read()                          # 3. bytes (i.e. UTF-8)
                game_date = l[0]
                game_id = l[1]
                game_UNKNOWN = l[2] # this is mostly 4
                game_event_details = json.loads(l[3].replace("=>", ":"))
                game_period = int(l[4])
                game_details = json.loads(l[5].replace("=>", ":"))
                #print(l[5])
                events = game_details["sports_content"]["game"]["play"]
                ### coarse (period level) header
                oyear = int(game_details["sports_content"]["sports_meta"]["season_meta"]["season_year"])
                odate = game_event_details["date"]
                oseasonphase = game_details["sports_content"]["sports_meta"]["season_meta"]["display_season"]
                oseasonphase2 = int(game_details["sports_content"]["sports_meta"]["season_meta"]["season_stage"])
                oid = game_id
                oabbrh = game_event_details["home"]["abbreviation"]
                oabbrv = game_event_details["visitor"]["abbreviation"]
                teamsabbrs = [oabbrh, oabbrv]
                operiod = game_period
                ogrow = [oyear, odate, oseasonphase, oseasonphase2, oid, oabbrh, oabbrv, operiod, ]
                ### fine (within period play level) header
                for j, e in enumerate(events):
                    #if j > 2: break
                    #print(e)
                    #descriptions catch shots, turnovers, fouls, rebound, freethrows, substittuions, violation, timeout, jump ball, period end and beginning
                    eventOriginator =  e["team_abr"]
                    oeventtype = detectEventType(e["description"])
                    eventDist[oeventtype] = 1 + eventDist.get(oeventtype,0)
                    opclock = parsePeriodClock(e["clock"])
                    ogclock = opclock + (4-operiod)*12*60
                    oscoreh = e["home_score"]
                    oscorev = e["visitor_score"]
                    opossessing = "FLAW"+estimatePosessingTeam(teamsabbrs, eventOriginator, "", oeventtype)
                    oerow = [ogclock, opclock, oeventtype, oscoreh, oscorev, eventOriginator, opossessing]
                    if oeventtype == "free_throw":
                        writer.writerow(ogrow + oerow)
                    #writer.writerow(ogrow + oerow)
                #print(len(l)) # = 6
                #print(l)
                #print(game_date)
                #print(game_id)
                #print(game_UNKNOWN)
                #print(json.dumps(game_event_details, sort_keys=True, indent=4, separators=(',', ': ')))
                #print(game_period)
                #print(json.dumps(game_details, sort_keys=True, indent=4, separators=(',', ': ')))
                #print(json.dumps(events, sort_keys=True, indent=4, separators=(',', ': ')))
                #print(ogrow)
    #print( eventDist )

gameClockAnnotations( "data/pbp_2014.csv.gz", "data/nba_2014_freethrows.csv")
gameClockAnnotations( "data/pbp_2015.csv.gz", "data/nba_2015_freethrows.csv")

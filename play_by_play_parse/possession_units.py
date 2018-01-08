"""
This takes parse play by play data and produces possession edges:
    the times at which possession swtich from no teams to one team,
    or from one team to another.
"""

import sys
sys.path.extend((".",".."))
from local_settings import *
import csv

TODO = """
Steps:
    DONE: Identify teams
    DONE: Identify events
        descriptions catch substittuions, violation, timeout, turnovers, fouls, rebound, period end and beginning, freethrows,  jump ball, shots, 
    DONE: Allow three states:
        DONE: no possession
        DONE: one team
        DONE: other team
    Classify events into two types:
        those that signal possession defo changing: turnover, period end, period beginning
        those that signal possession possibly changing: violation, foul, freethrow, rebound, jump ball, technical
        signals that indicate controlling team: period beginning (None), shot_made (This), shot_missed (This), rebound (Not Sure), turnover (Not Sure),jump
        signals that give no indication of controlling team: period end, violation, foul, freethrow, rebound?, technical
        other: substittuions, timeout, ruling, ejection
    Use events that signal possession changing to create edges
    Use events that indicate current possession changing to sanity check possession edges
"""

def possessFromPbP(incsvfilename):
    eventDist = {}
    pbp = {}
    pbpEdges = {} #game possession edges
    with open(incsvfilename, 'r') as fin:
        reader = csv.DictReader(fin)
        tposs = 2880
        for i, l in enumerate(reader):
            if l['game_id'] not in pbpEdges:
                pbpEdges[ l['game_id'] ] = {}
                pbp[ l['game_id'] ] = []
            if l['event'] in ("game_begin", "game_end", "turnover", "violation", "ejection", "foul", 'freethrow', 'rebound', "jump_ball", "shot_made", "shot_missed"):
                tposs = l['clock_game']
                pbpEdges[ l['game_id'] ][tposs] = []
            pbpEdges[ l['game_id'] ][tposs] = pbpEdges[ l['game_id'] ].get(tposs,[]).append( l )
            #pbpEdges.get(l['game_id'], {}).get(tposs, []).append( l )
            pbp[ l['game_id'] ].append( l )
        print("keys are ", l.keys())
        print("got as high as ", i)
        print("num of games", len(pbpEdges.keys()))
        print("num of games", len(pbp.keys()))
    return(pbpEdges)

def testPossess(pbpEdges) :
    gameData = {}
    print("games" + str(len(pbpEdges.keys())))
    for game, possess in pbpEdges.items():
        poss = "None"
        #if not bool(possess): continue
        print(game)
        print(possess)
        gameData[game] = {k: possess[2880][k] for k in ("year", "date", "season_phase", "iseason_phase", "game_id", "home", "visit", "period")}
        gameData[game]['control'] = {}
        for edgeTime, events in possess.items():
            gameData[game]['control'][edgeTime] = poss
            for i, event in enumerate(events):
                if event['event'] in ('game_end', 'shot_made', 'shot_missed', 'rebound', 'turnover', 'jump_shot'):
                    if event['event_team'] != poss:
                        print(events[i-1])
                        print(events[i])
                        print(events[i+1])
                        print(events[i+2])
                        print()
                        break
                    else:
                        print('smthing else')
                elif event['event'] in ('game_begin'):
                    if event['event_team'] != "":
                        print(events[i-1])
                        print(events[i])
                        print(events[i+1])
                        print(events[i+2])
                        print()
                        break
                    else:
                        print('smthing else')
                else:
                    print('smthing else')
    return(pbpEdges)

def writePossess(outcsvfilename, eventDist):
    #header = ["year", "date", "season_phase", "iseason_phase", "game_id", "home", "visit", "period", "clock_game", "clock_period", "event", "score_h", "score_v", "event_team", "possess"]
    #writer.writerow(header)
    with open(outcsvfilename, 'w') as fout:
        writer = csv.writer(fout)

try:
    events = possessFromPbP( pbpPath + "data/nba_2014_events.csv")
    print("events " + str(len(events.keys())))
    edges = testPossess(events)
    print("edges " + str(len(edges.keys())))
    writePossess( pbpPath + "data/scratch.csv", edges)
except (BrokenPipeError, IOError):
    pass

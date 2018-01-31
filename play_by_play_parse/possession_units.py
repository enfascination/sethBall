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
        #tposs = '2880'
        tposs = None
        for i, l in enumerate(reader):
            ### scrolling one by one through games, this recognized when I just hit a new one.
            if l['game_id'] not in pbpEdges:
                pbpEdges[ l['game_id'] ] = {}
                pbp[ l['game_id'] ] = []
            ### assign control of the ball in this even, if possible
            if l['event'] in ("jump_ball", "shot_made", "shot_missed", "turnover", "foul_offensive", "rebound", "free_throw", "shot_made", "shot_missed"):
                l['control'] = l['event_team']
            elif l['event'] in ("steal", "foul_shooting"):
                l['control'] = l['home'] if l['event_team'] == l['visit'] else l['visit']
            elif l['event'] in ("game_begin", "game_end", "substitution", "ruling", "ejection", "violation", "technical", "foul", "foul_personal", "timeout"):
                l['control'] = None
            else:
                l['control'] = None
            #print(l['event'] )
            #if l['clock_game'] == str(2880): print(l['event'])
            ### these events mark the beginning of a new period of possession
            if l['event'] in ("game_begin", "jump_ball", "steal", "rebound"):
                tposs = l['clock_game']
                pbpEdges[ l['game_id'] ][tposs] = {}
                pbpEdges[ l['game_id'] ][tposs]['e'] = []
                pbpEdges[ l['game_id'] ][tposs]['i'] = {k: l[k] for k in ("year", "date", "season_phase", "iseason_phase", "game_id", "home", "visit", "period")}
            pbpEdges[ l['game_id'] ][tposs]['e'].append( l )
            pbp[ l['game_id'] ].append( l )
            ### these events mark the end of a period of possession
            if l['event'] in ("game_end", "turnover", "shot_made", "shot_missed", "free_throw", "foul_offensive"):
                tposs = l['clock_game']
                pbpEdges[ l['game_id'] ][tposs] = {}
                pbpEdges[ l['game_id'] ][tposs]['e'] = []
                pbpEdges[ l['game_id'] ][tposs]['i'] = {k: l[k] for k in ("year", "date", "season_phase", "iseason_phase", "game_id", "home", "visit", "period")}
        reader = csv.DictReader(fin)
        for i, l in enumerate(reader):
            for g in l:
                for p in g:
                    controls = [e['control'] for e in p['e']]
                    print(controls)
        print("keys are ", l.keys())
        print("got as high as ", i)
        print("num of games", len(pbpEdges.keys()))
        print("num of games", len(pbp.keys()))
    return(pbpEdges)

def testPossess(pbpEdges) :
    print("games" + str(len(pbpEdges.keys())))
    for game, possess in pbpEdges.items():
        poss_l = "None"
        poss = "None"
        #if not bool(possess): continue
        print(game)
        #print(sorted(possess.keys(), reverse=True))
        nextPossTime = dict(zip(tuple(possess.keys())[:-1], tuple(possess.keys())[1:] ))
        for edgeTime, events in possess.items():
            for i, event in enumerate(events):
                event['aa_poss'] = poss
                if event['event'] in ('game_end', 'shot_made', 'shot_missed', 'rebound', 'turnover', 'jump_shot'):
                    if event['event_team'] != poss:
                        print(events)
                        print(possess[nextPossTime[edgeTime]])
                        print()
                        return()
                    else:
                        print('smthing else')
                elif event['event'] in ('game_begin'):
                    if event['event_team'] != "":
                        print(events)
                        print(possess[nextPossTime[edgeTime]])
                        print()
                        print()
                        return()
                    else:
                        print('smthing else')
                else:
                    print('smthing else')
    return()

def writePossess(outcsvfilename, eventDist):
    #header = ["year", "date", "season_phase", "iseason_phase", "game_id", "home", "visit", "period", "clock_game", "clock_period", "event", "score_h", "score_v", "event_team", "possess"]
    #writer.writerow(header)
    with open(outcsvfilename, 'w') as fout:
        writer = csv.writer(fout)

try:
    events = possessFromPbP( pbpPath + "data/nba_2014_events.csv")
    print("events " + str(len(events.keys())))
    edges = testPossess(events)
    #writePossess( pbpPath + "data/scratch.csv", edges)
except (BrokenPipeError, IOError):
    pass

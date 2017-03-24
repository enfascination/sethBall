import requests
import pandas as pd
import numpy as np
import json

# uncomment this line for inline use with ipython on mac
#%matplotlib osx


## in order to use the files please install following libraries
# matplotlib
# seaborn
# nba_py
# scipy
import matplotlib.pyplot as plt
import seaborn as sns
import nba_py as nba
from nba_py import player
from nba_py import team
from scipy.spatial.distance import euclidean


def data_loader(file_name):
    data = pd.read_json(file_name)

    home = {}
    moments = []
    visitor = {}
    player_moments = []

    for key in data["events"].keys():
        home.update(data["events"][key]["home"])
        visitor.update(data["events"][key]["visitor"])
        moments.append(data["events"][key]["moments"])

    headers = ["team_id", "player_id", "x_loc", "y_loc",
               "radius", "moment", "quarter", "game_clock", "shot_clock"]

    for event in moments:
        for moment in event:
        # For each player/ball in the list found within each moment
            for player in moment[5]:
                # Add additional information to each player/ball
                # This info includes the index of each moment, the game clock
                # and shot clock values for each moment
                player.extend((event.index(moment), moment[0], moment[2], moment[3]))
                player_moments.append(player)

    # Get them in the dataframe
    df = pd.DataFrame(player_moments, columns=headers)



    ### Create player dictionary
    id_dict = {}
    # creates the players list with all players in the game
    players = home["players"]
    players.extend(visitor["players"])

    # Update value for the ball
    id_dict.update({-1: ["ball", np.nan]})
    # Add players to their dictionary
    for player in players:
        id_dict[player["playerid"]] = [player["firstname"]+" "+player["lastname"],
                                       player["jersey"]]
    # Append them to the dataframe
    df["player_name"] = df.player_id.map(lambda x: id_dict[x][0])
    df["player_jersey"] = df.player_id.map(lambda x: id_dict[x][1])
    #df = add_possesion(df)
    return df




def add_possesion(df):
    # first append zeros
    df['has_ball'] = np.full(len(df.quarter), False, dtype=bool)
    # create list of players


    for quarter in df.quarter.unique():
        print("Quarter")
        seconds = df[df.quarter == quarter ].game_clock.unique()
        player_list = [ player for \
                player in df.player_name.unique() if player!= 'ball' ]
        for second in seconds:
            distances = {}
            time_mask = (df.game_clock==second) & (df.quarter == quarter)
            time_df = df[time_mask]
            for player in player_list:
                try:
                    ball = time_df[time_df.player_name=="ball"]
                    player_df = time_df[time_df.player_name==player]
                    distances[player] = player_dist(ball[["x_loc", "y_loc"]],
                                   player_df[["x_loc", "y_loc"]])
                except:
                    pass

            player_min = min(distances, key=distances.get)
            # df.ix[ (df.quarter == quarter) & \
            #     (df.game_clock == second) &\
            #     (df.player_name == player_min), "has_ball"] = True
            # df.set_value([quarter, second, player_min], True)



    return df




def plot_movement(df, player_input, colormap = 1):
    # Sort it by quarters
    df.sort_values(by=["quarter", "game_clock"], inplace=True)
    # Boolean mask used to grab the data within the proper time period

    for quarter in range(1,5):
        time_mask = (df.game_clock <= 706) & (df.game_clock >= 0) & \
                    (df.shot_clock <= 10.1) & (df.shot_clock >= 0) & \
                    (df.quarter == quarter)
        df_time = df[time_mask]
        leonard = df_time[df_time.player_name==player_input]

        plt.figure(figsize=(15, 11.5))

        # Plot the movemnts as scatter plot
        # using a colormap to show change in game clock
        if colormap == 1:
            plt.scatter(leonard.x_loc, leonard.y_loc, c=leonard.game_clock,
                        cmap=plt.cm.Blues, s=1000, zorder=1)
        else:
            plt.scatter(leonard.x_loc, leonard.y_loc, c=leonard.game_clock,
                        cmap=plt.cm.Greens, s=1000, zorder=1)
        # Darker colors represent moments earlier on in the game
        cbar = plt.colorbar(orientation="horizontal")
        cbar.ax.invert_xaxis()

        # This plots the court
        # zorder=0 sets the court lines underneath leonard's movements
        # extent sets the x and y axis values to plot the image within.
        # The original animation plots in the SVG coordinate space
        # which has x=0, and y=0 at the top left.
        # So, we set the axis values the same way in this plot.
        # In the list we pass to extent 0,94 representing the x-axis 
        # values and 50,0 representing the y-axis values
        plt.imshow(court, zorder=0, extent=[0,94,50,0])

        # extend the x-values beyond the court b/c leonard
        # goes out of bounds
        plt.xlim(0,101)
        plt.savefig('./tex/figs/'+player_input + str(quarter))
        plt.show()


def plot_movement_vs_kawhi(df, player_input):
    # Sort it by quarters
    df.sort_values(by=["quarter", "game_clock"], inplace=True)
    # Boolean mask used to grab the data within the proper time period

    for quarter in range(1,5):
        time_mask = (df.game_clock <= 706) & (df.game_clock >= 0) & \
                    (df.shot_clock <= 10.1) & (df.shot_clock >= 0) & \
                    (df.quarter == quarter)
        df_time = df[time_mask]
        leonard = df_time[df_time.player_name=="Kawhi Leonard"]
        other = df_time[df_time.player_name==player_input]

        plt.figure(figsize=(15, 11.5))

        # Plot the movemnts as scatter plot
        # using a colormap to show change in game clock
        plt.scatter(leonard.x_loc, leonard.y_loc, c=leonard.game_clock,
                    cmap=plt.cm.Blues, s=1000, zorder=1)
        cbar = plt.colorbar(orientation="horizontal")
        cbar.ax.invert_xaxis()

        plt.scatter(other.x_loc, other.y_loc, c=leonard.game_clock,
                    cmap=plt.cm.YlGn, s=1000, zorder=1)
        # Darker colors represent moments earlier on in the game
        cbar = plt.colorbar(orientation="horizontal")
        cbar.ax.invert_xaxis()

        # This plots the court
        # zorder=0 sets the court lines underneath leonard's movements
        # extent sets the x and y axis values to plot the image within.
        # The original animation plots in the SVG coordinate space
        # which has x=0, and y=0 at the top left.
        # So, we set the axis values the same way in this plot.
        # In the list we pass to extent 0,94 representing the x-axis 
        # values and 50,0 representing the y-axis values
        plt.imshow(court, zorder=0, extent=[0,94,50,0])

        # extend the x-values beyond the court b/c leonard
        # goes out of bounds
        plt.xlim(0,101)
        plt.savefig('./tex/figs/'+ player_input + str(quarter))


def plot_distribution(df, player_name="Kawhi Leonard", color=1):
    #### PLOT THE DISTRIBUTION FOR LEONARD
    leonard = df[df.player_name == player_name]
    if color == 1:
        cmap=plt.cm.YlOrRd_r
    else:
        cmap=plt.cm.winter

    # n_levels sets the number of contour lines for the main kde plot
    joint_shot_chart = sns.jointplot(leonard.x_loc, leonard.y_loc, stat_func=None,
                                     kind='kde', space=0, color=cmap(0.1),
                                     cmap=cmap, n_levels=50)


    name = player_name.split(' ', 1 )[1]


# class teamTable():
#     def __init__(self):

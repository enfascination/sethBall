clean:
	#rm ./code/*.pyc
	echo 'DROP DATABASE IF EXISTS nba_tracking;' > zzzhelp_dropdb.sql
	chmod u+x zzzhelp_dropdb.sql
	rm zzzhelp_dropdb.sql

build:
	python ./code/builddb.py

analysis_bruno:
	echo "This part of the pipeline includes scratch_analysis.py, mutual_information.py, utils.py, dataloader.py"
	echo "This is currently busted because add_possessions need to be made to work"
	python ./code/scratch_analysis.py

analysis:
	echo "This part of the pipeline includes pyBall.py, scripts.py, and iter_file.py"
	python ./code/scripts.py

pbp_parse:
	echo "Given play-by-play (not tracking) data, this produces a listing of various game-meaningful events by game time"
	python ./play_by_play_parse/pbp_parse.py

possession_edges:
	echo "Given output of pbp_parse, this breaks all games into the blocks in which one or another team was in control of the ball"
	python ./play_by_play_parse/possession_units.py



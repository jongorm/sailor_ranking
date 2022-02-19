Author: Jonathan Gorman

This project:

1) Scrapes data from techscore given a range of years (ex. 2011 and 2019) using BeautifulSoup and Requests.
2) It stores the data in a MySQL database which has four tables: sailors, teams, regattas, races.
3) The ELO system is implemented using a class called "EloRating" stored in elo_rating.py. 
4) The data collected from techscore is then cleaned and modeled with ELO using EloRating. This is performed in the 'techscore_analysis.ipynb' file.




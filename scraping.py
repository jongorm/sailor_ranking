import requests #Use to retrieve URL and webpage as an object
import logging
import re
import mysql
import lxml
import mysql.connector
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
from collections import defaultdict
from itertools import chain


#status code == 200
#Change appearance to chrome
#timeout=5
#add primary key
#unique
#not null

#apostrophe in names (also include hyphens) in the races auxiliary function
#error when loop finishes: "OperationalError: MySQL Connection not available."




def main():
    starttime = datetime.now()
    
    start_year = input('\nEnter year for lower bound of data collection range (YYYY): ')
    end_year = input('\nEnter year for upper bound of data collection range (YYYY): ')
    start_year_int = int(start_year) #convert to integer
    end_year_int = int(end_year) #convert to integer
    
    print("\n START COLLECTION. TIME: " + str(starttime.strftime('%Y/%m/%d at %H:%M:%S')))
    home_page_URL = "https://scores.collegesailing.org"
    con = mysql_db_connect() #Connect to database
    
    #make_sailors(home_page_URL, con, start_year_int, end_year_int)
    #make_regattas(home_page_URL, con, start_year_int, end_year_int)
    #make_teams(home_page_URL, con)
    #make_races(home_page_URL, con, start_year_int, end_year_int)
    table_alterations(con)
    con.close()



def soupify_link(link : str):
    try:
        response = requests.get(link, timeout=5)
        if response.status_code==200:
            response_soup = BeautifulSoup(response.content, 'lxml')
            return response_soup
        else:
            return None #response code is not acceptable
    except requests.Timeout as e:
        #log something here for e
        return None 
    except:
        raise Exception #raise other errors    


def make_regattas(base_URL : str, database : str, start_year_int : int, end_year_int : int): #Make a table of regattas for all desired seasons.       
    try: 
        cursor = database.cursor()
        seasons_link = base_URL + '/seasons'
        all_seasons_soup = soupify_link(seasons_link)
        if all_seasons_soup is None:
            pass
        else:    
            all_seasons_soup = all_seasons_soup.find('div', id='page-content') #Get subtree we need (page content)
            cursor.execute("DROP TABLE IF EXISTS regattas")
            cursor.execute("CREATE TABLE regattas (regatta_name VARCHAR(100) NOT NULL, \
                           start_date VARCHAR(10) NOT NULL, season_wo_year VARCHAR(8) NOT NULL,\
                           year SMALLINT UNSIGNED NOT NULL, regatta_host VARCHAR(100) NOT NULL, \
                           divisions VARCHAR(14) NOT NULL, regatta_type1 VARCHAR(50), \
                           regatta_type2 VARCHAR(50), boat_class VARCHAR(10), num_teams SMALLINT UNSIGNED, \
                           regatta_winner VARCHAR(100), num_races_each_div SMALLINT UNSIGNED, \
                           PRIMARY KEY(regatta_name, start_date))")
            database.commit()    #teams, sailors?
            print("CREATED TABLE regattas IN DATABASE\n")
            
            for season in all_seasons_soup.find_all('li'):
                season_year_list = season.find('span', class_='page-info-key').text.split(' ')
                parsed_year = season_year_list[1] 
                parsed_season = season_year_list[0]
                int_parsed_year = int(parsed_year)
                
                if (int_parsed_year >= start_year_int) and (int_parsed_year <= end_year_int): #use desired years
                    print("STARTING " + parsed_season + " " + parsed_year + " FOR regattas TABLE\n")
                    link_extension_season = season.find('span', class_='page-info-value').a['href'] #link extension for a season, /f17/ for example
                    season_page_link = base_URL + link_extension_season
                    season_page_soup = soupify_link(season_page_link)
                    if season_page_soup is None:
                        continue
                    else:
                        desired_regatta_scoring = ["2 Divisions", "1 Division", '3 Divisions'] #Only get those with type in this list
                        undesired_regatta_type = ["Scrimmage Regatta"] #Do not get scrimmage regattas
                        desired_status = ["Official"] #Regatta must be complete
                        desired_classes = ["FJ", "420/FJ", "420", "FJ/Z420", "Z420"] #Desired boat classes being competed in
                        
                        for regatta in season_page_soup.select('tr.row0, tr.row1'): #Get all regattas from the season specific page.
                            link_extension_regatta = regatta.a["href"]    
                            regatta_page_link = base_URL + link_extension_season + link_extension_regatta
                            regatta_soup = soupify_link(regatta_page_link)
                            if regatta_soup is None:
                                continue
                            else:
                                print("STARTING " + base_URL + link_extension_season + link_extension_regatta + " FOR regattas TABLE\n")
                                boat_class = regatta_soup.find('div', id="content-header").find_all('li')[3].find_all('span')[1].text #Boat class from regatta page                
                                regatta_traits_list = regatta.find_all('td') #List of regatta traits from season page
                                regatta_type1 = regatta_traits_list[2].text #Regatta type from season page
                                start_date = regatta_traits_list[4].text #Start date from season page
                                scoring_type = regatta_traits_list[3].text #Scoring format from season page
                                status_type = regatta_traits_list[5].text #Regatta status (We only want those with "Official", as specified in "desired_status")
                                if  (boat_class not in desired_classes) or (regatta_type1 in undesired_regatta_type) or \
                                    (scoring_type not in desired_regatta_scoring) or (status_type not in desired_status):
                                    continue
                                else:
                                    host = regatta_soup.find('div', id="content-header").find_all('li')[0].find_all('span')[1].text #Host school
                                    regatta_type2 = regatta_soup.find('div', id="content-header").find_all('li')[2].find_all('span')[1].text #Regatta type from regatta page
                                    regatta_name = regatta_soup.find('div', id="content-header").h1.find_all('span')[1].text #Regatta name from regatta page
                                    participants_list = regatta_soup.select('tr.row0, tr.row1') #A list of all teams html entries for the regatta
                                    winner_team = participants_list[0].a.find_all('span')[0].text #Winning team name
                                    num_teams = len(participants_list)
                                    
                                    full_scores_pattern = re.compile(r'href="([\w/-]+)">(Full\sScores)') #Regular expression pattern for full scores link in parsed html. used to get number of races
                                    link_extension_full_scores = re.search(full_scores_pattern, str(regatta_soup)).group(1)
                                    full_scores_link = base_URL + link_extension_full_scores
                                    full_scores_soup = soupify_link(full_scores_link)
                                    num_races = len(full_scores_soup.find('table', class_='results coordinate').find_all('th', class_='right')) - 1 #total races per division. Each division must have an equal number of races according to college regatta rules
                                    
                                    sql = "INSERT INTO regattas (regatta_name, start_date, season_wo_year, year, \
                                    regatta_host, divisions, regatta_type1, regatta_type2, boat_class, num_teams, \
                                    regatta_winner, num_races_each_div) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                                    vals = (regatta_name, start_date, parsed_season, parsed_year, host, scoring_type, \
                                            regatta_type1, regatta_type2, boat_class, num_teams, winner_team, num_races)
                                    print("ENTERING THE FOLLOWING INTO TABLE: \n", vals)
                                    cursor.execute(sql, vals)
                                    database.commit()
    except:
        raise Exception
    finally:
        cursor.close()



def make_teams(base_URL : str, database : str):
    try:
        cursor = database.cursor()
        all_teams_link = base_URL + '/schools'
        all_teams_soup = soupify_link(all_teams_link)
        if all_teams_soup is None:
            pass
        else:
            cursor.execute("DROP TABLE IF EXISTS teams")
            cursor.execute("CREATE TABLE teams (team_name_2 VARCHAR(100) PRIMARY KEY, team_name VARCHAR(100) NOT NULL, \
                           conference VARCHAR(10), location_town VARCHAR(30), location_region VARCHAR(30), \
                               num_seasons_active SMALLINT UNSIGNED)")
            database.commit()
            print("CREATED TABLE teams IN DATABASE\n")
            
            undesired_conferences = ['GUEST'] #We don't want any teams in the GUEST conference. There is typically only 1 team in this conference.
            
            for team in all_teams_soup.select('tr.row0, tr.row1'):
                location_list = team.find_all('td', class_='')
                town_location = location_list[0].text
                region_location = location_list[1].text
                team_name = team.a.text
                team_name_2_pattern = re.compile(r'href="/schools/([\w-]+)/')
                team_name_2 = re.search(team_name_2_pattern, str(team)).group(1)
                
                link_extension_team = team.a["href"]
                team_page_link = base_URL + link_extension_team
                team_page_soup = soupify_link(team_page_link)
                if team_page_soup is None:
                    continue
                else:
                    team_info_list = team_page_soup.find('ul', id='page-info').find_all('li')
                    conference_name = team_info_list[0].a.text #get conference name
                    
                    if conference_name not in undesired_conferences: #continue collection data as long as the team is in the desired conference
                        print("STARTING " + base_URL + link_extension_team + " FOR teams TABLE")
            
                        #team_code = team_info_list[1].find_all('span')[1].text #get team code
                        if team_page_soup.find('ul', id="other-seasons"):    
                            num_active_seasons = len(team_page_soup.find('ul', id="other-seasons").find_all("li"))  #Get number of active seasons from ohter seasons footer
                        else:
                            num_active_seasons = 0
                            
                        sql = "INSERT INTO teams (team_name, team_name_2, conference, location_town, \
                            location_region, num_seasons_active) VALUES (%s, %s, %s, %s, %s, %s)"
                        vals = (team_name, team_name_2, conference_name, town_location, region_location, num_active_seasons) 
                        print("ENTER THE FOLLOWING INTO TABLE: \n", vals)
                        cursor.execute(sql, vals)
                        database.commit()
                    else:
                        continue
    except:
        raise Exception
    finally:
        cursor.close()
        


def make_sailors(base_URL : str, database : str, start_year_int : int, end_year_int : int):
    try:
        cursor = database.cursor()
        all_teams_link = base_URL + '/schools'
        all_teams_soup = soupify_link(all_teams_link)
        if all_teams_soup is None:
            pass
        else:
            cursor.execute("DROP TABLE IF EXISTS sailors")
            cursor.execute("CREATE TABLE sailors (sailor_name VARCHAR(60) NOT NULL, sailor_team VARCHAR(100) NOT NULL, \
                           sailor_team_2 VARCHAR(100) NOT NULL, grad_year SMALLINT UNSIGNED, conference VARCHAR(10), \
                               latest_season VARCHAR(25), PRIMARY KEY(sailor_name, sailor_team))")
            database.commit()
            print("CREATED TABLE sailors IN DATABASE\n")
            
            for team in all_teams_soup.select('tr.row0, tr.row1'):
                link_extension_team = team.a['href'] #Get team page link from teams page
                
                if link_extension_team=="/schools/icsa-selectors/": #Skip this team, it is a placeholder on the website and should be ignored.
                    continue
                
                team_name_2_pattern = re.compile(r'href="/schools/([\w-]+)/') #Get team name from link extension
                team_name_2 = re.search(team_name_2_pattern, str(team)).group(1)
                
                print("STARTING TEAM ", team_name_2, " FOR sailors")
     
                team_page_link = base_URL + link_extension_team
                team_page_soup = soupify_link(team_page_link)
                if team_page_soup is None:
                    continue
                else:
                    team_name = team_page_soup.find("div", id="content-header").find("span", itemprop="name").text #Get full team name
                    
                    for submenu_season_tab in team_page_soup.find("ul", id="other-seasons").find_all('li'): #The goal of this loop is to get to the roster page. The main team page or even the latest season team page may not have a roster page (404), 
                        link_extension_season = submenu_season_tab.a['href']                                #so we must find the season that has the first 200 status code roster page, then iterate through that page's footer to go through all season's rosters.
                        team_page_season_link = base_URL + link_extension_season
                        team_page_season_soup = soupify_link(team_page_season_link)
                        if team_page_season_soup is None:
                            continue
                        else:
                        
                            roster_link_pattern = re.compile(r'href="([\w/-]+)">Roster') #regex pattern for roster tab link
                            link_extension_roster = re.search(roster_link_pattern, str(team_page_season_soup)).group(1) #first match in regular expression pattern
                            roster_page = requests.get(base_URL + link_extension_roster, timeout=5)
                            
                            if roster_page.status_code==200: #page exists, start here
                                roster_page_soup = BeautifulSoup(roster_page.content, 'lxml') #parsed html for the initial roster page
                                break
                            else: #page does not exist, try next season
                                continue
                            
                        for roster_season_tab in roster_page_soup.find("div", id="submenu-wrapper").find_all('li'):
                            link_extension_season_roster = roster_season_tab.a['href'] #link extension for a season's roster from the footer
                            roster_year = int(roster_season_tab.a.text.split(' ')[1])
                            
                            if (roster_year >= start_year_int) and (roster_year <= end_year_int):
                                roster_season_page = requests.get(base_URL + link_extension_season_roster, timeout=5)
                                
                                roster_season_page_soup = BeautifulSoup(roster_season_page.content, 'lxml')
                                sailor_link_pattern = re.compile(r'<a href="([\w\d/-]+)" itemprop="name">') #We are using a regex because some of the name hyperlinks are not true hyperlinks (they are dead links, absent in the html).
                                roster_table = roster_season_page_soup.find("div", class_="port")
                                
                                for link in sailor_link_pattern.findall(str(roster_table)):
                                    sailor_page = requests.get(base_URL + link, timeout=5) #findall returns a list of tuples, so the link is in the first (and only) element of the tuple, which corresponds to the only group in the regex pattern.
                                    sailor_page_soup = BeautifulSoup(sailor_page.content, 'lxml')
                                    
                                    if not sailor_page_soup.find('div', id='content-header'):
                                        continue #Some sailor pages are empty. So if the info header is absent, skip the sailor
                                    
                                    try:
                                        sailor_name_pattern = re.compile(r'itemprop="name">([\w@\"\#\*\%\$\!\s\(\)\'’\.-]+)<')
                                        sailor_name = re.search(sailor_name_pattern, str(sailor_page_soup.find('div', \
                                                                                                id='content-header'))).group(1)
                                    except AttributeError: #Sailor name has unvalid characters
                                        sailor_name = "CHARACTER_ERROR"
            
                                    info_list = sailor_page_soup.find('ul', id="page-info").find_all('li')
                                    grad_year = info_list[0].find('span', class_="page-info-value").text
                                    conference = info_list[2].find('span', class_="page-info-value").text
                                    latest_season = sailor_page_soup.find('div', class_="port", id="history").h3.a.text
            
                                    sql = "INSERT INTO sailors (sailor_name, sailor_team, sailor_team_2, grad_year, conference, \
                                        latest_season) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE sailor_name=sailor_name, \
                                        sailor_team=sailor_team"
                                    vals = (sailor_name, team_name, team_name_2, grad_year, conference, latest_season)
                                    cursor.execute(sql, vals)
                                    print("ENTER THE FOLLOWING INTO TABLE: \n", vals)
                                    database.commit()
                            else:
                                continue
    except:
        raise Exception
    finally:
        cursor.close()
        


def bounds_to_range(range_list): #turns list of form [["1", "4"], ["12", "16"]] to [range(1,5), range(12,16)], can handle [["1"], ["5", "7"]], where an isolated race was sailed.
    try:
        range_list[1]
    except IndexError:
        return list(range(int(range_list[0]), int(range_list[0])+1))
    else:
        return list(range(int(range_list[0]), int(range_list[1])+1))


def name_for_race(sailors_URL : str, num_races : int):
    try:
        sailors_soup = soupify_link(sailors_URL)
        if sailors_soup is None:
            print("FAIL 1\n")
            return
        else:
            #The two divisions are essentially separate regattas in terms of sailors competing against each other, so they are handled separately.
            division_A_sailors = defaultdict(dict) #Nested dictionaries to store team, race number, and sailor for division A
            division_B_sailors = defaultdict(dict) #Nested dictionaries to store team, race number, and sailor for division B
            division_C_sailors = defaultdict(dict) #Nested dictionaries to store team, race number, and sailor for division C
            
            for element in sailors_soup.find('div', class_="port").select('tr.row1, tr.row0'): #This only obtains the relevant rows in the sailors table.
                #'element' is a chunk of html for a row in the sailors table
                if element.find('td', title="Reserves") is None: #skip reserves rows
                    if element.find('td', class_="division-cell"):
                        current_division = element.find('td', class_="division-cell").text
                    if element.find('td', class_="schoolname"): #get school name from the link so it matches the school name on the full scores page.
                        school_link_name_pattern = re.compile(r'href="/schools/([\w-]+)/')
                        current_school = re.search(school_link_name_pattern, str(element)).group(1) #get the specified group in the regex pattern
                    if element.find('td', class_="teamname"):
                        current_subteam = element.find('td', class_="teamname").text
                        
                    row_element_list = element.find_all('td') #List of row elements
                    
                    if row_element_list[-4].text: #check if the row has a skipper name, if not, skip the row
                        sailor_name_pattern = re.compile(r'([\w@\"\#\*\%\$\!\s\(\)\'’\.-]+) \'\d\d') #Get name without year of graduation
                        sailor_name = re.search(sailor_name_pattern, str(row_element_list[-4].text)).group(1)
                    else:
                        continue
                    if row_element_list[-3].text: #check if the row specifies the races, if so, make a list of those races
                        split_strip = map(str.strip, row_element_list[-3].text.split(",")) #list of form ["1-4", "12-16"]
                        list_of_nums = map(lambda x: x.split("-"), split_strip) #list of form [["1", "4"], ["12", "16"]]
                        range_vals = map(bounds_to_range, list_of_nums) #creates [[1,2,3,4], [12,13,14,15,16]]
                        sailors_races = chain.from_iterable(range_vals) #creates the full list [1,2,3,4,12,13,14,15,16]
                    else: #if the row does not specify the range of races, the sailor competed in all races, so make a list of all races.
                        sailors_races = range(1, num_races+1)
                    
                    if current_division=="A":
                        for race in sailors_races:
                            division_A_sailors[current_subteam][race] = (sailor_name, current_school)
                    elif current_division=="B":
                        for race in sailors_races:
                            division_B_sailors[current_subteam][race] = (sailor_name, current_school)
                    elif current_division=="C":
                        for race in sailors_races:
                            division_C_sailors[current_subteam][race] = (sailor_name, current_school)
                else:
                    continue
    
    except:
        raise Exception
        
    finally:
        return division_A_sailors, division_B_sailors, division_C_sailors



def make_races(base_URL : str, database : str, start_year_int : int, end_year_int : int):
    try:
        cursor = database.cursor()
        cursor.execute("DROP TABLE IF EXISTS races")
        cursor.execute("CREATE TABLE races (regatta_name VARCHAR(100) NOT NULL, start_date VARCHAR(10) NOT NULL, \
                       sailor_name VARCHAR(60) NOT NULL, team_name VARCHAR(100) NOT NULL, subteam_name VARCHAR(100) NOT NULL, \
                           division VARCHAR(4), race_number SMALLINT NOT NULL, finishing_place VARCHAR(5) NOT NULL)")
        database.commit()
        print("CREATED TABLE races IN DATABASE")
        
        all_seasons_link = base_URL + '/seasons'
        all_seasons_soup = soupify_link(all_seasons_link)
        if all_seasons_soup is None:
            return
        else:
            all_seasons_soup = all_seasons_soup.find('div', id='page-content')
            
            for season in all_seasons_soup.find_all('li'):
                season_year_list = season.find('span', class_='page-info-key').text.split(' ')
                parsed_season = season_year_list[0]
                parsed_year = season_year_list[1]
                int_parsed_year = int(parsed_year)
                
                if (int_parsed_year >= start_year_int) and (int_parsed_year <= end_year_int):
                    print("STARTING " + parsed_season + " " + parsed_year + " FOR races TABLE\n")
                    link_extension_season = season.find('span', class_='page-info-value').a['href'] #link extension for a season, /f17/ for example
                    season_page_link = base_URL + link_extension_season
                    season_page_soup = soupify_link(season_page_link)
                    if season_page_soup is None:
                        continue
                    else:
                        
                        desired_regatta_scoring = ["2 Divisions", "1 Division", '3 Divisions'] #Only get those with type in this list
                        undesired_regatta_type = ["Scrimmage Regatta"] #Do not get scrimmage regattas
                        desired_status = ["Official"] #Regatta must be complete
                        desired_classes = ["FJ", "420/FJ", "420", "FJ/Z420", "Z420"] #Desired boat classes being competed in
                        
                        for regatta in season_page_soup.select('tr.row0, tr.row1'): #Get all regattas from the season specific page.
                        
                            link_extension_regatta = regatta.a["href"]
                            regatta_page_link = base_URL + link_extension_season + link_extension_regatta + '/'
                            regatta_soup = soupify_link(regatta_page_link)
                            if regatta_soup is None:
                                continue
                            else:
                                
                                print("STARTING " + base_URL + link_extension_season + link_extension_regatta + " FOR races TABLE\n")
                                
                                boat_class = regatta_soup.find('div', id="content-header").find_all('li')[3].find_all('span')[1].text #Boat class from regatta page                
                                regatta_traits_list = regatta.find_all('td') #List of regatta traits from season page
                                regatta_type1 = regatta_traits_list[2].text #Regatta type from season page
                                start_date = regatta_traits_list[4].text #Start date from season page
                                scoring_type = regatta_traits_list[3].text #Scoring format from season page
                                status_type = regatta_traits_list[5].text #Regatta status (We only want those with "Official", as specified in "desired_status")
                                
                                if  (boat_class not in desired_classes) or (regatta_type1 in undesired_regatta_type) or \
                                    (scoring_type not in desired_regatta_scoring) or (status_type not in desired_status):
                                    continue
                                else:
                                    #We need to check that the sailors tab exists for the regatta. If not, we will skip it.
                                    try:
                                        sailors_tab_pattern = re.compile(r'href="([\w\d/-]+)">Sailors</a>')
                                        header_soup = regatta_soup.find('div', id="menu-wrapper")
                                        sailors_link_extension = re.search(sailors_tab_pattern, str(header_soup)).group(1)
                                    except AttributeError:
                                        print("CONTINUE TO NEXT REGATTA, NO SAILORS TAB.\n")
                                        continue #skip if the link extension does not exist (ie. sailors tab is not present)
                                    
                                    sailors_URL = base_URL + sailors_link_extension
                                    #get number of races
                                    full_scores_pattern = re.compile(r'href="([\w/-]+)">(Full\sScores)') #Regular expression pattern for full scores link in parsed html. used to get number of races
                                    link_extension_full_scores = full_scores_pattern.findall(str(regatta_soup))[0][0] #First match, first group
                                    regattafull_page_link = base_URL + link_extension_full_scores
                                    regattafull_soup = soupify_link(regattafull_page_link)
                                    if regattafull_soup is None:
                                        continue
                                    else:
                                        num_races = len(regattafull_soup.find('table', class_='results coordinate').find_all('th', class_='right')) - 1
                                        regatta_name = regattafull_soup.find('div', id="content-header").h1.find_all('span')[1].text #Regatta name from full scores page
                                            
                                        division_A, division_B, division_C = name_for_race(sailors_URL, num_races) #Call function to get skippers and the races they competed in.
                                        
                                        for team_div_results in reversed(regattafull_soup.find('table', class_="results coordinate").select('tr.divB, tr.divA, tr.divC')):
                                            #within the row, look for a subteam name                                
                                            subteam_name_pattern = re.compile(r'([\s\w\d\']+)</td><td class="strong">')
                                            current_subteam_candidate = re.search(subteam_name_pattern, str(team_div_results))
                                            if current_subteam_candidate:
                                                current_subteam = current_subteam_candidate.group(1)
                                            
                                            if scoring_type=="1 Division": #divisions are not listed when there is only one division in the regatta, so just set it to A.
                                                current_division = "A"
                                            elif team_div_results.find('td', class_='strong'):
                                                current_division = team_div_results.find('td', class_='strong').text
                                            #Now iterate through races by obtaining the score in a regex
                                            score_pattern = re.compile(r'td\sclass="right"\s(title=""|title="\([\w\s\d\+,:]+\)[\s\w]*")>(\d\d?|\w{3})<') #Finds score (1 to max number of boats) or text for DSQ, DNF, BKD, etc..
                                            
                                            for race_num, score in enumerate(score_pattern.findall(str(team_div_results)), 1):
                                                try:
                                                    if current_division=="A":
                                                        sailor_name = division_A[current_subteam][race_num][0]
                                                        current_school = division_A[current_subteam][race_num][1]
                                                    elif current_division=="B":
                                                        sailor_name = division_B[current_subteam][race_num][0]
                                                        current_school = division_B[current_subteam][race_num][1]
                                                    elif current_division=="C": 
                                                        sailor_name = division_C[current_subteam][race_num][0]
                                                        current_school = division_C[current_subteam][race_num][1]
                                                except KeyError:
                                                    sailor_name = "KEY_ERROR"
                                                    current_school = "KEY_ERROR"
                                                
                                                if sailor_name=="":
                                                    sailor_name = "NOT_LISTED"
                
                                                sql = "INSERT INTO races (regatta_name, start_date, division, sailor_name, \
                                                    team_name, subteam_name, race_number, finishing_place) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                                                vals = (regatta_name, start_date, current_division, sailor_name, \
                                                        current_school, current_subteam, race_num, score[1])
                                                #print("ENTER THE FOLLOWING INTO TABLE: \n", vals)
                                                cursor.execute(sql, vals)
                                                database.commit()                         
    except:
        raise Exception
        
    finally:
        cursor.close()
        
        
        
        
def table_alterations(database : str): #Makes foreign keys
    cursor = database.cursor()
    cursor.execute("ALTER TABLE races ADD FOREIGN KEY(team_name) REFERENCES teams(team_name_2) ON DELETE SET NULL, \
                   FOREIGN KEY(regatta_name, start_date) REFERENCES regattas(regatta_name, start_date) ON DELETE SET NULL, \
                       FOREIGN KEY(sailor_name, team_name) REFERENCES sailors(sailor_name, sailor_team_2) ON DELETE SET NULL")
    cursor.execute("ALTER TABLE sailors ADD FOREIGN KEY(team_name_2) REFERENCES teams(team_name_2) ON DELETE SET NULL")
    database.commit()
    
    

def mysql_db_connect():
    host = input("\nHost Name: ")
    user = input("\nUser Name: ")
    passwd = input("\nPassword: ")
    database = input("\nDatabase Name: ")
    try:
        db_connection = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )
    except:
        raise Exception
    else:
        print(f"CONNECTION TO: {host} ESTABLISHED")
        return db_connection
    
            

if __name__ == '__main__':
    try:
        main()
    except:
        raise Exception







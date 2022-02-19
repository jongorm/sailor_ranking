
from collections import defaultdict
import os
import csv

class EloRating:
    def __init__(self, k_factor=32, scale=400, init_elo=1200):
        self.__k = k_factor
        self.__scale = scale
        self.__rankings = defaultdict(list)
        self.__init_elo = init_elo
    
    #getters
    
    def rankings(self):
        return self.__rankings
    
    def item_val(self, identifier, association=None):
        #get the value for a given key. Association could
        #be a team or group necessary to uniquely identify
        #a participant.
        if association:
            identifier = identifier + '_' + association
        if identifier in self.__rankings:
            return self.__rankings[identifier]
        else:
            print(f"Item '{identifier}' does not exist in the key set.")
            return None
        
    def keys_with_elo(self, elo): 
        #return a list of keys with value elo.
        keys_with_elo = [key for key, val in self.__rankings.items() if val[0]==elo]
        return keys_with_elo
    
    #modifiers
    
    def modify_rating(self, identifier, new_rating, increment_events=True, association=None):
        #changes a key's rating. It will increment the number of events
        #the key has participated in by default. This can be bypassed
        #by setting increment_events to False.
        if association:
            identifier = identifier + '_' + association
        if identifier not in self.__rankings:
            print(f"Item '{identifier}' does not exist in the key set.")
        if increment_events is True: #change rating and nos. events 
            self.__rankings[identifier][0] = new_rating
            self.__rankings[identifier][1] += 1
        else:
            self.__rankings[identifier] = new_rating
        
        
    def modify_events(self, identifier, new_events, association=None):
        #changes a key's events. 
        if association:
            identifier = identifier + '_' + association
        if identifier not in self.__rankings:
            print(f"Item '{identifier}' does not exist in the key set.")
        self.__rankings[identifier][1] = new_events
    
    def add_item(self, identifier, association=None, init_elo=None, init_events=0, overwrite=True):
        if init_elo is None:
            init_elo = self.__init_elo
        if association:
            identifier = identifier + '_' + association
        if overwrite is True: #if key exists, overwrite
            if identifier in self.__rankings:
                self.__rankings[identifier][0] = init_elo
                self.__rankings[identifier][1] = init_events #The number of events competed in.        
            else:
                self.__rankings[identifier].append(init_elo)
                self.__rankings[identifier].append(init_events)
        else:
            if identifier in self.__rankings:
                pass #print(f"Key '{identifier}' already present in rankings. Set overwrite to True to proceed.")
            else:
                self.__rankings[identifier].append(init_elo)
                self.__rankings[identifier].append(init_events)
                    
    def delete_item(self, identifier, association=None):
        if association:
            identifier = identifier + '_' + association
        if identifier in self.__rankings:
            del self.__rankings[identifier]
        else:
            print(f"Attempted to delete '{identifier}', key does not exist.")
            
    def clear_rankings(self):
        #clear the dictionary.
        self.__rankings.clear()
        
    def reset_rankings(self, reset_rating=None, reset_events=0):
        if reset_rating is None:
            reset_rating = self.__init_elo
        self.__rankings.update((key, [reset_rating, reset_events]) for key in self.__rankings)
                           
    #ranking methods
    
    def win_metric_lower(self, itemA_result, itemB_result):
        #This is how a win is defined is a lower score is better.
        #The return value will be used for the score S in the rating
        #update function update().
        if itemA_result < itemB_result:
            return 1.0 #win for itemA
        elif itemA_result==itemB_result:
            return 0.5 #draw
        else:
            return 0.0 #loss for itemA

    def win_metric_higher(self, itemA_result, itemB_result):
        #This is how a win is defined is a higher score is better.
        #The return value will be used for the score S in the rating
        #update function update().
        if itemA_result > itemB_result:
            return 1.0 #win for itemA
        elif itemA_result==itemB_result:
            return 0.5 #draw
        else:
            return 0.0 #loss for itemA
    
    def expected_func(self, itemA_rating, itemB_rating):
        return 1 / (1 + pow(10, (itemB_rating - itemA_rating) / self.__scale))
    
    def update_func(self, outcome, expected_outcome, current_rating):
        return current_rating + self.__k*(outcome - expected_outcome)
    
    def faceoff(self, itemA_identifier, itemB_identifier, itemA_result, itemB_result, itemA_association=None, itemB_association=None, win_metric=None):
        if win_metric is None: #default win metric
            win_metric = self.win_metric_lower
        if itemA_association:
            itemA_identifier = itemA_identifier + '_' + itemA_association
        if itemB_association:
            itemB_identifier = itemB_identifier + '_' + itemB_association
        #Add item if not already present
        self.add_item(itemA_identifier, overwrite=False)
        self.add_item(itemB_identifier, overwrite=False)
        #Get item values
        itemA_rating = self.item_val(itemA_identifier)[0]
        itemB_rating = self.item_val(itemB_identifier)[0]
        #Calculate expected outcome of faceoff
        itemA_expected = self.expected_func(itemA_rating, itemB_rating)
        itemB_expected = self.expected_func(itemB_rating, itemA_rating)
        #Calculate outcomes for both participants according to metric
        itemA_outcome = win_metric(itemA_result, itemB_result)
        itemB_outcome = win_metric(itemB_result, itemA_result)
        #Update vals according to outcomes and expected outcomes.
        itemA_updated = self.update_func(itemA_outcome, itemA_expected, itemA_rating)
        itemB_updated = self.update_func(itemB_outcome, itemB_expected, itemB_rating)
        return {itemA_identifier : itemA_updated, itemB_identifier : itemB_updated}
            
    def sort_copy(self, reverse=False): #sort keys according to elo ratings
        return {key : val for key, val in sorted(self.__rankings.items(), key = lambda item : item[1][0], reverse=reverse)}
    
    def sort(self, reverse=False): #replace the internal dictionary with a sorted one according to elo.
        self.__rankings = {key : val for key, val in sorted(self.__rankings.items(), key = lambda item : item[1][0], reverse=reverse)}
    
    def rankings_to_csv(self, output_csv_path):
        try:
            os.remove(output_csv_path)
        except OSError:
            pass #if the file does not exist, pass
        with open(output_csv_path, 'w') as file:
            row_writer = csv.writer(file, delimiter=',')
            row_writer.writerow(["Index", "Name", "Rating", "Num_Faceoffs"])
            for row, (key, val) in enumerate(self.__rankings.items(), 1):
                row_writer.writerow([row, key, val[0], val[1]])
        
        

        
    
    



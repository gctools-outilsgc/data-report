#Not part of report. Additional code for data requests. One is average number of users, currently for april 2019. Teh other for number of new users within a period

from pymongo import MongoClient
import datetime
import calendar
import pandas as pd

def avgOnlineUsers(): 

    myclient = MongoClient("mongodb://localhost:27017/")
    db = myclient["rocketchat"]

    START = datetime.datetime(2019, 4, 1, 0, 0, 0, 0)
    END = datetime.datetime(2019, 4, 30, 23, 59, 59, 999999)

    stats = db["rocketchat_statistics"].find( { 'createdAt' : { '$gte' : START, '$lt' : END } } )

    sum = 0
    max = 0
    count = 0
    allNums = []

    for i in stats:
        if  not (
            i["createdAt"].hour < 7 or              #don't include too early
            i["createdAt"].hour > 18 or             #don't include too late
            i["createdAt"].isoweekday() > 5 or      # don't include weekends
            i["createdAt"].day == 19 or             #a week-day holiday
            i["createdAt"].day == 22):              #a week-day holiday

            sum = sum + i['onlineUsers']
            if i['onlineUsers'] > max:
                max = i['onlineUsers']
            count = count + 1
            allNums.append( i['onlineUsers'] )
    
    print("The average is:", sum / count) 
    print ("The max is:", max) 
    allNums.sort
    print ("The median is:", allNums[round(len(allNums) / 2)] ) 
    print("Number of data points:", count)


def numNewUsers():
    
    myclient = MongoClient("mongodb://localhost:27017/")
    db = myclient["rocketchat"]

    START = datetime.datetime(2019, 8, 1, 0, 0, 0, 0)
    END = datetime.datetime(2019, 12, 1, 0, 0, 0, 0)

    new_users = db["users"].find( { 'createdAt' : { '$gte' : START, '$lt' : END } }, { 'emails' : 1, 'name': 1, '_id': 0 } )

    curr_ends = []

    count = 0
    for user in new_users:
        user = user["emails"][0]["address"]
        print(user)
        count += 1
        try:
            user = user.split("@")[1]
            curr_ends.append(user)
        except:
            print("bad", user)


    ends = pd.Series(curr_ends).value_counts()

    print(ends)

    #ends.to_csv("data.csv")

    print("num new users since aug 1: ", count)

    prev_start = datetime.datetime(2018, 8, 1, 0, 0, 0, 0)
    prev_end = datetime.datetime(2018, 12, 1, 0, 0, 0, 0)
    
    prev_users = db["users"].find( { 'createdAt' : { '$gte' : prev_start, '$lt' : prev_end,  } }, { 'emails' : 1, 'name': 1, '_id': 0 } )
    
    old_count = 0
    for user in prev_users:
        old_count += 1
        #print (user)

    print("num new users since aug 1 2018: ", old_count)

numNewUsers()
import os
import requests
import json
import time
from datetime import datetime
import array as arr
import random
from csv import DictReader
from csv import reader
import sys
from random import choice
from configparser import ConfigParser
from confluent_kafka import Producer



#path for dictionary files
cwd = os.getcwd()

sUserFile = cwd + '/users.csv'
sItemFile = cwd + '/items.csv'
config_file = "gaminganalytics-secret.ini"

# Parse the confluent cloud configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(open(cwd + '/' + config_file))
config = dict(config_parser['default'])

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
#def delivery_callback(err, msg):
#    if err:
#        print('ERROR: Message failed delivery: {}'.format(err))
    #else:
        #print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
def emit(producer, topic, emitRecord):
  global msgCount
  sid = ''
  if producer is None:
    print(f'{sid}|{json.dumps(emitRecord)}')
  else:
    producer.produce(topic, key=str(sid), value=json.dumps(emitRecord))
    msgCount += 1
    if msgCount >= 2000:
      producer.flush()
      msgCount = 0
    producer.poll(0)

def emitLogin(p, t):
  emitRecord = {
      'event_time' : iso_date,
      'Event_Type' : 'login',
      'Campaign_name' : '',
      'Platform' :  sPlatform ,
      'Version' :  sVersion,
      'Item_Name' : '',
      'Country' : sCountry,
      'Event_revenue' : '0',
      'User_Id' : sUserID,
      'Game_duration' : '0'
  }
  emit(p, t, emitRecord)

def emitMission(p, t):
  emitRecord = {
      'event_time': iso_date,
      'Event_Type': sMission,
      'Campaign_name': '',
      'Platform': sPlatform,
      'Version': sVersion,
      'Item_Name': '',
      'Country': sCountry,
      'Event_revenue': '0',
      'User_Id': sUserID,
      'Game_duration': lDuration
    }
  emit(p, t, emitRecord)

def emitItem(p, t, sCamp):
  emitRecord = {
      'event_time': iso_date,
      'Event_Type': 'Item_interaction',
      'Campaign_name': sCamp,
      'Platform': sPlatform,
      'Version': sVersion,
      'Item_Name': Item_Name,
      'Country': sCountry,
      'Event_revenue': Event_revenue,
      'User_Id': sUserID,
      'Game_duration': '0'
    }
  emit(p, t, emitRecord)

#define CC Topic Name for Gaming Analytics
topic = "gaming-analytics"
msgCount=0
# Create Confluent Cloud Producer instance
producer = Producer(config)

#fill user dictionnary
# read user file as a list of lists
with open(sUserFile, 'r') as read_obj:
    # pass the file object to reader() to get the reader object
    csv_reader = reader(read_obj)
    # Pass reader object to list() to get a list of lists
    DictUsers = list(csv_reader)

#fill item dictionnary
# read item file as a list of lists
with open(sItemFile, 'r') as read_obj:
    # pass the file object to reader() to get the reader object
    csv_reader = reader(read_obj)
    # Pass reader object to list() to get a list of lists
    DictItems = list(csv_reader)
#defining target weekly # of events per category
iLogins = 4000000
iSingleMissions = 10000000
iMultiMissions = 5000000
iItemInteractions = 1000000
sVersion = "1.3.3"


#assigning weekday weights 0=Monday, 1= Tuesday, 6=Sunday), total is sum of all weights elements
weekweights = arr.array('d',[0.8,1,1,1.1,1.5,1.7,2])
totweekWeight = 9.1
#assigning hourly weights for events (0 to 23, hours per day), total is sum of all weights elements
hourlyweights = arr.array('d',[1,0.7,0.5,0.4,0.2,0.1,0.2,0.6,0.7,0.8,0.9,1,1,1.1,1.2,1.2,1.3,1.3,1.5,1.7,1.5,1.4,1.3,1.2])
totalhourlyweights = 22.9
#assigning 2nd series of hourly weights for events (0 to 23, hours per day), total is sum of all weights elements
hourlyweights1 = arr.array('d',[0.2,0.2,0.1,0.1,0.1,0.2,0.4,0.6,0.7,0.8,0.9,1,1,1.2,1.4,1.3,1.1,1,0.9,0.8,0.7,0.5,0.4,0.2])
totalhourlyweights1 = 15.8
#assigning 3rd series of hourly weights for events (0 to 23, hours per day), total is sum of all weights elements
hourlyweights2 = arr.array('d',[1.4,1.3,1.1,1,0.5,0.4,0.2,0.6,0.7,0.8,0.9,1,1,1.1,1.6,1.7,1.8,1.9,1.9,2,1.8,1.7,1.6,1.5])
totalhourlyweights2 = 29.5
#assigning 4th series of hourly weights for events (0 to 23, hours per day), total is sum of all weights elements
hourlyweights3 = arr.array('d',[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,3,4,2,2,1])
totalhourlyweights3 = 34

iSec = 1
while iSec >0:
  #get current timestamp information
  today = datetime.now()
  curweekday = today.weekday()
  curhour = today.hour
  iso_date = today.isoformat()
  curweekday = today.weekday()
  sCampaign = ""
  lDuration = '0'

  #determine # of logins per current day
  NbLoginsperDay = iLogins * (weekweights[curweekday] /totweekWeight )
  NbLoginsperDay = NbLoginsperDay + int(random.randint(0, 50)) * NbLoginsperDay / 100
  #print ("#################################")
  #determine # of logins per current hour
  NbLoginsperHour = int(NbLoginsperDay * (hourlyweights[curhour] / totalhourlyweights))
  # determine # of logins per second to generate
  NbLoginperSec = (NbLoginsperHour / 60 / 60)
  #print ("Nb logins per sec:" + str(int(NbLoginperSec)))

  # determine # of Single Player missions per current day
  NbSingleMissionperDay = iSingleMissions * (weekweights[curweekday] / totweekWeight)
  NbSingleMissionperDay = NbSingleMissionperDay + (int(random.randint(0, 100)) * NbSingleMissionperDay / 100)*random.randint(0,1)*2-1
  # determine # of Single Player missions per current hour
  NbSingleMissionperHour = int(NbSingleMissionperDay * (hourlyweights1[curhour] / totalhourlyweights1))
  # determine # of Single Player missions per second to generate
  NbSingleMissionperSec = NbSingleMissionperHour / 60 / 60

  #print("Nb single mission per sec:" + str(int(NbSingleMissionperSec)))

  # determine # of Multiplayer Player missions per current day
  NbMultiMissionperDay = iMultiMissions * (weekweights[curweekday] / totweekWeight)
  NbMultiMissionperDay = NbMultiMissionperDay + (int(random.randint(0, 80)) * NbMultiMissionperDay / 100)*random.randint(0,1)*2-1
  # determine # of Multi Player missions per current hour
  NbMultiMissionperHour = int(NbMultiMissionperDay * (hourlyweights2[curhour] / totalhourlyweights2))
  # determine # of Multi Player missions per second to generate
  NbMultiMissionperSec = NbMultiMissionperHour / 60 / 60
  # on wednesday, display a marketing campaign to boost multiplayer and boost multiplayer events * 1,6


  # determine # of item interactions  per current day
  NbItemperDay = iItemInteractions * (weekweights[curweekday] / totweekWeight)
  NbItemperDay = NbItemperDay + int(random.randint(0, 100)) * NbItemperDay / 100
  # determine # of item interactions per current hour
  NbItemperHour = int(NbItemperDay * (hourlyweights3[curhour] / totalhourlyweights3))
  # determine # of item interactions per second to generate
  NbItemperSec = NbItemperHour / 60 / 60
  NbItemperSec = NbItemperSec + int(random.randint(0, 100)) * NbItemperSec / 100

  if NbItemperSec <1 :
    NbItemperSec=1
  #print("Nb item interaction per sec:" + str(int(NbItemperSec)))

  #On Wednesday, Generate campaign impressions in the game, affecting Multi player usage and revenue
  if curweekday == 2:
    sCampaign = "Mid week MP campaign boost"
    NbMultiMissionperSec = NbMultiMissionperSec * 1.6
  #print("Nb Multi mission per sec:" + str(int(NbMultiMissionperSec)))

  # on saturday, Generate campaign impressions in the game, affecting item interactions during this time
  if curweekday == 5 and curhour > 16 and curhour < 20:
    sCampaign = "Saturday Item discount"
    NbItemperSec = NbItemperSec * 1.5

  #Generate current day/hour payload information for the 4 event categories

  #generate login information
  sMission=''
  NbDistUsers = int(NbLoginsperHour)

  i=1
  rndUserindex = random.randint(2, NbDistUsers)
  while i < NbLoginperSec:
    # generate user information from User dictionary
    # get random index from distinct user count of the day
    # read information from user dictionary
    if int(random.randint(0, 4)) == 1:
      rndUserindex = random.randint(2, NbDistUsers)

    sUserID = DictUsers[rndUserindex - 1][0]
    sCountry = DictUsers[rndUserindex - 1][1]
    sPlatform = DictUsers[rndUserindex - 1][2]

    #produce Login events to confluent cloud
    emitLogin(producer,topic)

    i += 1

  # generate single player mission information
  i = 1

  sMission = 'Single_player_mission'
  rndUserindex = random.randint(2, NbDistUsers)
  while i < NbSingleMissionperSec:
    # generate user information from User dictionary
    # get random index from distinct user count of the day
    if int(random.randint(0, 2)) == 1:
      rndUserindex = random.randint(2, NbDistUsers)
    # read information from user dictionary
    sUserID = DictUsers[rndUserindex - 1][0]
    sCountry = DictUsers[rndUserindex - 1][1]
    sPlatform = DictUsers[rndUserindex - 1][2]

    lDuration = str(random.randint(1, 80))

    #produce single mission event to CC
    emitMission(producer,topic)

    i += 1


  # generate Multi player mission information
  i = 1

  sMission = 'Multi_player_mission'
  rndUserindex = random.randint(2, NbDistUsers)
  while i < NbMultiMissionperSec:
    # generate user information from User dictionary
    # get random index from distinct user count of the day
    if int(random.randint(0, 3)) == 1:
      rndUserindex = random.randint(2, NbDistUsers)
    # read information from user dictionary
    sUserID = DictUsers[rndUserindex - 1][0]
    sCountry = DictUsers[rndUserindex - 1][1]
    sPlatform = DictUsers[rndUserindex - 1][2]

    lDuration = str(random.randint(1, 130))


    emitMission(producer,topic)
    i += 1


  # generate Item interaction player mission information
  i = 1



  while i <= NbItemperSec:
    # generate user information from User dictionary
    # get random index from distinct user count of the day
    rndUserindex = random.randint(2, NbDistUsers)
    # read information from user dictionary
    sUserID = DictUsers[rndUserindex - 1][0]
    sCountry = DictUsers[rndUserindex - 1][1]
    sPlatform = DictUsers[rndUserindex - 1][2]
    lDuration = '0'
    # get random index from game item list (126 possible values)
    rndItemindex = random.randint(2, 126)

    # read information from item dictionary
    Item_Name = DictItems[rndItemindex - 1][0]
    Event_revenue = DictItems[rndItemindex - 1][1]
    #if campaign is not null, assign randomly this campaign for half of item interactions

    if sCampaign != "":
      if int(random.randint(0, 1)) == 1:
        emitItem(producer,topic,sCampaign)
      else:

        sEmptyCampaign = ''
        emitItem(producer, topic,sEmptyCampaign)
    else:
      emitItem(producer, topic, sCampaign)

    i += 1

  #sleep between 0 and 1 second to generate more variability
  time.sleep(random.randint(0, 1))

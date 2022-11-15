from scrapes import *
from addToolSleek import *
import requests
import json
import csv
import re
import numpy as np 
from tqdm import tqdm
from fullPipeline import * 

def fixYellowTemples(fs, token, pidFile, beta):


    # seperate into different groups:
    # pids that dont have any records attached to them are thrown out
    # pids that have the wrong ordinance error codes can be thrown out (unless related to spouse/parents.. save p or s pids and try to fix those pids )
    # pids that have only 1 source attached to them can be done automatically 
    # pids that have multiple sources attached need to be completed with the old criteria 
    pids = []
    birthPlaces = []
    birthCodes = []
    birthPlaceCounts = []
    birthDates = []
    birthDateCounts = []
    skippedPids = []
    skippedCount = 0
    pidsWithNoArks = 0
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0], disable=False):  
        
        pid = row['pid'] 
        # birthPlace = row['pr_birth_place']
        # birthDate = row['pr_birth_year']

        ## pids that have the wrong ordinance error codes can be thrown out (unless related to spouse/parents.. save p or s pids and try to fix those pids ) ##
        url = f"https://www.familysearch.org/service/tree/tree-data/reservations/person/{pid}/ordinances?pendingTransfer=true&locale=en&privateReservationsEx=false&owner=MM29-CQD"
        response = fsRequest(fs, token, url, beta=beta)
        response = response.data

        try:
        
            # print("Working on", pid)
            try: #if this try fails that means that at least one of the ordinances is 'READY' therefore not yellow temples 

                if 'missing.standardized.place' in response['data']['baptism']['whyNotQualifyingReasons'][0]['key'] or \
                    'matchability' in response['data']['baptism']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.date' in response['data']['baptism']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.place' in response['data']['confirmation']['whyNotQualifyingReasons'][0]['key'] or \
                    'matchability' in response['data']['confirmation']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.date' in response['data']['confirmation']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.place' in response['data']['endowment']['whyNotQualifyingReasons'][0]['key'] or \
                    'matchability' in response['data']['endowment']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.date' in response['data']['endowment']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.place' in response['data']['initiatory']['whyNotQualifyingReasons'][0]['key'] or \
                    'matchability' in response['data']['initiatory']['whyNotQualifyingReasons'][0]['key'] or \
                    'missing.standardized.date' in response['data']['initiatory']['whyNotQualifyingReasons'][0]['key']:
                    
                    # print("Valid",pid)
                    ...

                #TODO In the future I think that we could go through the relationships that are missing information and get the parent/spouse pid to try and fix later 
                #TODO for now I will just skip them and only focus on those who dont have their own ordinances completed 

                            # elif 'missing.standardized.place' in response['data']['sealingsToParents']['whyNotQualifyingReasons'][0]['key'] or \
                            #     'matchability' in response['data']['sealingsToParents']['whyNotQualifyingReasons'][0]['key']:
                                
                            #     for relationships in response['data']['sealingsToParents']['relationships']:
                            #         if re     
                                
                                        
                            #             potentialOtherPids.append()

                            # elif 'missing.standardized.place' in response['data']['sealingsToSpouses']['whyNotQualifyingReasons'][0]['key'] or \
                            #     'matchability' in response['data']['sealingsToSpouses']['whyNotQualifyingReasons'][0]['key']:
                                
                            #     validPid = False 
                            #     potentialOtherPids.append()

                #For now if not related to personal ordinance skip to the next pid 
                else:
                    skippedCount += 1
                    skippedPids.append(pid)  
                    continue
            except:
                skippedCount += 1
                skippedPids.append(pid)  
                continue
            
            # print("Past Initial Screen",pid)
            
            allArkInfo = getArks(fs, token, pid, beta )   
            ##Pids that dont have any records attached to them are thrown out##
            if allArkInfo is None:
                pidsWithNoArks += 1
                continue 


            ##MAJORITY TAKEN FROM FULLPIPELINE.PY BELOW THIS POINT
            arks = []
            for arkDescription in allArkInfo:
                try:
                    arkLink = arkDescription["about"]
                    arkLink = arkLink.split("1:1:")[1]
                    arkLink = arkLink.split("?")[0]
                    # print("ARK CHECKED:", arkLink)
                    res = re.search("^[a-zA-Z0-9]{4}-[a-zA-Z0-9]{3,4}$", arkLink)
                    if res is not None: 
                        arks.append(arkLink)
                        # print("ARK PROCESSED:", arkLink)
                except:
                    ...


            foundBirthDates = []
            foundBirthPlaces = []
            birthPlaceCount = 0
            birthDateCount = 0
            for ark in arks:
                arkInfo = getArkInfo(fs,token, ark, beta)
                
                #get the person from the ark that was specified
                arkPerson = {}
                if 'persons' in arkInfo.keys():
                    for person in arkInfo['persons']:
                        if ark in person['links']['persona']['href']:
                            arkPerson = person
                            break


                birthFacts = {}
                try:
                    for fact in arkPerson['facts']:
                        if fact['type'] == 'http://gedcomx.org/Birth':
                            birthFacts = fact
                except:
                    ...

                if birthFacts != {}:
                    if 'date' in birthFacts.keys():
                        date = birthFacts['date']
                        if 'original' in date.keys():                       
                            foundBirthDates.append( birthFacts['date']['original'] )
                            birthDateCount += 1
                    if 'place' in birthFacts.keys():
                        place = birthFacts['place']
                        if 'original' in place.keys():  
                            foundBirthPlaces.append( birthFacts['place']['original'] )
                            birthPlaceCount += 1
            
            #Get current birth info if any... if current info is there I will not replace it. I only add if nothing is there 
            originalDate = None
            originalPlace = None
            birthDate = None
            birthPlace = None 
            response = getPidInfo(fs,token, pid, beta)
            try:
                if 'birthDate' in response['persons'][0]['display']:
                    originalDate = response['persons'][0]['display']['birthDate']
                else:
                    originalDate = None
            except:
                originalDate = None 
            
            try:
                if 'birthPlace' in response['persons'][0]['display']:
                    originalPlace = response['persons'][0]['display']['birthPlace']
                else:
                    originalPlace = None
            except:
                originalPlace = None

            # print("ORIGINAL DATE & YEAR:", originalDate, originalPlace)
            
            #Get all the info from every ark, CRITERIA: all information must match exactly or it cant be done automatically 
            dateMatch = True
            if originalDate is None: 
                for date in foundBirthDates:

                    #Regex for getting the most complete date
                    full = re.search("^[0-3][0-9].+([0-9]{4})", date)
                    full2 = re.search("^[0-9].+([0-9]{4})", date) 
                    semi = re.search(".([0-9]{4})", date)
                    #print(pid,"Best Date:",birthDate,"Compared Date:",date)
                    #print("Full:",full,"Full2:",full2,"Semi:",semi)

                    if birthDate is None:
                        birthDate = date

                    elif full is not None or full2 is not None:
                        if re.search("^[0-3][0-9].+([0-9]{4})", birthDate) is not None or re.search("^[0-9].+([0-9]{4})", birthDate) is not None:
                            #check vs other full length 
                            if date[-4:] != birthDate[-4:]:
                                dateMatch = False
                        else:
                            if date[-4:] == birthDate[-4:]:
                                birthDate = date
                            else:
                                dateMatch = False 
                
                    elif re.search("^[0-3][0-9].+([0-9]{4})", birthDate) is not None:
                        #best date is full but new date isnt
                        if date[-4:] != birthDate[-4:]:
                            dateMatch = False

                    elif semi is not None:
                        if re.search(".([0-9]{4})", birthDate) is not None: 
                            #check vs other semi length 
                            if date[-4:] != birthDate[-4:]:
                                dateMatch = False
                        else:
                            if date[-4:] == birthDate[-4:]:
                                birthDate = date
                            else:
                                dateMatch = False

                    elif re.search(".([0-9]{4})", birthDate) is not None:
                        #best date is semi full but new date isnt
                        if date[-4:] != birthDate[-4:]:
                            dateMatch = False 
                    
                    elif date != birthDate:
                        dateMatch = False
                
                if birthDate is not None:
                    if any(chr.isdigit() for chr in birthDate) is False:
                        birthDate = None

            placeMatch = True
            if originalPlace is None: 
                for place in foundBirthPlaces: 
                    #Take the longest place string, the longer it is the more likely
                    #it has more information. 
                    if birthPlace is None:
                        birthPlace = place
                    elif place in birthPlace:
                        continue
                    elif birthPlace in place:
                        birthplace = place
                    else:
                        placeMatch = False 


            # #checks for accented locations... if its accented later I wont add it 
            # if birthPlace is not None:
            #     accented = re.search("[À-ÿ]", birthPlace)
            #     # print("RESULT OF ACCENTED:", accented); 

            #Standardize the birth place we found 
            if birthPlace and placeMatch:
                response = getStandardPlace(fs, token, birthPlace, beta )
                birthPlace = response[0][0]
                birthPlaceID = response[1][0]
                # print("BIRTH PLACE:", birthPlace)
                # print("PLACE ID:", birthPlaceID)

            if (originalDate is None and birthDate and dateMatch) or (originalPlace is None and birthPlace and placeMatch):
                pids.append(pid)

                if originalPlace is None and birthPlace and placeMatch:
                    birthPlaces.append(birthPlace)
                    birthCodes.append(birthPlaceID)
                    birthPlaceCounts.append(birthPlaceCount)
                else:
                    birthPlaces.append("")
                    birthCodes.append("")
                    birthPlaceCounts.append(0)

                if originalDate is None and birthDate and dateMatch:
                    birthDates.append(birthDate)
                    birthDateCounts.append(birthDateCount)
                else:
                    birthDates.append("")
                    birthDateCounts.append(0)

        except:
            skippedCount += 1
            skippedPids.append(pid)  
            continue



    print("Pids with no arks:", pidsWithNoArks)
    print("Pids skipped because error:", skippedCount)
    print("Can potentially change", len(pids), "pids out of", pidFile.shape[0], "total pids." )


    newPidFile = pd.DataFrame({'pid': pids, 'birthPlace' : birthPlaces, 'birthCode': birthCodes, 'placeCount': birthPlaceCounts, "birthDate": birthDates, "birthCount": birthDateCounts })
    newPidFile.to_csv('potentialEditsMaster.csv', index= False)

    newPidFile = pd.DataFrame({'invalidPids': skippedPids })
    newPidFile.to_csv('invalidPidsYTP.csv', index= False)
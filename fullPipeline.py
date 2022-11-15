from scrapes import *
from addToolSleek import *
import requests
import json
import csv
import re
import string
import numpy as np 
from tqdm import tqdm
from microEditsV2 import * 

#fullPipeline.py
def pipeline(fs, token, pidFile, beta):
    
    ###  Identify Pids with missing birth information  ###  
    missingBoth = []
    missingDate = []
    missingInfo = []
    missingPlace = []
    templeFilter = []
    pids = []
    info = []
    links = []
    placeCount = []
    birthCount = []
    count = 0

    filterByTempleEligibility = False

    print("\nFULL MICROEDIT PIPELINE IS RUNNING ON",pidFile.shape[0],"PIDS\n")
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0]):  
        urllink = row['url']
        pid = urllink.split('details/')[1]
        count += 1
        missingResult = getMissingInfo_P(fs,token,pidFile,beta,pid,count)
        
        if missingResult == 1:      #Missing both birth date and place
            missingBoth.append(pid)
            missingInfo.append(pid)
        elif missingResult == 2:    #Missing birth date
            missingDate.append(pid)
            missingInfo.append(pid)
        elif missingResult == 3:    #Missing birth place 
            missingPlace.append(pid)
            missingInfo.append(pid)

        if missingResult == -1:
            continue
        
    ### Filter Pids By Temple Eligibility ###
    #We are looking to perform micro operations for pids not currently elligible for temple ordinances
    #This section can be skipped by changing the filterByTempleEligibility variable at the top of the function  
        if filterByTempleEligibility is True:
            response = filterByOrdinance_P(fs,token,pidFile,beta,pid)
            if response != 1:
                templeFilter.append(pid)
                continue 

    ### Check Pids for missing information ### 
        infoString, link, birth, place = prepMicroV2_P(fs,token,pidFile,beta,pid) 
        if infoString and link and birth and place:
            info.append(infoString)
            links.append(link)
            birthCount.append(birth)
            placeCount.append(place)
            pids.append(pid)

    newPidFile = pd.DataFrame({'link': links, 'pid': pids, 'information to add' : info, 'dates compared': birthCount, "places compared": placeCount })
    newPidFile.to_csv('microEditsTODO.csv', index= False)

    print(count, "total pids checked for missing info")
    print(len(missingBoth), "total pids missing both birth date and place")
    print(len(missingDate), "total pids missing only birth date")
    print(len(missingPlace), "total pids missing only birth place")
    print(len(templeFilter), "total pids removed by temple eligibility filtering")
    print(len(pids), "potential micro edits")


def getMissingInfo_P(fs,token,pidFile, beta, pid, count):
 
    response = getPidInfo(fs,token, pid, beta)        
    try:
        if 'birthDate' not in response['persons'][0]['display'] and 'birthPlace' not in response['persons'][0]['display']:
            return 1
            
        elif 'birthDate' not in response['persons'][0]['display']:
            return 2

        elif 'birthPlace' not in response['persons'][0]['display']:
            return 3
        
        else:
            return -1
    except:
        return -1

def filterByOrdinance_P(fs, token, pidFile, beta, pid):
    
    url = f'https://www.familysearch.org/service/tree/tree-data/v8/person/{pid}/summary?locale=en&includeTempleRollupStatus=true'
    response = fsRequest(fs, token, url, beta)
    response = response.data
    if 'templeRollupStatus' in response.keys():
        if response["templeRollupStatus"] != "NEEDS_MORE_INFORMATION":
            return 1 
    return -1 

def prepMicroV2_P(fs, token, pidFile, beta, pid):
    
    #check if pid has been merged
    mergedPid = getMergedPid(fs, token, pid, beta)
    if pid not in mergedPid:
        pid = mergedPid
        # print("MERGED PID")
    # print("PID TO BE CHECKED:", pid)

    arks = []
    allArkInfo = getArks(fs, token, pid, beta)   
    placeDict = list(pd.read_csv("placeDictionary.csv").Place.values)


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


    #Checks birthdates and birthplaces in every arc for the pid 
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

        
    # if len(foundBirthDates) > 0: 
    #     print("NUMBER OF DATES FOUND:", len(foundBirthDates))
    #     print(foundBirthDates, "\n")
            
    #Get current birth info if any
    originalDate = None
    originalPlace = None
    response = getPidInfo(fs,token, pid, beta)
    try:
        if 'birthDate' in response['persons'][0]['display']:
            birthDate = response['persons'][0]['display']['birthDate']
            originalDate = birthDate
        else:
            birthDate = None

        if 'birthPlace' in response['persons'][0]['display']:
            birthPlace = response['persons'][0]['display']['birthPlace']
            originalPlace = birthPlace
        else:
            birthPlace = None
    except:
        birthPlace = None
        birthDate = None

    # print("ORIGINAL DATE & YEAR:", originalDate, originalPlace)
    
    #Get best info from arks and compare with previous 
    dateMatch = True
    if birthDate is None: 
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
        
        if birthDate is not None: # ? 
            if any(chr.isdigit() for chr in birthDate) is False:
                print("ENTER CHR LOOP. PID:", pid)
                birthDate = None

    # if birthDate is not None:
    #     print("Final Best Date:", birthDate, "Match:", dateMatch)


    placeMatch = True
    if birthPlace is None: 
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

        
    # print("Birth date found:", birthDate)
    # print("Birth place found:", birthPlace)
    infoString = None
    if birthDate is not None and birthDate != originalDate and dateMatch:
        infoString = birthDate

    #checks for accented locations
    if birthPlace is not None:
        accented = re.search("[À-ÿ]", birthPlace)
        # print("RESULT OF ACCENTED:", accented); 

    # print(birthPlace)
    # print(birthPlace in placeDict)

    if birthPlace is not None and birthPlace != originalPlace and placeMatch and accented is None :

        try:
            if(birthPlace is "Puerto Rico, United States"):
                birthPlace = "Puerto Rico"
            response = getStandardPlace(fs, token, birthPlace, beta)
            birthPlace = response[0][0]
            if(birthPlace is "Puerto Rico, Hidalgo, Texas, United States" ):
                birthPlace = "Puerto Rico"
        except:
            ...

        #print(birthPlace)
        commaCount = birthPlace.count(",")
        if commaCount > 1 or accented or birthPlace not in placeDict:
           #throw out 
           nothing = -1
        else:
            if infoString is None:
                infoString = birthPlace
            else:
                infoString += " "
                infoString += birthPlace 

    if infoString is not None:
        link = "=HYPERLINK(\"https://www.familysearch.org/tree/person/details/" + pid + "\")"
       # print("PID PASSED ALL TESTS AND WILL BE ADDED")
        return infoString, link, birthDateCount, birthPlaceCount
    else:
        return None, None, None, None
    #     pids.append(pid)
    #     info.append( infoString)
    #     links.append("=HYPERLINK(\"https://www.familysearch.org/tree/person/details/" + pid + "\")")
    #     birthCount.append(birthDateCount)
    #     placeCount.append(birthPlaceCount)

    # newPidFile = pd.DataFrame({'link': links, 'pid': pids, 'information to add' : info, 'dates compared': birthCount, "places compared": placeCount })
    # newPidFile.to_csv('microEditsTODO.csv', index= False)


def matchLocationDictionary(pidFile):
    #Drop any potential micro edits that dont match the place dictionary
    toRemove = []
    placeDict = list(pd.read_csv("placeDictionary.csv").Place.values)
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0]): 
        info = row['informationtoadd']
        if info.count(",") > 1:
            toRemove.append(index)
            continue
        if info not in placeDict:
            toRemove.append(index)
            continue

    print("Dropping", len(toRemove), "entries")
    pidFile = pidFile.drop(toRemove)
    pidFile.to_csv('updated_microEditsTODO_P.csv',index=False)


def performMicro_P( fs, token, pidFile, beta):
    
    count = 0
    limit =10000   
    pids = []
    failedPids = []
    
    placeDict = list(pd.read_csv("placeDictionary.csv").Place.values)
    print("\nFULL MICROEDIT PIPELINE IS RUNNING ON",pidFile.shape[0],"PIDS\n")
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0], disable=True):  

        pid = row['pid']
        #check if pid has been merged
        mergedPid = getMergedPid(fs, token, pid, beta )
        if pid not in mergedPid:
            pid = mergedPid
            # print("MERGED PID")
        print("PID TO BE CHECKED:", pid)

        arks = []
        allArkInfo = getArks(fs, token, pid, beta )   
        placeDict = list(pd.read_csv("placeDictionary.csv").Place.values)


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


        #Checks birthdates and birthplaces in every arc for the pid 
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

            
        # if len(foundBirthDates) > 0: 
        #     print("NUMBER OF DATES FOUND:", len(foundBirthDates))
        #     print(foundBirthDates, "\n")
                
        #Get current birth info if any
        originalDate = None
        originalPlace = None
        response = getPidInfo(fs,token, pid, beta)
        try:
            if 'birthDate' in response['persons'][0]['display']:
                birthDate = response['persons'][0]['display']['birthDate']
                originalDate = birthDate
            else:
                birthDate = None

            if 'birthPlace' in response['persons'][0]['display']:
                birthPlace = response['persons'][0]['display']['birthPlace']
                originalPlace = birthPlace
            else:
                birthPlace = None
        except:
            birthPlace = None
            birthDate = None

        # print("ORIGINAL DATE & YEAR:", originalDate, originalPlace)
        
        #Get best info from arks and compare with previous 
        dateMatch = True
        if birthDate is None: 
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
        if birthPlace is None: 
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


        #checks for accented locations
        if birthPlace is not None:
            accented = re.search("[À-ÿ]", birthPlace)
            # print("RESULT OF ACCENTED:", accented); 

        # print(birthPlace)
        # print(birthPlace in placeDict)

        response = getStandardPlace(fs, token, birthPlace, beta )
        birthPlace = response[0][0]
        birthPlaceID = response[1][0]
        print("BIRTH PLACE:", birthPlace)
        print("PLACE ID:", birthPlaceID)



        if birthPlace is not None and birthPlace != originalPlace and placeMatch and accented is None :

            try:
                if(birthPlace is "Puerto Rico, United States"):
                    birthPlace = "Puerto Rico"
                response = getStandardPlace(fs, token, birthPlace, beta)
                birthPlace = response[0][0]
                birthPlaceID = response[1][0]
                if(birthPlace is "Puerto Rico, Hidalgo, Texas, United States" ):
                    birthPlace = "Puerto Rico"
            except:
                ...

            #print(birthPlace)
            commaCount = birthPlace.count(",")
            if commaCount > 1 or accented or birthPlace not in placeDict:
                birthPlace = None
            
        

        facts = {'facts': []}
        factType = {}
        factDate = {}
        factPlace = {}

        if birthDate != originalDate or (birthPlace != originalPlace and placeMatch and accented is None):
            factType = {'type': 'http://gedcomx.org/Birth'}

            if birthDate is not None:

                factDate = {'date': {}}
                factDate['date'].update({'original' : birthDate})

            if birthPlace is not None and placeMatch and accented is None:
                factPlace = {'place': {}}
                factPlace['place'].update({'original': birthPlace})
                factPlace['place'].update({'description': "#" + birthPlaceID})
            
            singleFact = {}
            if factType is not None:
                singleFact.update(factType)
            if factDate is not None:
                singleFact.update(factDate)
            if factPlace is not None: 
                singleFact.update(factPlace)

            if singleFact is not None:
                #print(singleFact)
                #print("PREPARING TO ADD")
                facts['facts'].append(singleFact)

                toPost = {'persons': []}
                toPost['persons'].append(facts)

                persons_node = f'https://www.familysearch.org/platform/tree/persons/{pid}'
                fs.session.headers.update({'Content-type': 'application/x-gedcomx-v1+json'})
                response = fsRequest(fs, token, persons_node, requestType = 'post', post = json.dumps(toPost), beta = beta)
                if response.status_code == 201 or response.status_code == 204:
                    #print(f'Successfully added source {response.status_code}\n')
                    continue
                else:
                    print(f'Error adding source {response.status_code}\n')
                    failedPids.append(pid)
        
    newPidFile = pd.DataFrame({'Error Pids':  failedPids })
    newPidFile.to_csv('errorPids.csv', index= False)

    print(failedPids)

def checkTempleFinished( fs, token, pidFile, beta ):
    #Get the temple status for pids, creates new csv for the 
    #pids who's work is completed as well as those that need more info
    completed = []
    reserved_shared = []
    ready = []
    reserved = []
    needsInfo = []
    other = []
    print("\nChecking temple status on",pidFile.shape[0],"PIDS\n")
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0]): 
        pid = row['pids']
        mergedPid = getMergedPid(fs, token, pid, beta)
        if pid not in mergedPid:
            pid = mergedPid
        url = f'https://www.familysearch.org/service/tree/tree-data/v8/person/{pid}/summary?locale=en&includeTempleRollupStatus=true'
        response = fsRequest(fs, token, url, beta)
        response = response.data
        if 'templeRollupStatus' in response.keys():
            if response["templeRollupStatus"] == "NEEDS_MORE_INFORMATION":
                needsInfo.append(pid)
            elif response["templeRollupStatus"] == "COMPLETED":
                completed.append(pid)
            elif response["templeRollupStatus"] == "RESERVED_SHARED_READY":
                reserved_shared.append(pid)
            elif response["templeRollupStatus"] == "READY":
                ready.append(pid)
            elif response["templeRollupStatus"] == "RESERVED":
                reserved.append(pid)
            else:
                v = pid + " " + response["templeRollupStatus"]
                other.append(v)
    
    print("Needs more information:", len(needsInfo))
    print("Ready:", len(ready))
    print("Completed:", len(completed))
    print("Reserved & Shared:", len(reserved_shared))
    print("Reserved:", len(reserved))
    if len(other) > 0:
        print("OTHER:", other)

    #print("Completed Pids: ", completed)
    newPidFile = pd.DataFrame({'Completed Pids': completed})
    newPidFile.to_csv('completed-Temple.csv', index= False)

    newPidFile = pd.DataFrame({'Needs More Info': needsInfo})
    newPidFile.to_csv('needsInfo-Temple.csv', index= False)

def checkIfStandard( fs, token, pidFile, beta ):
    nonNorm = []
    places = []
    links = []
    print("\nChecking if standardized place on",pidFile.shape[0],"PIDS\n")
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0]): 
        pid = row['pid']
        try:
            response = getPidInfo(fs,token, pid, beta)        
            for fact in response['persons'][0]['facts']:
                if fact['type'] == 'http://gedcomx.org/Birth':
                    if 'place' in fact.keys(): 
                        place = fact['place']
                        if 'normalized' not in place.keys():
                            #print("FOUND NON NORMALIZED")
                            nonNorm.append(pid)
                            places.append(place["original"])
                            links.append("=HYPERLINK(\"https://www.familysearch.org/tree/person/changelog/" + pid + "/birth\")")
        except:
            errorPids.append(pid)
            continue
    print(len(nonNorm), "nonstandardized places found")
    newPidFile = pd.DataFrame({'Pids': nonNorm, 'Non-standardized Place': places, 'Links': links})
    newPidFile.to_csv('nonStandardizedPlaces.csv', index= False)

def checkIfStandard2( fs, token, pidFile, beta ):
    #Pass in a Pid to check if all places are standardized
    pids = []
    factInfo = []
    factType = []
    standardizedPlace =  []
    standardizedID = []
    links = []
    print("Starting")
    count = 1 
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0], disable=True): 
        pid = row['Pids']
        count += 1
        response = getPidInfo(fs,token, pid, beta)        
        for fact in response['persons'][0]['facts']:
            if fact['type'] == 'http://gedcomx.org/Birth':
                if 'place' in fact.keys(): 
                    place = fact['place']
                    if 'normalized' not in place.keys():
                        factInfo.append( place['original'] )
                        fType = fact['type']
                        if "http://gedcomx.org/" in fType:
                            fType = fType.split(".org/")[1]
                        factType.append( fType )
                        pids.append(pid)
                        links.append("=HYPERLINK(\"https://www.familysearch.org/tree/person/changelog/" + pid + "/birth\")")
                        #print("ORIGINAL", place['original'])
                        response = getStandardPlace(fs, token, place['original'], beta)
                        #print(len(response))
                        if response[0]:
                            place = response[0][0]
                            placeID = response[1][0]
                            standardizedPlace.append(place)
                            standardizedID.append(placeID)
                        else:
                            standardizedPlace.append(None)
                            standardizedID.append(None)


    print(len(pids), "nonstandardized places found")
    newPidFile = pd.DataFrame({'Pids': pids, 'Non-standardized Place': factInfo, 'Fact Type': factType, "Standardized Place": standardizedPlace , "Standardized ID": standardizedID, 'Links': links})
    newPidFile.to_csv('nonStandardizedPlaces2.csv', index= False)

def checkBirthPlace(fs,token,pidFile):
    #Gets the birth place of pids and exports to csv
    foundPids = []
    foundBirthPlaces = []
  
    print("\nChecking birth place for",pidFile.shape[0],"PIDS\n")
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0]): 
        pid = row['pid']
        try:
            response = getPidInfo(fs,token, pid, beta )
            if 'birthPlace' in response['persons'][0]['display']:
                foundBirthPlaces.append(response['persons'][0]['display']['birthPlace'])
                foundPids.append(pid)
            else:
                continue
        except:
            continue
            
    try: 
        newPidFile = pd.DataFrame({'pid': foundPids, 'Birth Place' : foundBirthPlaces})
        newPidFile.to_csv('birthPlaces.csv', index= False)
    except: 
        print("BACKUP CSV CREATED ON ERROR")
        letters = string.digits
        newName = ''.join(random.choice(letters) for i in range(10))
        newName += '.csv'
        newPidFile.to_csv(newName, index= False)

def fixStandardizedError( fs,token,pidFile,beta): 
    pids = []
    factInfo = []
    factType = []
    standardizedPlace =  []
    standardizedID = []
    links = []
    count = 1 
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0], disable=False): 
        pid = row['pids']
        count += 1
        facts = {'facts': []}

        mergedPid = getMergedPid(fs, token, pid, beta)
        if pid not in mergedPid:
            #print("MERGED PID:", pid)
            pid = mergedPid

        response = getPidInfo(fs,token, pid, beta)  
        if 'persons' in response.keys():      
            for fact in response['persons'][0]['facts']:
                if fact['type'] == 'http://gedcomx.org/Birth':
                    if 'place' in fact.keys(): 
                        place = fact['place']
                        if 'normalized' not in place.keys():
                            factInfo.append( place['original'] )
                            factType.append( fact['type'] )
                            pids.append(pid)
                            links.append("=HYPERLINK(\"https://www.familysearch.org/tree/person/changelog/" + pid + "/birth\")")
                            #print("ORIGINAL", place['original'])
                            response = getStandardPlace(fs, token, place['original'], beta)
                            if response[0]:
                                place = response[0][0]
                                placeID = response[1][0]
                                standardizedPlace.append(place)
                                standardizedID.append(placeID)
                            else:
                                standardizedPlace.append(None)
                                standardizedID.append(None)
                                continue
                            
                            original = fact['place']['original']
                            description = response[1][0]
                            factPlace = {'place': {}}
                            factPlace['place'].update({'original': original })
                            factPlace['place'].update({'description': "#" + description})
                            
                            value = response[0][0]
                            factNorm = {'normalized': []}
                            temp = {}
                            temp.update({'lang' : "en-US"})
                            temp.update({'value': value })
                            factNorm['normalized'].append(temp)
                            factPlace['place'].update(factNorm)
                            fact.update(factPlace)
                            #print(fact)
                            facts['facts'].append(fact)
                            #print(pid)
                            break
                        
                
        toPost = {'persons': []}
        toPost['persons'].append(facts) 
        #print(json.dumps(facts, indent=4))

        persons_node = f'https://www.familysearch.org/platform/tree/persons/{pid}'
        fs.session.headers.update({'Content-type': 'application/x-gedcomx-v1+json'})
        response = fsRequest(fs, token, persons_node, 'post', json.dumps(toPost), beta)

        if response.status_code == 201 or response.status_code == 204:
           # print(f'Successfully added source {response.status_code}\n')
            response = getPidInfo(fs,token, pid, beta)        
            #print(json.dumps(response, indent=4))
            continue
        else:
            print(f'Error adding source {response.status_code}\n')
            # failedPids.append(pid)
                        


    print(len(pids), "nonstandardized places found")
    newPidFile = pd.DataFrame({'Pids': pids, 'Non-standardized Place': factInfo, 'Fact Type': factType, "Standardized Place": standardizedPlace , "Standardized ID": standardizedID, 'Links': links})
    newPidFile.to_csv('nonStandardizedPlaces1234.csv', index= False)



def quickFix(fs, token, pidFile, beta):
    pids = []
    for index, row in tqdm(pidFile.iterrows(), total=pidFile.shape[0]):  
        urllink = row['url']
        pid = urllink.split('details/')[1]
        pids.append(pid)

    newPidFile = pd.DataFrame({'pids': pids})
    newPidFile.to_csv('quickfix.csv', index= False)

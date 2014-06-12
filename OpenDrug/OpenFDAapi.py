#!python
'''

Created on Jun 5, 2014

@author: ntilmans

A quick and dirty system to scan in the OpenFDA dataset into a set of 
normalized MySQL tables for doing various drug comparisons. Makes certain 
assumptions about field availability by querying the API for just those 
records that contain at least one instance of those fields.

'''


import requests
import pandas as pd
import json
import pprint

#Printer for debugging objects
pp = pprint.PrettyPrinter(indent=4)



openFDAbaseUrl = "https://api.fda.gov/drug/event.json?"
myAPIkey = "wPnfo2MSawEI25vhBoRYvxyJFcc3xqhH0A206glJ"
scratchAPIkey = "sXT14bAZRnSWZ8ee5dOW3qsEGZCeFUOSVOq48lof"

fieldsRequired = ["patient.drug.medicinalproduct","patient.reaction.reactionmeddrapt"]



def runQuery(search="",count="", apiKey="", skip = -1, limit=-1, forceFields=True):
    """Return object from openFDA API query
    
    It is possible to construct a query that returns no results due to improper
    formatting of the strings and values passed to this function. This function
    performs no checks to ensure that the user-entered query will return a 
    result.
    
    search should be the string following 'search=' in an openFDA query
        if ForceFields is true, will append the fieldsRequired to this query
        
        by default empty
        
    count should be the string that follows 'count=' in an openFDA query
        
        by default empty
    
    apiKey is the key to use for the query
    
        by default no API key is used
    
    forceFields defines whether the query will force inclusion of the 
        fields defined in the fieldsRequired array
        
        by default True
        
    skip is the first record to get, zero-indexed, this corresponds to 
        the 'skip' term in openFDA
        
        by default set to negative number and ignored. Positive numbers are 
        interpreted
    
    limit is the number of records to get, this corresponds to the 'limit' term
        in openFDA
        
        by default set to negative number and ignored. Positive numbers are 
        interpreted
    """
    
    #Start constructing the query
    
    query = openFDAbaseUrl
    
    #flag indicating that we're already builing a query
    buildingFlag = False
    
    if apiKey: 
        query = query + "api_key=" + apiKey +"&"
    if forceFields:
        query = query + "search=(_exists_:" +"+AND+_exists_:".join(fieldsRequired) + ")"
        buildingFlag = True
        
    if search:
        #handle whether certain things have already been added
        if buildingFlag:
            query += "+AND+("
        else:
            query += "search=("
        query += search + ")"
        buildingFlag = True
        
    if count:
        #again, handle if we're just starting the construction
        if buildingFlag:
            query += "&"
        query += "count=" + count
        buildingFlag = True
        
    if skip >= 0:
        #again, handle if we're just starting the construction
        if buildingFlag:
            query += "&" 
        query += "skip=" + str(skip)
        buildingFlag = True
        
    if limit >= 0:
        #again, handle if we're just starting the construction
        if buildingFlag:
            query += "&" 
        query += "limit=" + str(limit)
    
    #Runs the request and gets text
    rtext = requests.get(query).text
    
    #Print out the text for a cache
    open('lastQuery.json','w').write(rtext)
    
    #Return a python object
    return json.loads(rtext)


def chewDict( inputDict, setIgnorableKeys=set(), killNone = True):
    """Pulls in a dict with nested dicts and flattens it out to a simple dict
    
    This is useful because in several cases the FDA dataset has nested dicts
    that aren't super important to preserve as dicts for my purposes. The
    key for those dicts kijs 
    
    Note that if a value in the dict is a list, it will be set as such in the
    return dict. Lists are not flattened.
    
    WARNING if one of the keys in the sub-dict is the same as a key in the 
    parent dict, the value in the parent may get overwritten with that in the 
    sub dict.
    
    inputDict is the input dictionary
    setIgnorableKeys is a set of keys I may want to process manually
    
    default is that no keys are ignored
    
    killNull excludes all keys for which the value in the dictionary would
        evaluate to None in an if statement
        
    default is that all such keys will be killed        
    """
    returnDict = {}
    
    for key in inputDict.keys():
        if key not in setIgnorableKeys:
            
            #if there's a dict here I need to make a recursive call
            if isinstance(inputDict[key],dict):
                returnDict.update( chewDict( inputDict[key] ) )
                
            elif inputDict[key] is not None or not killNone:
                returnDict[key] = inputDict[key]
            
    
    return returnDict


#Process one array of results

#This could eventually be wrapped into a function to make it a bit prettier
#but to just get the data out of the API in the fastest way, I'm just going 
#to hack it together here.

#results = jsonResult["results"]

#Temporary guy to debug
#jsonResult = json.loads( open("testOneResult.json",'r').read() )
#results = [jsonResult]
jsonResult = json.loads( open("lastQuery.json",'r').read() )
results = jsonResult["results"]


#types of keys to avoid
specialReportKeys = set( ["patient"] )
specialPatientKeys = set( ["reaction", "drug"] )
specialDrugKeys = set( ["drugindication","medicinalproduct","openfda"] )
specialReactionKeys = set( ["reactionmeddrapt"] )

#Set up some indices for the tables, will be MySQL indices for each entity
currentReportIndex = 0;
currentDrugIndex = 0;
currentReactionIndex = 0;
currentIndicationIndex = 0;

#Set up the dataframes to 
reportDF = pd.DataFrame()
drugReportDF = pd.DataFrame()
reactionReportDF = pd.DataFrame()


drugsHash = {}
reactionsHash = {}
indicationsHash = {}

for result in results:
    reportDict = {} #the line that will go into the reports table
    drugDict = {} #may need this to be different
    reactionDict = {} #may need a list instead of a dict
    
    #This gives me my report table information
    currentReportIndex += 1;
    reportDict = chewDict(result, specialReportKeys)
    reportDict["report_id"] = currentReportIndex
    
    #Add patient information to the report table
    patientDict = chewDict( result["patient"], specialPatientKeys )
    reportDict.update(patientDict)
    
    #At this point, everything in the report that is tied to the top level of
    #the report is in the reportDict, so add that as a row to the report 
    #DataFrame that will later be the table for MySQL
    #This is extremely memory inefficient for large datasets, need to find
    #a workaround
    reportDF = reportDF.append(reportDict, ignore_index=True)
    
    
    #Process Drugs
    drugs = result["patient"]["drug"]
    
    for drug in drugs:
        
        #if I can't find a medicinal product, I won't bother constructing the
        #dict
        if "medicinalproduct" not in drug:
            continue

        #The main key here for all products
        drugName = drug["medicinalproduct"]
        
        #print "Entering drug " + drugName
        #Sets up for the drug_id -> drug table and makes sure the drug_id 
        #is unique to generate a normalized set of mysql tables later.
        if drugName not in drugsHash:
            #Initialize our drug in the drug hash
            currentDrugIndex += 1
            
            drugsHash[drugName] = {"drug_id": currentDrugIndex}
        
        drugDict["drug_id"] = drugsHash[drugName]["drug_id"]
        
        #I want to keep drug indication but to turn it into a normalized 
        #table as well. Will add the indication to a set inside the 
        #drugsHash as well for convenience, can decide later how to deal 
        #with the data
        if "drugindication" in drug:
            
            indication = drug["drugindication"]
            if "indications" not in drugsHash[drugName]:
                drugsHash[drugName]["indications"] = set()
            
            if indication not in indicationsHash:
                currentIndicationIndex += 1
                indicationsHash[indication] = {"indication_id": currentIndicationIndex}
        
            drugsHash[drugName]["indications"].add(indication)
            drugDict["indication_id"] = indicationsHash[indication]["indication_id"]
            
            
        drugDict.update( chewDict(drug, specialDrugKeys) )
        drugDict["report_id"] = currentReportIndex
        
        #Drug entry should be ready for the MySQL table at this point, so 
        #add it to the drug table
        #This is extremely memory inefficient for large datasets, need to find
        #a workaround
        drugReportDF = drugReportDF.append(drugDict, ignore_index=True)
        #pp.pprint(drugDict)
        
        
        #If there's an openFDA section, try and capture that information
        #this overwrites certain things, may not be production ready
        if "openfda" in drug:
            if "pharm_class_epc" in drug["openfda"]:
                #I know that some drugs may have more than one class,
                #I'm hoping that the first one will be sufficient 
                drugsHash[drugName]["openfdaPharmClass"] = drug["openfda"]["pharm_class_epc"][0]
                
                if "openfdaAllParmClass" in drugsHash[drugName]:
                    drugsHash[drugName]["openfdaAllParmClass"].update(drug["openfda"]["pharm_class_epc"])
                else:
                    drugsHash[drugName]["openfdaAllParmClass"] = set(drug["openfda"]["pharm_class_epc"])
                    
            if "generic_name" in drug["openfda"]:
                drugsHash[drugName]["openfdaGenericName"] = drug["openfda"]["generic_name"][0]
            if "brand_name" in drug["openfda"]:
                drugsHash[drugName]["openfdaBrandName"] = drug["openfda"]["brand_name"][0]          
            if "rxcui" in drug["openfda"]:
                #I know that some drugs have more than one rxcui.
                #I'm hoping that the first one will be sufficient to look 
                #up other information
                drugsHash[drugName]["openfdaRxcui"] = drug["openfda"]["rxcui"][0]
                if "openfdaAllRxcui" in drugsHash[drugName]:
                    drugsHash[drugName]["openfdaAllRxcui"].update(drug["openfda"]["rxcui"])
                else:
                    drugsHash[drugName]["openfdaAllRxcui"] = set(drug["openfda"]["rxcui"])  
                
            if "unii" in drug["openfda"]:
                if "openfdaAllUnii" in drugsHash[drugName]:
                    drugsHash[drugName]["openfdaAllUnii"].update(drug["openfda"]["unii"])
                else:
                    drugsHash[drugName]["openfdaAllUnii"] = set(drug["openfda"]["unii"])
                    
                #may want to figure out how to deal with SPL and NDC codes 
                #later
                
            #END OF OPENFDA DRUG SECTION
        
        #Need to set up the drugDict for the next entry in the drug section of
        #the report
        drugDict.clear()
        
        #END DRUG PROCESSING LOOP
    
    
    #Process reactions
    reactions = result["patient"]["reaction"]
    
    for reaction in reactions:
        
        #if the reaction doesn't have a name, skip the reaction entirely
        if "reactionmeddrapt" not in reaction:
            continue
    
        #The main key for a given reaction
        reactionName = reaction["reactionmeddrapt"]
        
        
        #Sets up for the reaction_id -> reaction table and makes sure the
        #reaction_id is unique to each reaction name unique to generate 
        #a normalized set of mysql tables later.
        if reactionName not in reactionsHash:
            #Initialize our reaction in the reactions hash
            currentReactionIndex += 1
            reactionsHash[reactionName] = {"reaction_id": currentReactionIndex}
        
        reactionDict["reaction_id"] = reactionsHash[reactionName]["reaction_id"]
        
         
        reactionDict.update( chewDict(reaction, specialReactionKeys) )
        reactionDict["report_id"] = currentReportIndex
        
        #Drug entry should be ready for the MySQL table at this point, so add
        #it to the drug table
        #This is extremely memory inefficient for large datasets, need to find
        #a workaround
        reactionReportDF = reactionReportDF.append(reactionDict, ignore_index=True)
    
        #Set up reactions dict for next cycle of the loop
        
        reactionDict.clear()
        
    
    reportDict.clear()

print "REPORT TABLE:\n", reportDF
print "DRUG TABLE:\n", drugReportDF
print "REACTION TABLE:\n", reactionReportDF

    







'''
#returnObj = runQuery(search="patient.drug.openfda.substance_name:ASCORBIC+ACID", limit=100, apiKey=scratchAPIkey)

returnObj = json.loads(open('lastQuery.json','r').read())
cuiset = set([])
name = set([])
asccuiset = set()


for returner in returnObj["results"]:
    for drug in returner["patient"]["drug"]:
        if 'openfda' in drug:
            print "JSON\n" + str(json.dumps(drug["openfda"],sort_keys=True,indent=4,separators=(',',': ')))
            
            if 'rxcui' in drug["openfda"]:
                print "Returner: " + str(drug["openfda"]["rxcui"])
                
                
                
                cuiset.update(drug["openfda"]["rxcui"])
            if 'generic_name' in drug["openfda"]:
                name.update(drug["openfda"]["generic_name"])
                if 'DROSPIRENONE AND ETHINYL ESTRADIOL' in drug["openfda"]['generic_name']:
                    asccuiset.update(drug["openfda"]['rxcui'])

print "CUISET:"+str(cuiset)
print "ASCCUISET:"+str(asccuiset)
print "NAMESET:"+str(name)
'''





#testset = runQuery(apiKey=scratchAPIkey, limit=3)
    
    

'''
Might want a function that takes a dict of dicts and turns it into a coherent
matrix/frequency table. Maybe pandas already does this? I think it does. To
investigate later.
'''



#r = requests.get("https://api.fda.gov/drug/event.json?api_key=wPnfo2MSawEI25vhBoRYvxyJFcc3xqhH0A206glJ&search=(patient.drug.medicinalproduct:aspirin)")
#print r.text
#r = requests.get("https://api.fda.gov/drug/event.json?api_key=wPnfo2MSawEI25vhBoRYvxyJFcc3xqhH0A206glJ&search=(receivedate:20050113)")
#r = requests.get("https://api.fda.gov/drug/event.json?api_key=wPnfo2MSawEI25vhBoRYvxyJFcc3xqhH0A206glJ&search=(receivedate:20050113)")
#requests.get("https://api.fda.gov/drug/event.json?search=(_missing_:openfda.brand_name)")

#print (json.loads(requests.get("https://api.fda.gov/drug/event.json?&count=patient.drug.drugindication.exact").text)['meta'])


#r = requests.get("https://api.fda.gov/drug/event.json?search=_exists_:patient.drug.openfda.generic_name")
#inputjson = open("testjson2.json", 'r').read()
#answer = json.loads(inputjson)

#print r.text
#print answer['meta']

#testjson = open("./testjson2.json", 'r')



#print requests.get("https://api.fda.gov/drug/event.json?search=patient.drug.openfda.pharm_class_epc:\"nonsteroidal+anti-inflammatory+drug\"&count=patient.reaction.reactionmeddrapt.exact").text


#print  pandas.read_json(inputjson)
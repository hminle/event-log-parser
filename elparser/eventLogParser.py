"""
Define module that can parse log file
"""

import numpy as np 
import pandas as pd 

from dateutil.parser import parse

class ResourceContinent():
    
    def __init__(self):
        self.dict = {}
        listContinent = ['Europe', 'Asia', 'North America', 'Africa', 'Antarctica', 'South America', 'Oceania']
        for continent in listContinent:
            self.dict[continent] = 0

    def addContient(self, continentName):
        if continentName in self.dict:
            self.dict[continentName] = 1

    def getDict(self):
        return self.dict

class LifecycleTransition():
    """
    LifecycleTransition is an object that is responsible for counting kinds of LifecycleTransition
    """
    def __init__(self):
        self.dict = {}
        listLifecycle = ['In Progress', 'Awaiting Assignment', 'Resolved', 'Assigned', 'Closed', 
            'Wait - User', 'Wait - Implementation', 'Wait', 'Wait - Vendor', 'In Call',
            'Wait - Customer', 'Unmatched', 'Cancelled']
        for lifeCycle in listLifecycle:
            self.dict[lifeCycle] = 0

    def add(self, name):
        self.dict[name] = self.dict[name] + 1

    def getDict(self):
        return self.dict    

class CaseInfo():
    """
    CaseInfo is an object that hold all infomation of 1 case-id
    """
    def __init__(self, id):
        self.case_id = id
        self.lifecycleTransition = LifecycleTransition()
        self.resourceContinent = ResourceContinent()

    def setStartTimestamp(self, startTimestamp):
        self.startTimestamp = startTimestamp

    def setEndTimestamp(self, endTimestamp):
        self.endTimestamp = endTimestamp
        self.__calculateDuration()

    def setVariantIndex(self, variantIndex):
        self.variantIndex = variantIndex

    def setImpact(self, impact):
        if not hasattr(self, 'impact'):
            self.impact = impact
            # If there is two kinds of impact in one Case --> raise Exception
            if self.impact != impact:
                 raise ValueError('Impact is already set')

    def setOrganizationContinent(self, continentName):
        if not hasattr(self, 'organizationContinent'):
            self.organizationContinent = continentName
            #if self.organizationContinent != continentName:
                #raise ValueError('Org Continent is already set')
    
    def addLifecycleTransition(self, name):
        self.lifecycleTransition.add(name)

    def addResourceContinent(self, continentName):
        self.resourceContinent.addContient(continentName)

    """
    After registering all features of 1 Case, we generate a dict for one case
    """    
    def buildDict(self):
        tempDict = {}
        tempDict['CaseID'] = self.case_id
        tempDict['StartTime'] = self.startTimestamp
        tempDict['EndTime'] = self.endTimestamp
        tempDict['Impact'] = self.impact
        tempDict['OrganizationContinent'] = self.organizationContinent
        tempDict['VariantIndex'] = self.variantIndex
        tempDict['Duration'] = self.duration
        tempDict.update(self.lifecycleTransition.getDict())
        tempDict.update(self.resourceContinent.getDict())
        return tempDict 
    
    def __calculateDuration(self):
        self.duration = round(((parse(self.endTimestamp) - parse(self.startTimestamp)).total_seconds()), 3)


class EventLogParser(object):
    """
    EventLogParser is an Object that can parse 1 event log csv file
    """
    # Define public methods 
    def __init__(self, fileName):
        self.fileName = fileName

    def loadEventLog(self):
        """
        Load Event Log to dataframe
        """
        self.df = pd.read_csv(self.fileName)
        listColumns = ['Resource', 'Activity', 'Variant', 'concept:name', 
            'organization involved', 'org:group', 'org:role', 'product']
        self.__dropUnnecessaryColumns(listColumns)
        self.__removeStringInColumn('Case ID')

        #Rename columns
        self.df.columns = ['CaseID', 'CompleteTimestamp', 'VariantIndex', 
            'Impact', 'LifecycleTransition', 'OrganizationContinent', 'ResourceContinent',]
        self.__renameCountryToContinent()
  
    def buildDataFrame(self):
        dataDict = self.__buildCaseInfo()
        newDf = pd.DataFrame(dataDict)
        newDf = newDf.transpose()
        columnsList = ['CaseID', 'StartTime', 'EndTime', 'Duration', 'Impact', 'VariantIndex',
            'OrganizationContinent',
            'In Progress', 'Awaiting Assignment', 'Resolved', 'Assigned', 'Closed', 
            'Wait - User', 'Wait - Implementation', 'Wait', 'Wait - Vendor', 'In Call',
            'Wait - Customer', 'Unmatched', 'Cancelled',
            'Europe', 'Asia', 'North America', 'Africa', 'Antarctica', 'South America', 'Oceania']
        newDf = newDf[columnsList]
        newDf['CaseID'] = newDf['CaseID'].apply(lambda x: int(x))
        resourceContinentDict = {'Europe':'ResuorceContinentEurope', 'Asia':'ResourceContinentAsia',
         'North America':'ResourceContinentNorthAmerica', 'Africa':'ResourceContinentAfrica', 
         'Antarctica':'ResourceContinentAntarctica', 'South America':'ResourceContinentSouthAmerica',
         'Oceania':'ResourceContinentOceania'}
        newDf = newDf.sort_values(by='CaseID')
        newDf = newDf.rename(columns=resourceContinentDict)
        return newDf

    # Define private methods for internal uses

    def __renameCountryToContinent(self):
        countryDict = pd.read_csv('./elparser/countryDict.csv')
        lowercaseCode = lambda x: str(x).lower()
        countryDict['countryCode'] = countryDict['countryCode'].apply(lowercaseCode)
        
        #Create dictionaries
        continentBasedOnCountryCode = countryDict.set_index('countryCode')['continentName'].to_dict()
        continentBasedOnCountryName = countryDict.set_index('countryName')['continentName'].to_dict()
        #Map to ContinentName
        self.df['OrganizationContinent'] = self.df['OrganizationContinent'].map(continentBasedOnCountryCode)
        self.df['ResourceContinent'] = self.df['ResourceContinent'].map(continentBasedOnCountryName)

    def __dropUnnecessaryColumns(self, listColumns):
        self.df = self.df.drop(listColumns, axis=1)

    def __transformDatetime(self):
        pass

    def __buildCaseInfo(self):
        i = 0
        tempDict = {}
        for row in self.df.itertuples(index=False):
            i = i + 1
            (caseId, completeTimestamp, variantIndex, impact, 
                lifecycleTransition, organizationContinent, resourceContinent) = row
            if impact == 'Start':
                tempDict[caseId] = CaseInfo(caseId)
                tempDict[caseId].setStartTimestamp(completeTimestamp)
            elif impact == 'End':
                tempDict[caseId].setEndTimestamp(completeTimestamp)
                tempDict[caseId] = tempDict[caseId].buildDict()
            else:
                tempDict[caseId].setVariantIndex(variantIndex)
                tempDict[caseId].setImpact(impact)
                tempDict[caseId].setOrganizationContinent(organizationContinent)
                tempDict[caseId].addLifecycleTransition(lifecycleTransition)
                tempDict[caseId].addResourceContinent(resourceContinent)

        return tempDict
    
    def __removeStringInColumn(self, column_name):
        remover = lambda x: x.split()[-1]
        self.df[column_name] = self.df[column_name].apply(remover)
        

def main():
    """
    Write some test code
    """
    eventLogParser = EventLogParser('./BPI_Challenge_2013_incidents.csv')
    eventLogParser.loadEventLog()
    eventLogParser.buildDataFrame()
    #eventLogParser.printDFSample()

if __name__ == "__main__":
    main()

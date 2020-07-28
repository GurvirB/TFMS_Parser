#!/usr/bin/env python
# coding: utf-8

# In[1]:


cd C:\Users\bawag\Desktop\A21 Project


# In[2]:


#Imports

import itertools
import xml.etree.ElementTree as ET
from lxml import etree, objectify
import os,sys,csv
import glob
import pandas as pd


# In[3]:


#Supporting functions to be used


def groups(iterable):
    i = itertools.count()
    for x, group in itertools.groupby(iterable, lambda x: x.startswith('2019')):
        yield group
        

def indent(elem, level=0):
    i = "\n" + level*"  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


# In[4]:


#Functions to be performed by the parser

class Data_Parser:
    
    def cleanFile(self,inputfilepath,outputfile):
        self.inputfile=open(inputfilepath,'r')
        self.outputfile=open(outputfile,'w')
        for line in self.inputfile:
            if not line.isspace():
                self.outputfile.write(line)
        self.inputfile.close()
        return self.outputfile
        
    def createNewFiles(self,inputfilepath,outputfilepath):
        self.inputfile=open(inputfilepath)
        self.outputfilepath=outputfilepath
        self.count=1
        for group in groups(self.inputfile):
            for line in group:
                if not line in ['\n', '\r\n']:
                    outputfile = open(self.outputfilepath+'split_msg%s' % (self.count) + '.xml', 'w')
                    if not line.startswith(('2019', 'Property:','NON')):
                        outputfile.writelines(line+'\n')
                        print('\n'.join(line.strip() for line in group), file=outputfile)
                        self.count += 1
                    outputfile.close()
                    
    def beautify(self,inputfilepath):
        self.inputfile=glob.glob(os.path.join(inputfilepath, '*.xml'))
        for filename in self.inputfile:
            with open(filename, 'r+') as f:
                parser = etree.XMLParser(remove_blank_text=True)
                tree = etree.parse(f, parser)
                root = tree.getroot()
                indent(root)
                for elem in root.getiterator():
                    if not hasattr(elem.tag, 'find'): continue  # (1)
                    i = elem.tag.find('}')
                    if i >= 0:
                        elem.tag = elem.tag[i+1:]
                objectify.deannotate(root, cleanup_namespaces=True)
                indent(root)
                tree.write(filename,xml_declaration=True, encoding='UTF-8')
        
                
    def getTrackInfoMessages(self,inputfilepath,outputfilepath):
        self.inputfile=glob.glob(os.path.join(inputfilepath, '*.xml'))
        self.count=1
        for filename in self.inputfile:
            with open(filename, 'r+') as f:
                tree = ET.parse(f)
                try:
                    xmlFile = ET.tostring(tree.getroot(), encoding='utf8').decode('utf8')
                except:
                    sys.stderr.write("Wrong xml2 header\n")
                    exit(31)
                output=tree.find('/fltdOutput')
                if output is not None:
                    messages=output.findall('fltdMessage')
                    for message in messages:
                        messagetype = message.get('msgType')
                        #print(messagetype)
                        if messagetype =="trackInformation" :
                            continue
                        else:
                            output.remove(message)
                            msg=output.findall('fltdMessage')
                            #print(len(message),f.name)
                            if len(msg)==0:
                                print(f.name,len(msg))
                                f.close()
                                os.unlink(f.name)

                    tree.write(outputfilepath+'track%s' % (self.count) + '.xml',xml_declaration=True, encoding='UTF-8')
                    self.count+=1
                    
    def removeNoTrackFiles(self,inputfilepath):
        self.inputfile=glob.glob(os.path.join(inputfilepath, '*.xml'))
        for filename in self.inputfile:
            with open(filename, 'r+') as f:
                tree = ET.parse(f)
                try:
                    xmlFile = ET.tostring(tree.getroot(), encoding='utf8').decode('utf8')
                except:
                    sys.stderr.write("Wrong xml2 header\n")
                    exit(31)
                message=tree.findall('fltdOutput/fltdMessage')
                #print(len(message),f.name)
                if len(message)==0:
                    f.close()
                    os.unlink(f.name)
                else:
                    continue
    
    def convertToCsv(self,inputfilepath,outputfilepath):
        self.inputfile=glob.glob(os.path.join(inputfilepath, '*.xml'))
        self.outputfilepath=outputfilepath
        self.count=1
        for filename in self.inputfile:
            with open(filename, 'r+') as f:
                print("Processing file "+ f.name)
                #Parse xml file and add tags and values of attributes and text into list
                xmlTree = ET.parse(f)
                try:
                    xmlFile = ET.tostring(xmlTree.getroot(), encoding='utf8').decode('utf8')
                except:
                    sys.stderr.write("Wrong xml2 header\n")
                    exit(31)
                dataLists=[] #a list for all the rows of headers and values
                fltd_output=[]
                fltd_message={}
                temp=[]
                listofdic=[]

                dataLists.append(headerList)
                fl=False

                elem_tobechanged=['departurePoint','arrivalPoint']
                for i in elem_tobechanged:
                    value=i
                    for elem in xmlTree.findall(".//%s/fix" % value):
                        elem.tag=value + "_fix"
                        #print(elem.tag)

                elem_forparent=['airport']
                for i in elem_forparent:
                    for elem in xmlTree.iter():
                        if elem.tag == i:
                            parentelem=xmlTree.find(".//%s/.." % i)
                            #print("---------------->" + parentelem.tag)
                            elem.tag="{}_{}".format(parentelem.tag,elem.tag)
                            #print(elem.tag)

                elem_forgparent=['fixRadialDistance']
                for i in elem_forgparent:
                    for elem in xmlTree.iter():
                        if elem.tag == i:
                            parentelem=xmlTree.find(".//%s/../.." % i)
                            #print("---------------->" + parentelem.tag)
                            elem.tag="{}_{}".format(parentelem.tag,elem.tag)
                            #print(elem.tag)

                attrib_tobechanged=['seconds','minutes','degrees','direction','timeValue','fixName','arrTime',
                                    'routeName','routeType','radial','distance','latitudeDecimal','longitudeDecimal']
                for i in attrib_tobechanged:
                    for elem in xmlTree.iter():
                        if elem.attrib != {}:
                            for k,v in elem.attrib.items():
                                if k == i:
                                    new_k="{}_{}".format(elem.tag,k)
                                    elem.attrib[new_k]=elem.attrib.pop(i)

                for elem in xmlTree.iter():
                    if elem.tag=='tfmDataService':
                        continue
                    if elem.tag=='fltdOutput':
                        continue
                    #print(elem.tag,fl)
                    if elem.tag=='fltdMessage':
                        if fl:
                            #fl=False
                            #print(elem.tag)
                            fltd_output.append(fltd_message)
                            fltd_message={}
                        else:
                            fl=True
                    fltd_message[elem.tag]=elem.text

                    if elem.attrib != {}:
                        for k,v in elem.attrib.items():
                            if k=='latitudeDecimal' and elem.tag != 'waypoint':
                                fltd_message.update(elem.attrib)
                            if k=='longitudeDecimal' and elem.tag != 'waypoint':
                                fltd_message.update(elem.attrib)
                            fltd_message.update(elem.attrib)
                fltd_output.append(fltd_message) 
                #print(fltd_output)

                for elem in xmlTree.iter():
                    if elem.tag=='flightTraversalData2':
                        traversaldata={}
                        tditems=['fix','waypoint','airway','center','sector']
                        for i in tditems:
                            if i=='fix':
                                traversaldata['fix']=[]
                            else:
                                traversaldata[i]=[]
                            for d in elem.findall(i):
                                #print(d)
                                b=ET.tostring(d,encoding='unicode')
                                if i=='fix':
                                    traversaldata['fix'].append(b.strip())
                                else:
                                    traversaldata[i].append(b.strip())
                        listofdic.append(traversaldata)
                #print(listofdic)

                if len(listofdic)==0:
                    pass
                else:
                    #print(fltd_output)
                    for i in range(len(fltd_output)):
                        #for j in fltd_output[i].keys():
                         #   print(j)
                        found=False
                        if 'fix' in fltd_output[i].keys():
                            #print(fltd_output[i]['flightRef'])
                            fltd_output[i]['fix']=listofdic[0]['fix']
                            found=True
                        if 'waypoint' in fltd_output[i].keys():
                            #print(i)
                            fltd_output[i]['waypoint']=listofdic[0]['waypoint']
                            found=True
                        if 'airway' in fltd_output[i].keys():
                            fltd_output[i]['airway']=listofdic[0]['airway']
                            found=True
                        if 'center' in fltd_output[i].keys():
                            fltd_output[i]['center']=listofdic[0]['center']
                            found=True
                        if 'sector' in fltd_output[i].keys():
                            fltd_output[i]['sector']=listofdic[0]['sector']
                            found=True
                        if not found:
                            continue
                        #print(listofdic[0])
                        listofdic.remove(listofdic[0])

                for index in range(len(fltd_output)):
                    csvmsg=[]
                    for i in headerList:
                        if i in [key for key,value in fltd_output[index].items()]:
                            csvmsg.append(fltd_output[index][i])

                        else:
                            csvmsg.append('')
                    #print(csvmsg)
                    dataLists.append(csvmsg)       
                #print(dataLists)
                print(len(dataLists))
                print("Processed file "+ f.name)
                #f.close()


            #output csv file
            with open(self.outputfilepath+'fileop' + str(self.count) + '.csv','w+',newline='') as op:
                self.count+=1
                writer = csv.writer(op)
                for dataList in dataLists:
                    writer.writerow(dataList)
                op.close()
                
    def mergeCsv(self,inputfilepath,outputfile,lastindex):
        self.tobecreated=open(outputfile,"a")
        self.index=lastindex
        # first file:
        self.toberead=open(inputfilepath+'fileop1.csv')
        for line in self.toberead:
            self.tobecreated.write(line)
        # now the rest:    
        for num in range(2,self.index):#(second files number, end file number)
            readfrom = open(inputfilepath+'fileop'+str(num)+".csv")
            next(readfrom) # skip the header
            for line in readfrom:
                 self.tobecreated.write(line)
            readfrom.close()
        self.tobecreated.close()


# In[5]:


tfmsparser=Data_Parser()


# In[ ]:


#Perform initial clean up of the file

inputfile='C:/Users/bawag/Desktop/A21 Project/Downloaded_files/ERAU.TFMS.Q01.OUT.20190422_180000/ERAU.TFMS.Q01.OUT.20190422_180000'
outputfile='C:/Users/bawag/Desktop/A21 Project/raw_files/raw_20190422_180000.txt'
tfmsparser.cleanFile(inputfile,outputfile)


# In[ ]:


#Extract xml files from the decompressed file

filetobereadfrom='C:/Users/bawag/Desktop/A21 Project/raw_files/raw_20190422_180000.txt'
outputfilepath='C:/Users/bawag/Desktop/A21 Project/Files_Output/20190422_180000/'
tfmsparser.createNewFiles(filetobereadfrom,outputfilepath)


# In[ ]:


inputfilepath='C:/Users/bawag/Desktop/A21 Project/files_Output/20190422_180000/'


# In[ ]:


#Beautify the files and remove namespaces

tfmsparser.beautify(inputfilepath)


# In[ ]:


#Extract only trackInformation messages

outputfilepath='C:/Users/bawag/Desktop/A21 Project/Track_Only/20190422_180000/'
tfmsparser.getTrackInfoMessages(inputfilepath,outputfilepath)


# In[6]:


outputfilepath='C:/Users/bawag/Desktop/A21 Project/Track_Only/20190422_180000/'
tfmsparser.removeNoTrackFiles(outputfilepath)


# In[7]:


headerList=['sensitivity','cdmPart','airline','sourceFacility','sourceTimeStamp','flightRef','acid',
           'msgType','fdTrigger','depArpt','arrArpt','sensReason','aircraftId','facilityIdentifier',
           'computerSystemId','idNumber','gufi','igtd','latitudeDMS_degrees','latitudeDMS_minutes',
           'latitudeDMS_seconds','latitudeDMS_direction','latitudeRadians','longitudeDMS_degrees',
           'longitudeDMS_minutes','longitudeDMS_seconds','longitudeDMS_direction',
           'longitudeRadians','departurePoint_airport','arrivalPoint_airport','facilityId','flightClass',
            'speed','filedTrueAirSpeed','groundSpeed','mach','classified','simpleAltitude','above','blockedAltitude',
           'visualFlightRules','altitudeFixAltitude','timeAtPosition','eta_timeValue','etd_timeValue',
            'etaType','etdType','equipped','currentCompliance','futureCompliance','arrivalFixAndTime_fixName',
            'arrivalFixAndTime_arrTime','departureFixAndTime_fixName','departureFixAndTime_arrTime',
            'nextEvent_latitudeDecimal','nextEvent_longitudeDecimal','diversionIndicator','dp_routeName',
           'dp_routeType','dpTransitionFix','star_routeName','star_routeType','starTransitionFix',
           'nextPosition_latitudeDecimal','nextPosition_longitudeDecimal',
            'departurePoint_fixRadialDistance',
            'departurePoint_fixRadialDistance_radial','departurePoint_fixRadialDistance_distance',
            'arrivalPoint_fixRadialDistance_distance','arrivalPoint_fixRadialDistance_radial',
            'fix','waypoint','airway','center','sector','routeOfFlight','remarksKeywords',
            'estimatedDeparture','estimatedArrival','originalDeparture','originalArrival','gateDeparture',
            'gateArrival','runwayDeparture','runwayArrival','airlineOffTime','airlineOnTime',
            'airlineInTime','airlineOutTime','flightCreation']

print(len(headerList))


# In[ ]:


#Convert all the xml files to csv format

inputfilepath='C:/Users/bawag/Desktop/A21 Project/Track_Only/20190422_180000/'
outputfilepath='C:/Users/bawag/Desktop/A21 Project/csv_files/20190422_180000/'
tfmsparser.convertToCsv(inputfilepath,outputfilepath)


# In[ ]:


#Merge all the csv files

outputfile='C:/Users/bawag/Desktop/A21 Project/combined_csv/20190422_180000.csv'
inputfilepath='C:/Users/bawag/Desktop/A21 Project/csv_files/20190422_180000/'

lastfile=8183
tfmsparser.mergeCsv(inputfilepath,outputfile,lastfile)


# In[ ]:


#Sort data based on the sourceTimeStamp and filter data for depArpt and arrArpt

datafile=pd.read_csv("C:/Users/bawag/Desktop/A21 Project/combined_csv/20190422_180000.csv")
#datafile['sourceTimeStamp']=pd.to_datetime(datafile['sourceTimeStamp'], format="%Y-%m-%dT%H:%M:%S%z").sort_values()
#datafile.sort_values(by='sourceTimeStamp',ascending=True).to_csv("C:/Users/bawag/Desktop/A21 Project/filtered_files/20190422_220000_Sorted.csv",index=False)
datafile=datafile.sort_values(by='sourceTimeStamp',ascending=True)
datafile.query('depArpt=="KDFW" or arrArpt=="KDFW"').to_csv("C:/Users/bawag/Desktop/A21 Project/filtered_files/KDFW20190422_220000.csv",index=False)


# In[ ]:





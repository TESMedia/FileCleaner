#FILE CLEANER - 13 Mar 2017 - Jon Masters v1.3 
# This Programme will clean a set of data files ready for detailed analysis by:
#  a) removing data rows related to randomised Mac Addresses
#  b) removing Mac Address included in an exclusion file (static devices and crew)
#  c) removing the duplicate 'Quadrant' data row when GEO FENCED areas overlap
#  d) identify Mac Addresses onboard 2 days prior or one day after to eliminate harbour noise
#  e) removing deck hops and adding GEOFENCING

# 1.  Configuration
#rootDir = 'c:\\Users\\porro\\Documents\\Thomson Cruises\\sftp\\discovery1\\'
rootDir = 'c:\\Users\\Jon Masters\\Documents\\Python\\' # input files to be located at 'rootDir + YYMM (of file)'
subDirout = '\\Cleaned\\'                               # output file is written to    'rootDir + YYMM + subDirout'
file = 'TUI_DI1_history_data_'                          # name of files are to be in format 'file+MM-DD-YYYY.csv.gz'
macexclusions = 'exclusionsDiscovery1'                  # exclusions file to be located at 'rootDir'
cleaned = 'TUI_D1_location_data_'                       # name allocated to cleaned file ='cleaned+MM-DD-YYYY.csv.gz'
runFrom = 22                                            # days prior to current day processing starts from (normally 15)
findMore = 2000                                         # if Common Macs falls below this threshold, next day is added
logfile = 'FILE CLEANER v1.3, Log for run  '
tfrom = ' 18:00:00'
print ('FILE CLEANER v1.3')

import matplotlib.path as mplPath
import numpy as np                                     # To add GeoFencing also update code lines 250-270  
Barcoords =[ [161.23, 6.49],                           # Configuration - Coordinates for Deck10Bar
           [ 160.61, 8.36 ],
           [ 164.13, 16.05],
           [ 160.47, 10.99], 
           [ 161.5,  13.62],
           [ 164.13, 16.05],
           [ 169.75, 16.05],
           [ 174.16, 11.66], 
           [ 177.99, 11.66],
           [ 180.7,  9.02 ],
           [ 180.7,  6.49 ] ]
Deck10bar = mplPath.Path(np.array(Barcoords))

import time                                              # Establish current time
t0=time.time()
t1=t0
import pandas as pd
import datetime
from datetime import date
from datetime import timedelta
import os
import errno
import gzip
import csv

# 2. Initial set up, establish a start date for Files to be cleaned, and initiate loop
currentdate = datetime.datetime.today()                   # create a run log file with current DateTime in file name 
logdate="{:%m%d-%H%M}".format(currentdate)
logRun=open(rootDir+logfile+logdate+'.txt','w')
logRun.write ('Log '+logdate+'\n')
try:
    inputFile = open(rootDir+macexclusions+'.csv')        # load the list of Macs for exclusion into 'MacExcl'
except Exception as e:
    logRun.write('Error occurred trying to open Mac Address exclusion file - Exclusions set to None'+'\n')
    logRun.write('  '+str(e)+'\n')
    print ('Opening the exclusions file failed, setting exclusions to 0 -',e)
    dfData2 = pd.DataFrame(columns=['mac'])    
else:
    try:
        dfData2 = pd.read_csv(inputFile , usecols= [0], names=['mac'])
    except Exception as e:
        logRun.write('Error occurred reading Mac Address exclusion file - Exclusions set to None'+'\n')
        logRun.write('  '+str(e)+'n')
        print ('Reading exclusions file failed, setting exclusions to 0 -',e)
    else:
        inputFile.close()
MacExcl=dfData2.mac.unique()                              # establish data for start up and initiate loop
MacValues=['0','1','4','5','8','9','c','d']
dfData = pd.DataFrame(columns=['mac','type','time','user','area','site','bld','floor','x','y'])
dfData2 = pd.DataFrame(columns=['mac'])
dfData4 = pd.DataFrame(columns=['mac'])
date1=currentdate-timedelta(days=runFrom)
while date1<currentdate:

# 3. If no cleaned file exists, load the original file and remove Randomised and Excluded Mac Addresses
    TargetData=[]
    file1date="{:%m-%d-%Y}".format(date1)
    file1path="{:%y%m}".format(date1)
    if os.path.isfile(rootDir+file1path+subDirout+cleaned+file1date+'.csv.gz')==True:
        logRun.write('Cleaned file found for date '+file1date+'\n')
        date1=date1+timedelta(days=1)
        print (file1date,' already cleaned')
        dfData = pd.DataFrame(columns=['mac','type','time','user','area','site','bld','floor','x','y'])
        dfData4 = dfData2
        dfData2 = pd.DataFrame(columns=['mac'])                        
        continue
    if dfData.empty==True:
        try:
            inputFile=gzip.open(rootDir+file1path+'\\'+file+file1date+'.csv.gz')
        except Exception as e:
            logRun.write('Error occurred trying to open file to be cleaned dated '+file1date+', Programme terminated.'+'\n')
            logRun.write('  '+str(e)+'\n')
            print('Error opening file to be cleaned - ',e)
            break
        try:
            dfData = pd.read_csv(inputFile , names=['mac','type','time','user','area','site','bld','floor','x','y'], parse_dates = ['time'])
        except Exception as e:
            logRun.write('Error occurred trying to read file to be cleaning dated '+file1date+', Programme terminated.'+'\n')
            logRun.write('  '+str(e)+'\n')
            print ('Error reading file to be cleaned - ',e)
            break
        inputFile.close()
    InitSize = len(dfData)                                      # establish initial size of file   
    InitMacs = len(dfData.mac.unique())
    dfData = dfData[dfData['mac'].isin(MacExcl)==False]         # remove excluded Mac Addresses
    XMac = InitMacs-len(dfData.mac.unique())
    dfData = dfData[dfData.mac.str[1:2].isin(MacValues)]        # remove randomised Mac Addresses
    dfData.sort_values(['mac','time'], ascending=[True,True], inplace=True)
    PMac = dfData.mac.unique()
    YMac = InitMacs-XMac-len(PMac)
    print ('File ',file1date,' loaded, ',XMac,' exclusions and ',YMac,' randomised Macs removed, leaving ',len(PMac),'Macs.')
    logRun.write ('File '+str(file1date)+' initial rows '+str(InitSize)+', '+str(XMac)+' and '+str(YMac)+' excluded & randomised Macs removed, leaving '+str(len(PMac))+'\n')
    try:
        os.makedirs(rootDir+file1path+subDirout)                # Create the Cleaned sub-directory if it does not already exist
    except IOError as e:
        if e.errno!=errno.EEXIST:
            raise
    dfMacs=pd.DataFrame(PMac,columns=['mac'])
    dfMacs.to_csv(rootDir+file1path+subDirout+'TUI_D1_Macs'+file1date+'.csv.gz', header=False, index=False, compression='gzip')

# 4. Identify the Mac Addresses that are common to those in the data files on days prior to and after the target file.
    if dfData2.empty==True:                                     # if previous day's Mac addresses not already in memory, load them 
        file2date="{:%m-%d-%Y}".format(date1-timedelta(days=1))
        file2path="{:%y%m}".format(date1-timedelta(days=1))
        try:
            inputFile=gzip.open(rootDir+file2path+subDirout+'TUI_D1_Macs'+file2date+'.csv.gz')
        except Exception as e:
            try:
                inputFile=gzip.open(rootDir+file2path+'\\'+file+file2date+'.csv.gz')
            except Exception as e:
                logRun.write('Error occurred trying to open file dated '+file2date+', Programme terminated.'+'\n')
                logRun.write('  '+str(e)+'\n')
                print('Error opening file day before - ',e)
                break
        try:
            dfData2 = pd.read_csv(inputFile , usecols= [0], names=['mac'])
        except Exception as e:
            logRun.write('Error occurred trying to read file dated '+file2date+', Programme terminated.'+'\n')
            logRun.write('  '+str(e)+'\n')
            print ('Error reading file day before - ',e)
            break
        else:
            inputFile.close()   
    if dfData4.empty==True:                                   # if Mac addresses for 2 days ago not already in memory, load them
        file4date="{:%m-%d-%Y}".format(date1-timedelta(days=2))
        file4path="{:%y%m}".format(date1-timedelta(days=2))
        try:
            inputFile=gzip.open(rootDir+file4path+subDirout+'TUI_D1_Macs'+file4date+'.csv.gz')
        except Exception as e:
            try:
                inputFile=gzip.open(rootDir+file4path+'\\'+file+file4date+'.csv.gz')
            except Exception as e:
                logRun.write('Error occurred trying to open file dated '+file4date+', Programme terminated.'+'\n')
                logRun.write('  '+str(e)+'\n')
                print('Error opening file 2 days before - ',e)
                break
        try:
            dfData4 = pd.read_csv(inputFile , usecols= [0], names=['mac'])
        except Exception as e:
            logRun.write('Error occurred trying to read file dated '+file4date+', Programme terminated.'+'\n')
            logRun.write('  '+str(e)+'\n')
            print ('Error reading file 2 days before - ',e)
            break
        else:
            inputFile.close()
    TgtMacs = dfData2.mac.unique()   
    dfData4 = dfData4[dfData4['mac'].isin(TgtMacs)]         # identify Mac Address common with previous 2 days
    TgtMacs = dfData4.mac.unique()
    print ('Macs common between previous 2 days = ',len(TgtMacs))
    dfCom = dfData[dfData['mac'].isin(TgtMacs)]
    CMac = len(dfCom.mac.unique())
    if CMac < findMore:                                     # if common Mac Address < threshold, add those common with next evening
        file3date="{:%m-%d-%Y}".format(date1+timedelta(days=1))
        file3path="{:%y%m}".format(date1+timedelta(days=1))
        try:
            inputFile=gzip.open(rootDir+file3path+'\\'+file+file3date+'.csv.gz')
        except Exception as e:
            logRun.write('Error occurred trying to open next days file dated '+file3date+', Programme terminated.'+'\n')
            logRun.write('  '+str(e)+'\n')
            print('Error opening next days file - ',e)
            break
        try:
            dfData3 = pd.read_csv(inputFile , names=['mac','type','time','user','area','site','bld','floor','x','y'], parse_dates=['time'])
        except Exception as e:
            logRun.write('Error occurred trying to read next days file dated '+file1date+', Programme terminated.'+'\n')
            logRun.write('  '+str(e)+'\n')
            print ('Error reading next days file - ',e)
            break
        inputFile.close()
        s = file3date+tfrom
        dayfrom = datetime.datetime.strptime(s,"%m-%d-%Y %H:%M:%S")
        print ('file for next day loaded')
        dfData4 = pd.concat([dfData4,dfData3[dfData3.time > dayfrom]])
        TgtMacs = dfData4.mac.unique()
        dfData = dfData[dfData['mac'].isin(TgtMacs)]
        fileMacs = len(dfData.mac.unique())
    else:
        dfData = dfCom
        fileMacs = CMac
        dfData3 = pd.DataFrame(columns=['mac','type','time','user','area','site','bld','floor','x','y'])
    print ('Macs in neighbouring files =',len(TgtMacs),'. With ',CMac,' Macs matching the 2 days before, Macs in common =',fileMacs)
    logRun.write('  Based on '+str(len(TgtMacs))+' Mac in neighbouring files and '+str(CMac)+' matching the 2 days before, file reduced to '+str(fileMacs)+' Macs.'+'\n')

# 5. Remove duplicate rows and Deck hops and add Geofencing
    Deck1=[]
    Row1=[None]*9
    Row2=[]
    Row3=[]
    Row4=[]
    n=0
    m=0
    FileData = dfData.values
    dfData = dfData3
    dfData3 = pd.DataFrame(columns=['mac','type','time','user','area','site','bld','floor','x','y'])
    dfData4 = dfData2
    dfData2 = dfMacs
    for row in FileData:
        if row[0]!=Row1[0]: #reset if Mac number changes and on first pass
            Prime=[]
            Count=0
            MacRows=0
        elif row[2]==Row1[2] and row[7]==Row1[7] and row[8]==Row1[8] and row[9]==Row1[9]:
            if Area1[0:3]=='PQU':
                Row1=row
            m=m+1
            continue
        Area1=str(row[4])
        if row[7]==Prime: #if Deck is stable just check the count and continue
            if Count<5:
                Count=Count+1
        else:
            if Count>0:       #if Deck changes reduce the prime count until 0
                Count=Count-1
            if row[7]==Row1[7]: #if Deck is the same 3 rows when count is 0, set the Prime deck
                if Count==0 and row[7]==Reset:
                    Prime=row[7]
                    Count=3
                else:
                    Reset=row[7] #if Deck is same for 2 row set Reset
            else:
                Reset=[]    #if Deck is not the same as the last row clear Reset
        MacRows=MacRows+1
        Row4=Row3
        Row3=Row2
        Row2=Row1
        Row1=row
        if Row2[7]=='Deck4' and Row2[8]>=197 and Row2[8]<=220:  # add additional GeoFenced areas
            if Row2[9]>=4 and Row2[9]<=15:
                Row2[4]='POO0004BRDWLNGSB'
            elif Row2[9]>=17 and Row2[9]<=27:
                Row2[4]='POO0004BRDWLNGP'
        if Row2[7]=='Deck5' and Row2[8]>=148 and Row2[8]<=164:
            if Row2[9]>=14 and Row2[9]<=23:
                Row2[4]='POO0005SHOPPRCNT'     
        if Row2[7]=='Deck6' and Row2[8]>=114 and Row2[8]<=124:
            if Row2[9]>=3 and Row2[9]<=7:
                Row2[4]='POO0006CFPRTSB'
            elif Row2[9]>=30 and Row2[9]<=33:
                Row2[4]='POO0006CFPRTP'
            elif Row2[9]>=9 and Row2[9]<=11:
                Row2[4]='POO0006CFPRTSBWIN'
        if Row2[7]=='Deck10' and Row2[8]>160 and Row2[8]<181 and Row2[9]<17:
            if Deck10bar.contains_point((Row2[8],Row2[9]))==True:
                Row2[4]='PVV0010DBAR10'
        if n<3 or Row4[7]!=Prime and Count>1 and MacRows>4:
            n=n+1
            continue
        TargetData.append(Row4)
    Size=len(TargetData)
    print (m,' duplicate and ',n-3,' deck hops removed, leaving ',Size,' rows (initial size - ',InitSize,')')
    TargetData.append(Row3)
    TargetData.append(Row2)
    TargetData.append(Row1)
    logRun.write ('  '+str(m)+' duplicate row and '+str(n-3)+' deck hops removed, leaving '+str(Size)+' rows - Geofencing added'+'\n')
    
    #  6. Create a new file containing the cleaned data and reset for next loop
    with gzip.open(rootDir+file1path+subDirout+cleaned+file1date+'.csv.gz','wt', newline='') as outputFile:
        csvWriter = csv.writer(outputFile)
        csvWriter.writerows(TargetData)
    outputFile.close()
    t2=time.time()
    print ('Loop time = ',t2-t1)
    logRun.write ('  Cleaned file saved at '+file1path+subDirout+cleaned+file1date+'.csv.gz'+', loop time ='+str(t2-t1)+'\n'+'\n')
    date1=date1+timedelta(days=1)
    t1=t2
    continue
t2=time.time()
logRun.write ('Run Time = '+str((t2-t0)/60)+' mins')
logRun.close()

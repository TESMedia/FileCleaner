Prog='FILE CLEANER v1.7' # - 12 Sep 2017 - Jon Masters 
# v6 - added Discovery 2, and add a deck count validation
# v7 - changes to support new file format introduced from 1 Sept
# This Programme will clean a set of data files ready for detailed analysis by:
#  a) removing data rows related to randomised Mac Addresses
#  b) removing Mac Address included in an exclusion file (static devices and crew)
#  c) removing the duplicate 'Quadrant' data row when GEO FENCED areas overlap
#  d) identify Mac Addresses onboard 2 days prior or one day after to eliminate harbour noise
#  e) removing deck hops and adding GEOFENCING

# 1.  Configuration
Sites = ["D1","D2"]
rootDir = 'c:\\Users\\Jon Masters\\Documents\\Python\\' # input files to be located at 'rootDir + YYMM (of file)'
#rootDir = 'c:\\Users\\porro\\Documents\\Thomson Cruises\\sftp\\discovery1\\'
cleaned = 'TUI_'                                        # name allocated to cleaned file = cleaned+site+'location_data_MM-DD-YYYY.csv.gz'
excludeDays = 22                                        # Macs that occur longer than this number of days are exluded
runFrom = 24                                            # days prior to current day processing starts from (normally 15)
findMore = 1500                                         # if Common Macs falls below this threshold, next day is added
tfrom = ' 18:00:00'

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
Barcoords =[ [188.01, 21.65],                           # Configuration - Coordinates for Kids Club
           [ 188.01, 25.33],
           [ 183.64, 25.33],
           [ 183.64, 28.37], 
           [ 197.02, 28.37],
           [ 197.02, 21.65] ]
Deck10KidsClub = mplPath.Path(np.array(Barcoords))
Barcoords =[ [186.52, 12.81],                           # Configuration - Coordinates for The Hide Out
           [ 189.21, 12.81],
           [ 189.21,  9.61],
           [ 197.03,  9.61], 
           [ 197.03, 21.24],
           [ 186.58, 21.24],
           [ 186.52, 17.31] ]
Deck10HideOut = mplPath.Path(np.array(Barcoords))
Barcoords =[ [188.94, 12.65],                           # Configuration - Coordinates for Gamer Zone
           [ 185.15, 12.65],
           [ 185.15, 13.78],
           [ 183.62, 13.78], 
           [ 183.62,  9.59],
           [ 189.0,   9.59] ]
Deck10GamerZone = mplPath.Path(np.array(Barcoords))
Barcoords =[ [183.3,  9.58],                           # Configuration - Coordinates for Baby Centre
           [ 183.3,  12.48],
           [ 179.69, 12.48],
           [ 178.4,  13.75], 
           [ 178.4,  15.27],
           [ 177.37, 15.27],
           [ 177.37, 16.16],
           [ 172.69, 16.16], 
           [ 172.71, 13.77],
           [ 174.14, 12.58],
           [ 177.8,  12.58],
           [ 180.65,  9.58] ]
Deck10BabyCentre = mplPath.Path(np.array(Barcoords))
Barcoords =[ [40.61, 23.02],                           # Configuration - Coordinates for Climbing Wall
           [ 40.61, 15.01 ],
           [ 30.96, 15.01],
           [ 30.96,  23.02 ] ]
Deck10ClimbingWall = mplPath.Path(np.array(Barcoords))
Barcoords =[ [60.5, 8.92],                           # Configuration - Coordinates for Mini Golf
           [ 40.98, 8.92],
           [ 40.98, 28.96],
           [ 60.5,  28.96] ]
Deck10MiniGolf = mplPath.Path(np.array(Barcoords))

import time                                            # Establish current time
t0=time.time()
t1=t0
import pandas as pd
import datetime
from datetime import date
from datetime import timedelta
import glob
import os
import errno
import gzip
import csv
import logging
print (Prog)

# 2. Initial set up, establish a start date for Files to be cleaned, and initiate loop
currentdate = datetime.datetime.today()                # create a run log file with current DateTime in file name 
logdate="{:%m%d-%H%M}".format(currentdate)
logging.basicConfig(filename=rootDir+'Log for run '+logdate+'.txt',format='%(levelname)s: %(message)s',level=logging.DEBUG)
logging.info('Log '+logdate+',  '+Prog)

for Site in Sites:
    print ('Starting Processing for Site ',Site)
    logging.info('Logging data for files processed at Site'+Site)
    if Site == "D1":
        subDirout = '\\Cleaned\\'                               # output file is written to    'rootDir + YYMM + subDirout'
        file = '10.1.100.11_history_data_'                      # name of files are to be in format 'file+MM-DD-YYYY.csv.gz'
        macexclusions = 'exclusionsDiscovery1'                  # exclusions file to be located at 'rootDir'    
    else:
        subDirout = '\\CleanedD2\\'                             # output file is written to    'rootDir + YYMM + subDirout'
        file = '10.2.50.5_history_data_'                        # name of files are to be in format 'file+MM-DD-YYYY.csv.gz'
        macexclusions = 'exclusionsDiscovery2'                  # exclusions file to be located at 'rootDir'

    date1=currentdate-timedelta(days=4)
    x=0
    y=0
    while x<excludeDays:                                   #identify any extra Mac Addresses to Exclude
        date1=date1-timedelta(days=1)
        file1date="{:%m-%d-%Y}".format(date1)
        file1path="{:%y%m}".format(date1)
        try:
            dfData = pd.read_csv(rootDir+file1path+subDirout+cleaned+Site+'_Macs'+file1date+'.csv.gz', usecols = [0], names=['mac'])
        except IOError as e:
            y=y+1
            if y>20:
                dfMacs=pd.DataFrame(columns=['mac'])
                break
            continue
        dfData.mac = dfData.mac.str.lower()
        MacList = dfData.mac.unique()
        if x==0:
            dfMacs=dfData
        else:
            dfMacs=dfMacs[dfMacs.mac.isin(MacList)]
        x=x+1
    try:                                                   # obtain the list of excluded Mac Addresses
        dfExcl = pd.read_csv(rootDir+macexclusions+'.csv', usecols= [0,1,2], header=0)
    except Exception as e:
        logging.info('Error occurred reading Mac Address exclusion file'+'\n'+'      '+str(e))
        print ('Reading exclusions file failed',e)
        dfExcl = pd.DataFrame(columns=['mac','type','used'])
    dfExcl['mac']=dfExcl['mac'].str.lower()
    MacExcl= dfExcl.mac.unique()
    dfMacs=dfMacs[dfMacs.mac.isin(MacExcl)==False]         # add any extra Mac Addresses to exclude
    dfMacs['type'] = 'Added '+logdate
    dfExcl=pd.concat([dfExcl,dfMacs])
    dfExcl.sort_values(['mac'], ascending=[True], inplace=True)
    MacExcl= dfExcl.mac.unique()                           # establish data for start up and initiate loop
    dfUnused = dfExcl
    print ('With',len(dfMacs),'added, total Mac exclusions =',len(MacExcl),'(',x,' Mac Files used,',y,' not found)')
    logging.info ('With '+str(len(dfMacs))+' added, total Mac exclusions = '+str(len(MacExcl))+' ('+str(x)+' used,'+str(y)+' not found)')
    dfData = pd.DataFrame(columns=['mac','trilat_result','last_updated_time','dwell_periods','area','site','building','floor','xcoords','ycoords'])
    dfData2 = pd.DataFrame(columns=['mac'])
    dfData4 = pd.DataFrame(columns=['mac'])
    date1=currentdate-timedelta(days=runFrom)
    while date1<currentdate:

    # 3. If no cleaned file exists, load the original file and remove Randomised and Excluded Mac Addresses
        TargetData=[]
        file1date="{:%m-%d-%Y}".format(date1)
        file1path="{:%y%m}".format(date1)
        if os.path.isfile(rootDir+file1path+subDirout+cleaned+Site+'_location_data_'+file1date+'.csv.gz')==True:
            logging.info('Cleaned file found for date '+file1date)
            date1=date1+timedelta(days=1)
            print (file1date,'already cleaned')
            dfData = pd.DataFrame(columns=['mac','trilat_result','last_updated_time','dwell_periods','area','site','building','floor','xcoords','ycoords'])
            dfData4 = dfData2
            dfData2 = pd.DataFrame(columns=['mac'])                        
            continue
        if dfData.empty==True:
            try:
                all_file_parts = glob.glob(os.path.join(rootDir+file1path+"\\"+file+file1date+".csv*.gz"))
                fileCheck = pd.read_csv(all_file_parts[0],nrows=1)
                if len(fileCheck.columns) == 10:
                    df_from_each_file = (pd.read_csv(f, names=['mac','trilat_result','time','dwell_periods','area','site','bld','floor','x','y'], parse_dates = ['time']) for f in all_file_parts)
                    dfData = pd.concat(df_from_each_file, ignore_index=True)
                else:
                    df_from_each_file = (pd.read_csv(f) for f in all_file_parts)
                    dfData = pd.concat(df_from_each_file, ignore_index=True)
                    dfData.rename(columns={'last_updated_time':'time','building':'bld','xcoords':'x','ycoords':'y'},inplace='true')
                    dfData = dfData[['mac','trilat_result','time','dwell_periods','area','site','bld','floor','x','y']]             
            except Exception as e:
                logging.error('Error occurred trying to read file to be cleaned dated '+file1date+', Programme terminated.'+'\n'+'      '+str(e))
                print ('Error reading file to be cleaned - ',e)
                break
            dfData['mac']=dfData['mac'].str.lower()   
        InitSize = len(dfData)                                      # establish initial size of file
        InitMacs = len(dfData['mac'].unique())
        dfData = dfData[dfData.mac.str[1:2].isin(['0','1','4','5','8','9','c','d'])]        # remove randomised Mac Addresses
        MainMacs = dfData.mac.unique()
        XMac = InitMacs-len(MainMacs)    
        dfUnused = dfUnused[dfUnused.mac.isin(MainMacs)==False]
        dfData = dfData[dfData['mac'].isin(MacExcl)==False]         # remove excluded Mac Addresses
        dfData.sort_values(['mac','time'], ascending=[True,True], inplace=True)
        PMac = dfData.mac.unique()
        YMac = InitMacs-XMac-len(PMac)
        print ('File ',file1date,' loaded, ',XMac,' randomised and ',YMac,' exclusion Macs removed, leaving ',len(PMac),'Macs.')
        logging.info('File '+str(file1date)+' initial rows '+str(InitSize)+', '+str(XMac)+' random and '+str(YMac)+' excluded Macs removed, leaving '+str(len(PMac)))
        try:
            os.makedirs(rootDir+file1path+subDirout)                # Create the Cleaned sub-directory if it does not already exist
        except IOError as e:
            if e.errno!=errno.EEXIST:
                raise
        dfMacs=pd.DataFrame(PMac,columns=['mac'])
        dfMacs.to_csv(rootDir+file1path+subDirout+cleaned+Site+'_Macs'+file1date+'.csv.gz', header=False, index=False, compression='gzip')
        decks = dfData.floor.unique()
        if len(decks)<12: 
            print ('WARNING - data only exists for decks ',decks)
            logging.info('WARNING - data only exists for decks '+decks)
    # 4. Identify the Mac Addresses that are common to those in the data files on days prior to and after the target file.
        if dfData2.empty==True:                                     # if previous day's Mac addresses not already in memory, load them 
            file2date="{:%m-%d-%Y}".format(date1-timedelta(days=1))
            file2path="{:%y%m}".format(date1-timedelta(days=1))
            try:
                dfData2= pd.read_csv(rootDir+file2path+subDirout+cleaned+'Macs'+file2date+'.csv.gz', usecols= [0], names=['mac'])
            except Exception as e:
                try:
                    all_file_parts = glob.glob(os.path.join(rootDir+file2path+"\\"+file+file2date+".csv*.gz"))
                    fileCheck = pd.read_csv(all_file_parts[0],nrows=1)
                    if len(fileCheck.columns) == 10:
                        df_from_each_file = (pd.read_csv(f, usecols=[0], names=['mac']) for f in all_file_parts)
                        dfData2 = pd.concat(df_from_each_file, ignore_index=True)
                    else:
                        df_from_each_file = (pd.read_csv(f, usecols=['mac']) for f in all_file_parts)
                        dfData2 = pd.concat(df_from_each_file, ignore_index=True)             
                except Exception as e:
                    logging.error('No file found for date '+file2date+'.'+'\n'+'      '+str(e))
                    print('No file found for day before - ',e)
            dfData2['mac'] = dfData2['mac'].str.lower()
        TgtMacs = dfData2.mac.unique()   
        if dfData4.empty==True:                                   # if Mac addresses for 2 days ago not already in memory, load them
            file4date="{:%m-%d-%Y}".format(date1-timedelta(days=2))
            file4path="{:%y%m}".format(date1-timedelta(days=2))
            try:
                dfData4=pd.read_csv(rootDir+file4path+subDirout+cleaned+'Macs'+file4date+'.csv.gz', usecols= [0], names=['mac'])
            except Exception as e:
                try:
                    all_file_parts = glob.glob(os.path.join(rootDir+file4path+"\\"+file+file4date+".csv*.gz"))
                    fileCheck = pd.read_csv(all_file_parts[0],nrows=1)
                    if len(fileCheck.columns) == 10:
                        df_from_each_file = (pd.read_csv(f, usecols=[0], names=['mac']) for f in all_file_parts)
                        dfData4 = pd.concat(df_from_each_file, ignore_index=True)
                    else:
                        df_from_each_file = (pd.read_csv(f, usecols=['mac']) for f in all_file_parts)
                        dfData4 = pd.concat(df_from_each_file, ignore_index=True)             
                except Exception as e:
                    logging.error('No file found for date '+file4date+'.'+'\n'+'      '+str(e))
                    print('No file found for 2 days before - ',e)
            dfData4['mac'] = dfData4['mac'].str.lower()
        if len(dfData4)>0:
            if len(TgtMacs)>0:
                dfData4 = dfData4[dfData4['mac'].isin(TgtMacs)]         # identify Mac Address common with previous 2 days
            TgtMacs = dfData4.mac.unique()
        if len(TgtMacs)==0:
            break
        print ('Macs common between previous 2 days = ',len(TgtMacs))
        dfCom = dfData[dfData['mac'].isin(TgtMacs)]
        CMac = len(dfCom.mac.unique())
        if CMac < findMore:                                     # if common Mac Address < threshold, add those common with next evening
            file3date="{:%m-%d-%Y}".format(date1+timedelta(days=1))
            file3path="{:%y%m}".format(date1+timedelta(days=1))
            try:
                all_file_parts = glob.glob(os.path.join(rootDir+file3path+"\\"+file+file3date+".csv*.gz"))
                fileCheck = pd.read_csv(all_file_parts[0],nrows=1)
                if len(fileCheck.columns) == 10:
                    df_from_each_file = (pd.read_csv(f, names=['mac','trilat_result','time','dwell_periods','area','site','bld','floor','x','y'], parse_dates = ['time']) for f in all_file_parts)
                    dfData3 = pd.concat(df_from_each_file, ignore_index=True)
                else:
                    df_from_each_file = (pd.read_csv(f, usecols=['mac','trilat_result','last_updated_time','dwell_periods','area','site','building','floor','xcoords','ycoords'], parse_dates = ['last_updated_time']) for f in all_file_parts)
                    dfData3 = pd.concat(df_from_each_file, ignore_index=True)
                    dfData3.rename(columns={'last_updated_time':'time','building':'bld','xcoords':'x','ycoords':'y'},inplace='true')
                    dfData3 = dfData3[['mac','trilat_result','time','dwell_periods','area','site','bld','floor','x','y']]             
            except Exception as e:
                logging.error('Error occurred trying to read next days file dated '+file1date+', Programme terminated.'+'\n'+'      '+str(e))
                print ('Error reading next days file - ',e)
                break
            dfData3['mac'] = dfData3['mac'].str.lower()
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
        logging.info('  Based on '+str(len(TgtMacs))+' Mac in neighbouring files and '+str(CMac)+' matching the 2 days before, file reduced to '+str(fileMacs)+' Macs.')
    #    Size=len(dfData)
    #    check_df = dfData[pd.notnull(dfData['area'])]   # This separates out any areas that are comma separated                                  
    #    check_df = check_df[check_df['area'].str.contains(',')]
    #    print (len(check_df),'rows found with areas separated by commas,',end='')
    #    check_df_ix = check_df.index                    # get indexes of split area rows
    #    for ix in check_df_ix:                          # loop through these indexes
    #        tmp_add = check_df.loc[ix]                  # copy row, then delete from dfData
    #        dfData.drop(ix,axis=0,inplace=True)
    #        for area in check_df.loc[ix,'area'].strip().split(','): # loop to seperate out areas
    #            tmp_add_sgl = tmp_add.copy(deep=True)
    #            tmp_add_sgl['area'] = area
    #            dfData = dfData.append(tmp_add_sgl)      
    #    dfData.reset_index()                            #reset index
    #    print (' and',len(dfData)-Size,' rows added to separate them out')

    # 5. Remove duplicate rows and Deck hops and add Geofencing
        Deck1=[]
        Row1=[None]*9
        Row2=[]
        Row3=[]
        Row4=[]
        n=0
        m=0
        p=0
        for row in dfData.values:
            Areas = str(row[4])
            Areas = Areas.split(",")
            if len(Areas)>1:
                m=m+1
                if Areas[0][0:3]!="PQU":
                    row[4]=Areas[0]
                else:
                    row[4]=Areas[1]
                if len(Areas)>2:
                    p=p+1
            row[4]=str(row[4])
            if row[0]!=Row1[0]: #reset if Mac number changes and on first pass
                Prime=[]
                Count=0
                MacRows=0
            elif row[2]==Row1[2] and row[7]==Row1[7] and row[8]==Row1[8] and row[9]==Row1[9]:
                print (Row1[4][0:3],',',end="")
                if Row1[4][0:3]=='PQU':
                    Row1=row
                print (Row1[4][0:3])
                m=m+1
                continue
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
            if Row2[7]=='Deck3' and Row2[8]>=119.5 and Row2[8]<=122.9:  # add additional GeoFenced areas
                if Row2[9]>=20.9 and Row2[9]<=27.6:
                    Row2[4]='POO0003PHOTOSTUDIOP'
                elif Row2[9]>=10.6 and Row2[9]<=17.3:
                    if Site == 'D1':
                        Row2[4]='POO0003PHOTOSTUDIOSB'
                    else:
                        Row2[4]='POO0003VREXP'
            if Row2[7]=='Deck3' and Row2[8]>=122.9 and Row2[8]<=126.8:
                if Row2[9]>=20.9 and Row2[9]<=27.6:
                    if Site == 'D1':
                        Row2[4]='POO0003BREAKOUTP'
                    else:
                        Row2[4]='POO0003PHOTOSTUDIOP'
                elif Row2[9]>=10.6 and Row2[9]<=17.3:
                    if Site == 'D1':
                        Row2[4]='POO0003BREAKOUTSB'
                    else:
                        Row2[4]='POO0003GREENSCREEN'
            if Site == 'D1' and Row2[7]=='Deck10':
                if Deck10bar.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='PVV0010DBAR10'
                elif Deck10KidsClub.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='POO0010KIDSCL'
                elif Deck10HideOut.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='POO0010HIDEOUT'            
                elif Deck10GamerZone.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='POO0010GZONE'
                elif Deck10BabyCentre.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='POO0010BABYC'
                elif Deck10ClimbingWall.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='POO0010WALL'
                elif Deck10MiniGolf.contains_point((Row2[8],Row2[9]))==True:
                    Row2[4]='POO0010MINIGLF' 
            if n<3 or Row4[7]!=Prime and Count>1 and MacRows>4:
                n=n+1
                continue
            TargetData.append(Row4)
        TargetData.append(Row3)
        TargetData.append(Row2)
        TargetData.append(Row1)
        Size=len(TargetData)
        print (m,' duplicate and ',n-3,' deck hops removed, leaving ',Size,' rows (initial size -',InitSize,')')
        if p>0:
            print ('WARNING - ',p,' rows detected that had more than 2 comma separated areas defined - only one retained') 
        logging.info ('  '+str(m)+' duplicate row and '+str(n-3)+' deck hops removed, leaving '+str(Size)+' rows - Geofencing added')
        dfData = dfData3
        dfData3 = pd.DataFrame(columns=['mac','trilat_result','time','dwell_periods','area','site','bld','floor','x','y'])   
        dfData4 = dfData2
        dfData2 = dfMacs
    
        #  6. Create a new file containing the cleaned data and reset for next loop
        with gzip.open(rootDir+file1path+subDirout+cleaned+Site+'_location_data_'+file1date+'.csv.gz','wt', newline='') as outputFile:
            csvWriter = csv.writer(outputFile)
            csvWriter.writerows(TargetData)
        outputFile.close()
        t2=time.time()
        print ('Loop time = ',str(int(t2-t1)),'secs')
        logging.info ('  Cleaned file saved at '+file1path+subDirout+cleaned+'location_data_'+file1date+'.csv.gz'+', loop time '+str(int(t2-t1))+'s'+'\n')
        date1=date1+timedelta(days=1)
        t1=t2
        continue
    MacList=dfUnused.mac.unique()
    dfExcl=dfExcl[dfExcl.mac.isin(MacList)==False]
    dfExcl['used']=logdate
    dfExcl=pd.concat([dfExcl,dfUnused])
    dfExcl.sort_values(['mac'], ascending=[True], inplace=True)
    dfExcl.to_csv(rootDir+macexclusions+'.csv', header=True, index=False)
    t2=time.time()
    logging.info ('Run Time = '+str((t2-t0)/60)+' mins')

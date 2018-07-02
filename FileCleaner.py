Prog='FILE CLEANER v3.0' # - 14 Mar 2018 - Jon Masters 
# 3.0 - amended process to remove short journeys, retain Mac Addresses present across 5 day periods, and create a visits file
# This Programme will clean a set of data files ready for detailed analysis by:
#  a) removing data rows related to randomised Mac Addresses
#  b) removing Mac Address included in an exclusion file (static devices and crew)
#  c) removing the duplicate 'Quadrant' data row when GEO FENCED areas overlap
#  d) identify Mac Addresses onboard 2 days prior or one day after to eliminate harbour noise
#  e) removing deck hops and adding GEOFENCING

# 1.  Configuration
Sites = ["EXP","DI2","DI1"]
RunType ='Routine'                                          # enter either 'Routine','Gap' or 'Save'
rootDir = 'c:\\Users\\Jon Masters\\Documents\\Python\\' # input files to be located at 'rootDir + YYMM (of file)'
#rootDir = 'c:\\Users\\porro\\Documents\\Thomson Cruises\\sftp\\discovery1\\'
cleaned = 'TUI_'                                        # name allocated to cleaned file = cleaned+site+'location_data_MM-DD-YYYY.csv.gz'
excludeDays = 22                                        # Macs that occur longer than this number of days are exluded
runFrom = 12                                            # days prior to current day processing starts from (normally 15)
tfrom = ' 18:00:00'
print (Prog,'\n')

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

delta_secs = 1800
dwell_secs = 60
group1 = ['BGJSHOP','BLSSHOP','BFFWSHOP','BPERFSHOP','BSOUVSHOP']
group2 = ['POO000447DEGP','POO000447DEGSB','PQQ00547DEGGALL','PQQ00547DEG']
group3 = ['POO0005VEN2NDLNG','POO0005VENMIDLNG','POO0005VENCONLNG','POO0005VENENT']
group4 = ['POO0004BRDWLNGP','POO0004BRDWLNGSB']
group5 = ['POO0009WHPOOL9P','POO0009WHPOOL9SB']
group6 = ['POO0006CFPRTSB','POO0006CFPRTP','POO0003CFPRTWIN']

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


# 2. Initial set up, establish a start date for Files to be cleaned, and initiate loop
currentdate = datetime.datetime.today()                # create a run log file with current DateTime in file name 
logdate="{:%y%b%d-%H%M}".format(currentdate)
logging.basicConfig(filename=rootDir+'Log for run '+logdate+'.txt',format='%(levelname)s: %(message)s',level=logging.DEBUG)
logging.info('Log '+logdate+',  '+Prog)

for Site in Sites:
    print ('Starting Processing for Site ',Site)
    logging.info('Logging data for files processed at Site'+Site)
    subDirout = '\\Cleaned'+Site+'\\'                        # output file is written to    'rootDir + YYMM + subDirout'
    file = 'TUI_'+Site+'_history_data_'                      # name of files are to be in format 'file+MM-DD-YYYY.csv.gz'
    macexclusions = 'exclusions'+Site                        # exclusions file to be located at 'rootDir'    
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
            if y>21:
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
        print (' Reading exclusions file failed',e)
        dfExcl = pd.DataFrame(columns=['mac','type','used'])
    dfExcl['mac']=dfExcl['mac'].str.lower()
    MacExcl= dfExcl.mac.unique()
    dfMacs=dfMacs[dfMacs.mac.isin(MacExcl)==False]         # add any extra Mac Addresses to exclude
    dfMacs['type'] = 'Added '+logdate
    dfExcl=pd.concat([dfExcl,dfMacs])
    dfExcl.sort_values(['mac'], ascending=[True], inplace=True)
    MacExcl= dfExcl.mac.unique()                           # establish data for start up and initiate loop
    dfUnused = dfExcl
    print (' With',len(dfMacs),'added, total Mac exclusions =',len(MacExcl),'(',x,' Mac Files used,',y,' not found)')
    logging.info ('With '+str(len(dfMacs))+' added, total Mac exclusions = '+str(len(MacExcl))+' ('+str(x)+' used,'+str(y)+' not found)')
    dfMacs = pd.DataFrame(columns=['mac'])
    dfData = pd.DataFrame(columns=['mac'])
    dfData1 = pd.DataFrame(columns=['mac'])
    dfData2 = pd.DataFrame(columns=['mac'])   
    noorg=0
    date1=currentdate-timedelta(days=runFrom)
    while date1<currentdate:

# 3. If no cleaned file exists, load the original file and remove unwanted Mac Addresses
        TargetData=[]
        file1date="{:%m-%d-%Y}".format(date1)
        file1path="{:%y%m}".format(date1)
        if os.path.isfile(rootDir+file1path+subDirout+cleaned+Site[0]+Site[-1]+'_location_data_'+file1date+'.csv.gz')==True:
            logging.info('Cleaned file found for date '+file1date)
            date1=date1+timedelta(days=1)
            print ('File ',file1date,'already cleaned')
            dfData = dfData1
            dfData1= dfData2
            dfData2= pd.DataFrame(columns=['mac'])                        
            continue
        if dfData.empty==True:
            try:
                all_file_parts = glob.glob(os.path.join(rootDir+file1path+"\\"+file+file1date+".csv*.gz"))
                df_from_each_file = (pd.read_csv(f, parse_dates=['start_ts','last_updated_time']) for f in all_file_parts)
                dfData = pd.concat(df_from_each_file, ignore_index=True) 
            except Exception as e:
                logging.error('Error occurred trying to read original file dated '+file1date+'\n'+'      '+str(e))
                print ('Error reading original file',file1date,' - ',e)
                noorg=noorg+1
                if noorg>2:
                    print ()
                    break
                else:
                    date1=date1+timedelta(days=1)
                    dfData = dfData1
                    dfData1= dfData2
                    dfData2= pd.DataFrame(columns=['mac'])           
                    continue
        noorg=0
        dfData['mac']=dfData['mac'].str.lower()   
        InitSize = len(dfData)                                      # establish initial size of file
        InitMacs = len(dfData['mac'].unique())
        dfData = dfData[dfData.mac.str[1:2].isin(['0','1','4','5','8','9','c','d'])]        # remove randomised Mac Addresses
        MainMacs = dfData.mac.unique()
        XMac = InitMacs-len(MainMacs)    
        dfUnused = dfUnused[dfUnused.mac.isin(MainMacs)==False]
        dfData = dfData[dfData['mac'].isin(MacExcl)==False]         # remove excluded Mac Addresses
        dfData.sort_values(['mac','last_updated_time'], ascending=[True,True], inplace=True)
        dfData = dfData.reset_index(drop=True)
        NMac = len(dfData.mac.unique())
        YMac = InitMacs-XMac-NMac
        print ('File ',file1date,' loaded, ',XMac,' randomised and ',YMac,' exclusion Macs removed, leaving ',NMac,'Macs.')
        logging.info('File '+str(file1date)+' initial rows '+str(InitSize)+', '+str(XMac)+' random and '+str(YMac)+' excluded Macs removed, leaving '+str(NMac))
        if len(dfData.floor.unique())<12:
            decks = len(dfData.floor.unique()) 
            print ('    WARNING - data only exists for decks ',decks)
            logging.info('WARNING - data only exists for decks '+str(decks))
        dfStart = dfData.drop_duplicates(subset=['mac'], keep='first', inplace=False)
        dfEnd = dfData.drop_duplicates(subset=['mac'], keep='last', inplace=False)
        dfStart = pd.concat([dfStart,dfEnd])
        dfStart = dfStart[['mac','start_ts','last_updated_time']]
        dfStart['rows'] = dfStart.index.values
        dfStart.sort_values(['mac','last_updated_time'], ascending=[True,True], inplace=True)
        dfStart = dfStart.reset_index(drop=True)
        dfStart.loc[(dfStart['mac']==dfStart['mac'].shift(-1)), ['rows']] = 1+dfStart['rows'].shift(-1)-dfStart['rows']
        dfStart['dwell'] = ((dfStart['last_updated_time'].shift(-1)-dfStart['last_updated_time']).dt.seconds)
        dfStart['last_updated_time'] = dfStart['last_updated_time'].shift(-1)
        dfStart = dfStart.drop_duplicates(subset=['mac'], keep='first', inplace=False)
        dfStart = dfStart[(dfStart['rows']>3)]
        dfStart.loc[((dfStart['last_updated_time']-dfStart['start_ts']).dt.seconds) > dfStart['dwell'], ['dwell']] = ((dfStart['last_updated_time']-dfStart['start_ts']).dt.seconds)
        dfStart = dfStart[(dfStart['dwell']>900)]
        PMac = dfStart.mac.unique()
        dfData = dfData[dfData['mac'].isin(PMac)]
        print ('    Mac Addresses with minimal data removed, leaving ',len(PMac),'Macs')
        logging.info('Mac Addresses with minimal data removed, leaving '+str(len(PMac))+' Macs')
        try:
            os.makedirs(rootDir+file1path+subDirout)                # Create the Cleaned sub-directory if it does not already exist
        except IOError as e:
            if e.errno!=errno.EEXIST:
                raise
        dfStart.to_csv(rootDir+file1path+subDirout+cleaned+Site+'_Macs'+file1date+'.csv.gz', index=False, compression='gzip')

# 4. Identify the Mac Addresses that are common to those in the data files on days prior to and after the target file.
        dfData0 = pd.DataFrame(columns=['mac'])
        x=0
        y=0
        while x<4:                              # Load the Mac Addresses for previous 3 days
            x=x+1
            fileDate="{:%m-%d-%Y}".format(date1-timedelta(days=x))
            filePath="{:%y%m}".format(date1-timedelta(days=x))
            displayDate="{:%d-%m-%Y}".format(date1-timedelta(days=x))
            try:
                dfData3 = pd.read_csv(rootDir+filePath+subDirout+cleaned+Site+'_Macs'+fileDate+'.csv.gz', usecols= [0], names=['mac'])
                dfData0 = pd.concat([dfData0,dfData3], ignore_index=True)
            except Exception as e:
                logging.error('No Macs found for date '+displayDate+'.'+'\n'+'      '+str(e))
                print('    No Macs found for',displayDate,' - ')
                y=y+1
            continue
        if y>1:
            if RunType=='Routine':
                logging.error('Insufficient data to validate Mac Addresses. Completing processing for this site')
                print('    Insufficient data to validate Mac Addresses.  Completing processing for this site')
                break
        else:
            print('    Loaded Mac Addresses for previous days with ',y,'files not found, now loading future files')
        fileDate="{:%m-%d-%Y}".format(date1+timedelta(days=1))          # load Macs addresses for 1 day in the future
        filePath="{:%y%m}".format(date1+timedelta(days=1))
        displayDate="{:%Y-%m-%d}".format(date1+timedelta(days=1))
        try:
            dfMacs = pd.read_csv(rootDir+filePath+subDirout+cleaned+Site+'_Macs'+fileDate+'.csv.gz', usecols= [0], names=['mac'])
        except Exception as e:
            try:
                all_file_parts = glob.glob(os.path.join(rootDir+filePath+"\\"+file+fileDate+".csv*.gz"))
                df_from_each_file = (pd.read_csv(f,usecols=['mac']) for f in all_file_parts)
                dfMacs = pd.concat(df_from_each_file, ignore_index=True)
            except Exception as e:               
                logging.error('Error reading next day Macs file dated '+displayDate+'\n'+'      '+str(e))
                print ('    Error reading next day Macs file dated ',displayDate,' - ')
                dfMacs = pd.DataFrame(columns=['mac'])
                x=6
        fileDate="{:%m-%d-%Y}".format(date1+timedelta(days=2))          # load file for 2 days in the future - save Mac Addresses
        filePath="{:%y%m}".format(date1+timedelta(days=2))
        displayDate="{:%Y-%m-%d}".format(date1+timedelta(days=2))
        try:
            all_file_parts = glob.glob(os.path.join(rootDir+filePath+"\\"+file+fileDate+".csv*.gz"))
            df_from_each_file = (pd.read_csv(f, parse_dates = ['last_updated_time','start_ts']) for f in all_file_parts)
            dfData2 = pd.concat(df_from_each_file, ignore_index=True)
            dfData2['mac'] = dfData2['mac'].str.lower()
            Macs2 = dfData2.mac.unique()
            df2Macs=pd.DataFrame(Macs2,columns=['mac'])
            try:
                os.makedirs(rootDir+filePath+subDirout)                # Create the Cleaned sub-directory if it does not already exist
            except IOError as e:
                if e.errno!=errno.EEXIST:
                    raise
            df2Macs.to_csv(rootDir+filePath+subDirout+cleaned+Site+'_Macs'+fileDate+'.csv.gz', index=False, compression='gzip')
            dfData0 = pd.concat([dfData0,dfMacs,df2Macs])
            checkMac = dfData0.mac.unique()
            dfData =  dfData[dfData['mac'].isin(checkMac)]
        except Exception as e:
            logging.error('Error reading future file dated '+displayDate+'\n'+'      '+str(e))
            print ('    Error reading future file dated ',displayDate)
            if RunType=='Routine':
                print ('Insufficient data to continue Routine run type, completing processing for Site',Site)
                logging.error('Insufficient data to continue a Routine run type - completing processing for this site')
                break
            else:
                dfData2 = pd.DataFrame()
                if x!=6 and y<3:
                    dfData0 = pd.concat([dfData0,dfMacs])
                    checkMac = dfData0.mac.unique()
                    dfData =  dfData[dfData['mac'].isin(checkMac)]                    
                    print('    File will be updated based on Mac Address data available')
                else:
                    if RunType=='Gap':
                        print('    Insufficient data to continue Gap runtype, completing processing for Site',Site)
                        logging.error('    Insufficient data to continue Gap run type - completing processing for this site')
                        break   
                    else:
                        print('    ',y,'files before and 0 after, but RunType Save so continuing with no further Macs removed')
                        logging.error ('LACK OF FILES before and after - file will be cleaned and saved with no further Macs removed')
        NMac = len(dfData.mac.unique())
        print ('    Macs removed if they do not occur in files 2 days before or after, leaving ',NMac,'Macs.')
        logging.info('  Macs removed if they do not occur in files 2 days before or after, leaving '+str(NMac)+' Macs.')
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
        dfData.rename(columns={'last_updated_time':'time','building':'bld','xcoords':'x','ycoords':'y'},inplace='true')
        dfData = dfData[['mac','trilat_result','time','dwell_periods','area','site','bld','floor','x','y']]            
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
                if Row1[4][0:3]=='PQU':
                    Row1=row
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
            if Row2[7]=='Deck6' and Row2[8]>=114 and Row2[8]<=124:  # add additional GeoFenced areas
                if Row2[9]>=9 and Row2[9]<=11:
                    Row2[4]='POO0006CFPRTSBWIN'
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
        print ('   ',m,' duplicate and ',n-3,' deck hops removed, leaving ',Size,' rows (initial size -',InitSize,')')
        if p>0:
            print ('   WARNING - ',p,' rows detected that had more than 2 comma separated areas defined - only one retained') 
        logging.info ('  '+str(m)+' duplicate row and '+str(n-3)+' deck hops removed, leaving '+str(Size)+' rows - Geofencing added')
    
        #  6. Create a new file containing the cleaned data and prepare file for creating visits
        dfTarget=pd.DataFrame(TargetData, columns=['mac','trilat_result','time','dwell_periods','area','site','bld','deck','x','y'])
        dfTarget.to_csv(rootDir+file1path+subDirout+cleaned+Site[0]+Site[-1]+'_location_data_'+file1date+'.csv.gz', header=True, index=False, compression='gzip')
        t2=time.time()
        print ('   Loop time = ',str(int(t2-t1)),'secs')
        logging.info ('  Cleaned file saved at '+file1path+subDirout+cleaned+Site[0]+Site[-1]+'_location_data_'+file1date+'.csv.gz'+', loop time '+str(int(t2-t1))+'s')
        dfTarget['seq']=0
        dfTarget['until']=dfTarget['time']
        dfTarget['area1'] = dfTarget['area']
        dfTarget['area1']=dfTarget['area1'].str.upper()
        dfTarget=dfTarget[(dfTarget['area1']!="NAN")]
        dfTarget.loc[(dfTarget['area1'].isin(group1)), ['area1']] = 'Shops'
        dfTarget.loc[(dfTarget['area1'].isin(group2)), ['area1']] = '47Degree'
        dfTarget.loc[(dfTarget['area1'].isin(group3)), ['area1']] = 'VenueLounge'
        dfTarget.loc[(dfTarget['area1'].isin(group4)), ['area1']] = 'BroadwayLounge'
        dfTarget.loc[(dfTarget['area1'].isin(group5)), ['area1']] = 'Whirlpool'
        dfTarget.loc[(dfTarget['area1'].isin(group6)), ['area1']] = 'CoffeePort'
        dfTarget.loc[(dfTarget.area1.str[0:3]=="PUU"), ['area1']] = dfTarget['area'].str[0:7]
        dfTarget.loc[(dfTarget.area1.str[0:3]=="PSU"), ['area1']] = dfTarget['area'].str[0:7]
        dfTarget.loc[(dfTarget.area1.str[0:3]=="PQU"), ['area1']] = dfTarget['area'].str[0:7]
#        dfTarget.loc[(dfTarget['area1']=="   "), ['area']] = 'On'+dfTarget['deck']
        dfTarget.sort_values(['mac','area1','time'], ascending=[True,True,True], inplace=True)
        dfTarget=dfTarget.reset_index(drop=True)
        print ('     Data updated to enable creation of visits')

#7  Establish data point sequences and create visits
        cond1 = dfTarget['area1']==dfTarget['area1'].shift(1)
        cond2 = (((dfTarget['time']-dfTarget['time'].shift(1)).dt.seconds)<(delta_secs+dfTarget['dwell_periods']*5))
        combd = (cond1&cond2)
        dfTarget.loc[combd, ['seq']] = 1
        dfTarget.loc[(dfTarget['seq']==(dfTarget['seq'].shift(-1))+1), ['seq']] = 2
        dfTarget=dfTarget[(dfTarget['seq']!=1)]
        dfTarget=dfTarget.reset_index()
        dfTarget['count'] = 1
        dfTarget['count'] = (dfTarget['index'].shift(-1)-dfTarget['index'])
        dfTarget.loc[(dfTarget['seq']+2==dfTarget['seq'].shift(-1)), ['until']] = dfTarget['time'].shift(-1)
        dfTarget.loc[(dfTarget['seq']+2==dfTarget['seq'].shift(-1)), ['count']] = dfTarget['count']+1
        dfTarget = dfTarget[(dfTarget['seq']!=2)]
        dfTarget = dfTarget[(dfTarget['area1']!="NAN")]
        NewSize = len(dfTarget)
        print ('     Journeys created, removing ',InitSize-NewSize,' rows of data and leaving',NewSize)
#8  Clean up Data file, identify length of each visit, and remove visits of zero time (single data point rows)
        dfTarget.sort_values(['mac','time'], ascending=[True,True], inplace=True)
        dfTarget=dfTarget.reset_index(drop=True)
        dfTarget.loc[(dfTarget.area1.str[0:3]=="PUU"), ['area']] = dfTarget['area1']
        dfTarget['t_diff']=0
        dfTarget.loc[(dfTarget['count']>1), ['t_diff']] = ((dfTarget['until']-dfTarget['time']).dt.seconds)
        dfTarget['dwell_periods']= dfTarget['dwell_periods']+dfTarget['count']
        dfTarget.drop(['index','seq','area1','site','bld','x','y','count'], axis=1, inplace=True, errors='ignore')
        MacCount=len(dfTarget.mac.unique())
        print ('     Preparation of data completed, file reduced to ',len(dfTarget),'rows,',MacCount,' Mac Addresses')
        logging.info ('    Visits file created with '+str(len(dfTarget))+' data rows, Mac Addresses ='+str(MacCount))

#9  Save visits file and prepare for next loop
        dfTarget.to_csv(rootDir+file1path+subDirout+cleaned+Site+'_visits_'+file1date+'.csv.gz', header=True, index=False, compression='gzip')
        t2=time.time()
        print ('     Visits file saved -  Total loop time = ',str(int(t2-t1)),'secs','\n')
        logging.info ('  Visits file saved at '+file1path+subDirout+cleaned+Site+'_visits'+file1date+'.csv.gz'+', loop time '+str(int(t2-t1))+'s'+'\n')
        date1=date1+timedelta(days=1)
        t1=t2
        dfData = dfData1
        dfData1= dfData2
        dfData3= pd.DataFrame()
        continue
    MacList=dfUnused.mac.unique()
    dfExcl=dfExcl[dfExcl.mac.isin(MacList)==False]
    dfExcl['used']=logdate
    dfExcl=pd.concat([dfExcl,dfUnused])
    dfExcl.sort_values(['mac'], ascending=[True], inplace=True)
    dfExcl.to_csv(rootDir+macexclusions+'.csv', header=True, index=False)
    t2=time.time()
    logging.info ('Run Time = '+str((t2-t0)/60)+' mins')

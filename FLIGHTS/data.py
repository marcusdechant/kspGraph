from ColorOut import CO
from doNotGit.psqlConn import psql
import pandas as pd
import numpy as np
from time import sleep as sl
from psycopg2 import errors, errorcodes

# data processing for ksp flight telemetry
# by: marcus dechant

# global text 
nl='\n'

# global math
km=1000

tyme=0.25  # global time
t=[tyme,(tyme*2),(tyme*3),(tyme*4),(tyme*12),(tyme*6),(tyme*16)] # [0,1,2,3,4,5]

man=[np.NaN,np.inf] # non existent data manipulation    man[0,1]

temp=['templates.data','templates.telemetry'] # database template tables    temp[0,1]

raw=[]

def test_params():

    file_test='HM_CAT'
    test_ver=3
    test_table1=('ttest{}'.format(test_ver))
    test_table2=('ftest')
    test_table3=('dtest{}'.format(test_ver))
    test_pad='KSC Pad 1'
    test_schema='test'

    testing=[file_test,test_ver,test_table1,test_table3,test_pad,test_schema,test_table2] # test[0,1,2,3,4,5,6]

    return(testing)

def color():        # text colour vars

    # normal
    nor=        '\033[0;37m'        # cr[0]
    # text
    red=        '\033[1;31m'        # cr[1]
    pur=        '\033[1;35m'        # cr[3]
    grn=        '\033[1;32m'        # cr[6]
    cyn=        '\033[1;36m'        # cr[7]
    yel=        '\033[1;33m'        # cr[8]
    # highlight
    hyel=        '\033[1;30;43m'    # cr[2]
    hcyn=        '\033[1;30;46m'    # cr[4]
    hgrn=        '\033[1;30;42m'    # cr[5]

    color=[nor,red,hyel,pur,hcyn,hgrn,grn,cyn,yel]                              # cr[0,1,2,3,4,5,6,7,8]

    return(color)

def start():
    
    cr=color()
    test=test_params()
    CO()

    print('{} Kerbal Space Program Launch Telemetry Processing and Database Insertion'.format(nl))
    print(' Python/Postgres')
    print(' By: Marcus Dechant')

    return(cr,test)

def user():

    (cr,test)=start()

    def lv_name():

        file_raw=test[0]

        # ksp launch vehicle class based naming convention 
        # (class_mission_number)
        # where class is the family of launch vehicle, or in cases when multiple missions are grouped together as in the case of orbital construction (ex. hailmary, KOSS).
        # where mission refers to either the individual mission name ex. HM_CORE, HM_CST, Cache Alpha. Some refer to the payload while some are sequential.
        # where number is when payload named missions required more than one launch (ex. HM_Eng_4)
        # file_raw=input('{} Flight Name?                                     : '.format(nl))     # user specific flight name (name of telmu csv data)
        fn=file_raw.split("_") # fn[0,1]
        
        if str.lower(fn[0])=='cache':           # if flight is named cache (obselete) rename to Callisto                                                               
            fn[0]='Callisto'
        
        display_name=(' '.join((fn[0],fn[1])))    # reform for display
        store_name=(''.join((fn[0],fn[1])))      # reform for storage
        names=[file_raw,display_name,store_name,fn]         #names[0,1,2,3]    [3][0,1]

        return(names)

    def launch_pad():

        lpad=test[4]

        # ready pads
        # KSC Pad 1 - equatorial - Kerbal Space Center
        # DES Pad 2 - (-6) South - Dessert Airforce Base
        # WLS Pad 3 - 45 North - Woomerang Launch Site
        # lpad=input(' Launch Pad?                                      : ')                  # user defines what launch comlex and pad where used                                                                                            
        pd=lpad.split(' ')
        display_name=lpad
        store_name=(''.join((pd[0],pd[1],pd[2])))
        names=[display_name,store_name,pd]          # names[0,1,2]  [2][0,1,2]

        return(names)
    
    lvn=lv_name()
    pdn=launch_pad()

    cl=lvn[3][0]

    if(cl=='Callisto'): # Callisto preset factors
        bstc='y' 
        payc='n'
        fopt='n'
    else:
        bstc=input('{} Did the Launch Vehicle have boosters? (y/n)      : '.format(nl))                  # user defines if the launch vehicle had boosters
        payc=input(' Did the Launch Vehicle need a fairing? (y/n)     : ')                  # user defines if launch vehicle staged a payload
        fopt=input(' Did the Launch Vehicle release a payload? (y/n)  : ')                # user defines if launch vehicle staged a fairing
        
    
    options=[bstc,payc,fopt]
    opts=[]                                 # for loop for launch vehicle options boolean translation
    for o in options:                       
        if(o=='y'):                         
            opt=True                        # if user anwsered yes (y) = True
            opts.append(opt)
        else:
            opt=False                       # if user anwsered no (n) = False
            opts.append(opt)
    
    sl(t[2])

    inc=[lvn,pdn,opts,cr,test]   
    # inc[0,1,2,3,4]                user output list
    # inc[0][0,1,2,3] - [3][0,1]    lv name list
    # inc[1][0,1,2] - [2][0,1,2]    pad list
    # inc[2],inc[3],inc[4]          user questions
    # inc[5][0,1,2,3,4,5,6,7,8]     text colour list
    # inc[6][0,1,2,3,4,5]           test param list

    #print('{}{}'.format(nl,inc))

    return(inc)

def source():

    inc=user()

    csv=inc[0][0]
    folder=inc[0][3][0]

    print('{} Reading {} Telemetry CSV file. [{}\{}.csv]'.format(nl,csv,folder,csv))
    
    df=pd.read_csv(r'{}\{}.csv'.format(folder,csv))    # extracts telmetry csv to dataframe
    col=df.columns.values                                      # defines columns as col[0,1,2,3,n]
    # print('{}{}{}'.format(nl,col,nl))                        # column name print out
    name=['TIME','STG','ASL','DRG','SFVEL','ORVEL',  
          'MASS','ACC','Q','AoA','AoS','AoD',
          'Atru','PITCH','gLS','dLS','sLS','DLTV']             # shortened column names
    a=0       
    while a<len(col):                                          # update column names to shorter names
        col[a]=name[a]
        a+=1
    # print('{}{}{}'.format(nl,col,nl))                        # new column name print out

    return(col,df,inc)

def processing():

     # rate of change for stages, mass, and acceleration

    (col,df,inc)=source()
    cr=inc[3]

    print(' Processing Data...')
    sl(t[4])

    t1=['sROC','mRoC','aRoC']                       # titles list
    a=0                                             # columns iterative starting point
    b=0                                             # titles iterative starting point
    for i in col:                                   # for loop through colummns
        if (i==col[1] or i==col[6] or i==col[7]):   # if i equals 1 (stages), 6 (mass), 7 acceleration
            ser=df[col[a]]                          # based on the iteration find the equivalent column
            num=[]                                  # 
            for f in ser:                           # loop through each row in column
                num.append(f)                       # isolate metrics from each row into a list
            ic=pd.Series(num)                       # place isolated metrics into a series
            roc=ic.pct_change()                     # find the rate of change of the series    
            title=t1[b]                             # b will select the correct title from titles list
            idf=pd.DataFrame({title:roc})           # place the rate of change series into an isolated dataframe
            df=df.join(idf,how='right')             # join isolated dataframe to then original dataframe
            b+=1                                    # titles iterative value
        a+=1                                        # columns iterative value
    col=df.columns.values                           # update col
    for h in col:
        if(h==col[0]):
            df[col[0]]=df[col[0]].round(3)          # round time to 3 decimal
        if(h==col[18]):
            df[col[18]]=df[col[18]].abs()           # stage roc absolute value
    c=0
    for f in col:                                   # remove all inf and nan, replace with zero
        if(f==t1[0] or f==t1[1] or f==t1[2]):
            rate=df[col[c]]
            rate.replace([man[1],(-man[1])],man[0],inplace=True)
            rate.fillna(0,inplace=True)
        c+=1

    print('{} Complete!{}{}'.format(cr[6],cr[0],nl))

    return(col,df,inc)

def flight_telemetry():

    # data from telemetry csv to postgres database
    
    (col,df,inc)=processing()
    cr=inc[3]
    test=inc[4]

    print(' Connecting to PostgreSQL Database: {}[ksp]{}'.format(cr[7],cr[0]))
    conn=psql()
    cur=conn.cursor()
    sl(t[5])
    print('{} Connected!{}{}'.format(cr[6],cr[0],nl))
    sch=['callisto','hailmary']                         # schema list, based on LV naming conventions the LV class is the database schema
    # if(cl=='Callisto' or cl=='Cache'):          # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                               # set correct schema from list
    # if(cl=='HM' or cl=='Hailmary'):             # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                               # set correct schema from list
    # table='telemetry'                                 # flights table differs from the other as its a record of all flights in one program or project

    schema_table=("{}.{}".format(test[5],test[2]))         # set schema name for query
    
    print(' Creating Table:{} [ksp.{}]{}'.format(cr[7],schema_table,cr[0]))
    
    while(True):                                                                            # telemetry table creation
        try:
            TBL=('''CREATE TABLE {} () INHERITS ({});
                 '''.format(schema_table,temp[1]))                                         # create table command
            cur.execute(TBL)                                                                # execute sql command
            break                                                                           # break while loop
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):                                  # if table already exists
            while(True):                                        
                ovrw=input('{} Table Exists. Overwrite? (y/n):{} '.format(cr[1],cr[0]))  # ask user if they wish to overwrite
                ovry=(ovrw=='y')
                ovrx=(ovrw=='n')
                if(ovry)or(ovrx):
                    if(ovry):                                                               # user wishes to proceed
                        print(' Overwriting...')
                        sl(t[5])
                        DUP=('''ROLLBACK;
                                DROP TABLE {};
                             '''.format(schema_table,temp[1]))                            # drop table command
                        cur.execute(DUP)                                                    # execute sql command
                        break                                                               # break while loop
                    else:
                        print('{}{} Process Canceled...{}{}'.format(nl,cr[1],cr[0],nl))     # user does not want to overwritte
                        exit()                                                              # exit program
                else:
                    print(' Incorrect Input...')                                            # neither yes or no
                    continue                                                                # continue while loop
            continue                                                                        # if table exists, delete and loop            
    print('{} Complete!{}{}'.format(cr[6],cr[0],nl))
    print(' Populating table... ')
    for (i,r) in df.iterrows():                                     # iterate through rows
        a=tuple(r.values)                                           # turn each row into a tuple
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')          # sql command
        cur.execute(SQL, (a,)) 
    sl(t[6])
    print('{} Complete!{}{}'.format(cr[6],cr[0],nl))

    return(col,df,inc,cur)

def flight_info():

    # overall flight information

    cr=color()
    (col,df,inc,cur)=flight_telemetry()
    test=inc[4]

    a=0
    val=[]                                    # for loop for general flight information
    lst2=[0,2,3,5,6,17]                      # columns where either the final or inital value are flight information
    while a<len(col):                       # while loop to iterate through columns
        for i in lst2:                     # for loop to iterate through flight information list
            if(a==i):
                if(a==6):                     # if while loop iteration equals flight info list value
                    val.append(df[col[i]].iloc[1]) 
                    val.append(df[col[i]].iloc[-1])
                else:
                    val.append(df[col[i]].iloc[-1] )
        a+=1

    fl_in={'name'       :[inc[0][2]],
           'pad'        :[inc[1][1]],
           'time'       :[val[0]],
           'altitude'   :[val[1]],
           'range'      :[val[2]],
           'speed'      :[val[3]],
           'lv_mass'    :[val[4]],
           'pay_mass'   :[val[5]],
           'delta_v'    :[val[6]],
           'bst_q'      :[inc[2][0]],
           'pay_q'      :[inc[2][1]],
           'fst_q'      :[inc[2][2]]}

    fl=pd.DataFrame(fl_in)
    fol=fl.columns.values

    data=[fl[fol[0]][0],fl[fol[1]][0],fl[fol[2]][0],fl[fol[3]][0],
          fl[fol[4]][0],fl[fol[5]][0],fl[fol[6]][0],fl[fol[7]][0],
          fl[fol[8]][0],inc[2][0],inc[2][1],inc[2][2]]
    
    put=tuple(data)
          
    # sch=['callisto','hailmary']                       # schema list, based on LV naming conventions the LV class is the database schema
    # if(cl=='Callisto' or cl=='Cache'):          # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                               # set correct schema from list
    # if(cl=='HM' or cl=='Hailmary'):             # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                               # set correct schema from list
    # table='flights'                                   # flights table differs from the other as its a record of all flights in one program or project
        
    schema_table=('{}.{}'.format(test[5],test[6]))         #set schema name for query)
    SQL=(f'''INSERT INTO {schema_table} VALUES %s ON CONFLICT DO NOTHING;''')          # sql command
    cur.execute(SQL,(put,))
    
    time=pd.to_timedelta(data[2],'sec')                     # translating raw time (seconds) into clock time for readability
    mm=time.components.minutes                           # split minutes off
    ss=time.components.seconds                             # spilt seconds off
    ms=time.components.milliseconds                    # split milliseconds off
    time=('{:.2f}:{:.2f}.{:.3f}'.format(mm,ss,ms))                      # reform

    print('{} Database: [ksp.{}] {}{}'.format(cr[5],schema_table,cr[0],nl))
    print('{} Flight Information {}{}'.format(cr[2],cr[0],nl))
    print('{}  - Flight Name:              {} {}'.format(cr[1],inc[0][1],cr[0]))
    print('  - Launch Complex/Pad:       {}'.format(inc[1][0]))
    print('{}  - Flight Time:              {} {}'.format(cr[6],time,cr[0]))
    print('  - Flight Time (Raw):        {:.3f} s'.format(data[2]))
    print('{}  - Final Altitude:           {:.3f} km{}'.format(cr[7],(data[3]/km),cr[0]))
    print('  - Downrange:                {:.3f} km'.format(data[4]/km))
    print('  - Orbital Velocity:         {:.4f} km/s'.format(data[5]/km))    
    print('{}  - Launch Vehicle Mass:      {:.3f} t{}'.format(cr[3],data[6],cr[0]))
    print('  - Payload Mass:             {:.3f} t'.format(data[7]))
    print('  - Launch Vehicle DeltaV:    {:.0f} m/s{}'.format(data[8],nl))
    
    raw.append(data)

    exit()
          
    

    flin={'name'    :[ict],
          'pad'     :[idp],
          'time'    :[d[0]],
          'altitude':[d[1]],
          'range'   :[d[2]],
          'speed'   :[d[3]],
          'lv_mass' :[d[4]],
          'pay_mass':[d[5]],
          'delta_v' :[d[6]],
          'bst_q'   :[opts[0]],
          'pay_q'   :[opts[1]],
          'fair_q'  :[opts[2]]}             # flight info dataframe
    
    fl=pd.DataFrame(flin)
    fol=fl.columns.values                   #flight info columns

    data=[file,
          pad,
          fl[fol[2]][0],
          fl[fol[3]][0],
          fl[fol[4]][0],
          fl[fol[5]][0],
          fl[fol[6]][0],
          fl[fol[7]][0],
          fl[fol[8]][0],
          opts[0],
          opts[1],
          opts[2]]                                      #flight info data
    
    # sch=['callisto','hailmary']                       # schema list, based on LV naming conventions the LV class is the database schema
    # if(cl=='Callisto' or cl=='Cache'):          # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                               # set correct schema from list
    # if(cl=='HM' or cl=='Hailmary'):             # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                               # set correct schema from list
    schema='test'
    table='ftest'
    # table='flights'                                   # flights table differs from the other as its a record of all flights in one program or project
    schema_table=('{}.{}'.format(schema,table))         #set schema name for query

    SQL=('''INSERT INTO 
            {} 
            ({},{},{},{},{},{},{},{},{},{},{},{})
            VALUES 
             ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
            ON CONFLICT DO NOTHING;
            '''.format(schema_table,
            fol[0],fol[1],fol[2],fol[3],fol[4],
            fol[5],fol[6],fol[7],fol[8],fol[9],
            fol[10],fol[11],
            data[0],data[1],data[2],data[3],
            data[4],data[5],data[6],data[7],
            data[8],data[9],data[10],data[11]))             # sql insert for postgres
    cur.execute(SQL)                                        # execute postgres insert
     
    time=pd.to_timedelta(data[2],'sec')                     # translating raw time (seconds) into clock time for readability
    mm=time.components.minutes                              # split minutes off
    ss=time.components.seconds                              # spilt seconds off
    ms=time.components.milliseconds                         # split milliseconds off
    time=('{}:{}.{}'.format(mm,ss,ms))                      # reform

    print('{} Database: [ksp.{}] {}{}'.format(cr[5],schema_table,cr[0],nl))
    sl(t1) 
    print('{} Flight Information {}{}'.format(cr[2],cr[0],nl))
    sl(t1)
    print('{}  - Flight Name:              {} {}'.format(cr[1],flnm,cr[0]))
    sl(t2)
    print('  - Launch Complex/Pad:       {}'.format(data[1]))
    sl(t2)
    print('{}  - Flight Time:              {} {}'.format(cr[6],time,cr[0]))
    sl(t2)
    print('  - Flight Time (Raw):        {:.3f} s'.format(data[2]))
    sl(t2)
    print('{}  - Final Altitude:           {:.3f} km{}'.format(cr[7],(data[3]/km),cr[0]))
    sl(t2)
    print('  - Downrange:                {:.3f} km'.format(data[4]/km))
    sl(t2)
    print('  - Orbital Velocity:         {:.4f} km/s'.format(data[5]/km))    
    sl(t2)
    print('{}  - Launch Vehicle Mass:      {:.3f} t{}'.format(cr[3],data[6],cr[0]))
    sl(t2)
    print('  - Payload Mass:             {:.3f} t'.format(data[7]))
    sl(t2)
    print('  - Launch Vehicle DeltaV:    {:.0f} m/s{}'.format(data[8],nl))
    sl(t3)
    # print('Raw Data: {}{}'.format(data,nl))
    # print('SQL: {}{}'.format(SQL,nl))
    # print('DataFrame{}{}{}'.format(nl,fl,nl))

    raw.append(data)
    sl(t4)
    
    return(col,df,fol,fl,raw,cur)

def flight_data():

    cr=color()
    (col,df,fol,fl,raw,cur)=flight_info()

    stgs=df[col[18]]

    pst=['milestone','special','altitude','range','speed','delta','time','row']     # milestones data positions columns 
    pos=[(0,2),(0,3),(0,5),(0,17),(0,0),(0)]                                        # milestone data positions
    no_data=[None,nan,nan,nan,nan,nan,nan,-1]                                       # no data, data var fill
    options=[fl[fol[9]].iloc[0],fl[fol[10]].iloc[0],fl[fol[11]].iloc[0]]            # user inputs
    mile=['maxq','thup','beco','meco','fair','seco','oib','oibeco','pay','eof']     # milestone names
    
    stage=[]                                                                        # stages roc column refinded
    for stg in stgs:                                                                # non zero number list                
        if(stg!=0):                                                                 # remove zero values
            stage.append(stg)

    sch=['callisto','hailmary']                     # schema list, based on LV naming conventions the LV class is the database schema
    # if(cl=='Callisto' or cl=='Cache'):        # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                             # set correct schema from list
    # if(cl=='HM' or cl=='Hailmary'):           # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                             # set correct schema from list
    # table='data'                                    # flights table differs from the other as its a record of all flights in one program or project
    schema='test'

    schema_table=('{}.{}'.format(schema,table2))     #set schema name for query
    prime_id=pst[0]                                 # primary key columnn (0)

    print('{} Creating Table: [ksp.{}] {}{}'.format(cr[5],schema_table,cr[0],nl))
    sl(t3)

    while(True):                                                                            # telemetry table creation
        try:
            TBL=('''CREATE TABLE {} () INHERITS ({});'''.format(schema_table,data_temp))    # create table command
            
            cur.execute(TBL)                                                                # execute sql command
            break                                                                           # break while loop
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):                                  # if table already exists
            while(True):                                        
                ovrw=input('{} Table Exists. Overwrite? (y/n):{} '.format(cr[1],cr[0],nl))  # ask user if they wish to overwrite
                sl(t2)
                ovry=(ovrw=='y')
                ovrx=(ovrw=='n')
                if(ovry)or(ovrx):
                    if(ovry):                                                               # user wishes to proceed
                        print('{}{} Overwriting...{}{}'.format(nl,cr[1],cr[0],nl))
                        sl(t2)
                        DUP=('''ROLLBACK;
                                DROP TABLE {};
                             '''.format(schema_table))                            # drop table command
                        cur.execute(DUP)                                                    # execute sql command
                        break                                                               # break while loop
                    else:
                        print('{}{} Process Canceled...{}{}'.format(nl,cr[1],cr[0],nl))     # user does not want to overwritte
                        exit()                                                              # exit program
                else:
                    print(' Incorrect Input...')                                            # neither yes or no
                    continue                                                                # continue while loop
            continue

    print('{} Flight Milestones {}{}'.format(cr[2],cr[0],nl))
    sl(t3)

    def maxq():
        
        Q=df[col[8]]            # Q column
        maxQ=max(Q)             # max value in column
        val=df[Q==maxQ]         # generate dataframe for value from Q dataframe
        
        # using generated dataframe find relevant values
        alt=val.iloc[pos[0]]    
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        # data list
        data=[mile[0],maxQ,alt,rng,spd,dtv,rwt,row]

        # sql insert
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        # read out
        print('{} Maximum Dynamic Pressure (MAXQ) {}{}'.format(cr[1],cr[0],nl))
        sl(t2)
        print('{}  - Max Q:        {:.2f} Pa {}'.format(cr[3],data[1],cr[0]))
        sl(t1)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t1)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))
        sl(t1)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t1)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t1)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t1)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t1)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))
        
        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)
    
    def thup():
        
        time=df[col[0]]
        rate=df[col[20]]

        go=[]
        for i in time:
            if(i>35)&(i<50):            # throttle up is between 35 and 50 seconds into flight shortly after max Q
                go.append(i)
        try:
            up=df[time.isin(go)][rate]  # find the equivelant value in the rate column
            th=max(up)                  # will be the highest acc roc value at that timne
            val=df[rate==th]            # generate dataframe for value from rate dataframe

            # using generated dataframe find relevant values
            alt=val.iloc[pos[0]]    
            rng=val.iloc[pos[1]]
            spd=val.iloc[pos[2]]
            dtv=val.iloc[pos[3]]
            rwt=val.iloc[pos[4]]
            row=val.index[pos[5]]
            
            # clock time generation
            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

            # data list
            data=[mile[1],nan,alt,rng,spd,dtv,rwt,row]

            # read out
            print('{} Throttle Up (THUP) {}{}'.format(cr[1],cr[0],nl))
            sl(t2)
            print('{}  - Alt:          {:.3f} km{}'.format(cr[7],(data[2]/km),cr[0]))
            sl(t1)
            print('  - Range:        {:.3f} km'.format(data[3]/km))
            sl(t1)
            print('  - Speed:        {:.3f} m/s'.format(data[4]))
            sl(t1)
            print('  - Delta V Used: {:.3f} m/s'.format(data[5]))
            sl(t1)
            print('  - Time (Raw):   {:.3f} s'.format(data[6]))
            sl(t1)
            print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
            sl(t1)
            print('  - Row:          {:.0f}{}'.format(data[7],nl))

        # no throttle up found
        except(KeyError):
            no_data[0]=mile[1]
            data=no_data
            print('{}! No Throttle Up Found !{}{}'.format(cr[1],cr[0],nl))

        # sql insert        imporve
        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}');
                    '''.format(schema_table,
                               pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                               data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        # raw data read out
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))

        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)

    def beco():

        # boosers equipped
        if(options[0]==True):

            stg=stage[0]
            val=df[stgs==stg]

            # using generated dataframe find relevant values
            alt=val.iloc[pos[0]]    
            rng=val.iloc[pos[1]]
            spd=val.iloc[pos[2]]
            dtv=val.iloc[pos[3]]
            rwt=val.iloc[pos[4]]
            row=val.index[pos[5]]
            
            # clock time generation
            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

            # data list
            data=[mile[2],nan,alt,rng,spd,dtv,rwt,row]

            # read out
            print('{} Booster Engine Cut Off (BECO) {}{}'.format(cr[1],cr[0],nl))
            sl(t2)
            print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
            sl(t1)
            print('  - Range:        {:.3f}    km'.format(data[3]/km))
            sl(t1)
            print('  - Speed:        {:.3f}  m/s'.format(data[4]))
            sl(t1)
            print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
            sl(t1)
            print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
            sl(t1)
            print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
            sl(t1)
            print('  - Row:          {:.0f}{}'.format(data[7],nl))
        
        # no boosters equipped
        else:
            no_data[0]=mile[2]
            data=no_data
            print('{}! Boosters Not Equipped !{}{}'.format(cr[1],cr[0],nl))
            
        # sql insert        imporve
        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}');
                    '''.format(schema_table,
                               pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                               data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        # raw data read out
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))

        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)

    def meco():

        if(options[0]==True):
            stg=stage[1]
        else:
            stg=stage[0]

        val=df[stgs==stg]

        # using generated dataframe find relevant values
        alt=val.iloc[pos[0]]    
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        # data list
        data=[mile[3],nan,alt,rng,spd,dtv,rwt,row]

        # sql insert
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        # read out
        print('{} Main Engine Cut Off (MECO) {}{}'.format(cr[1],cr[0],nl))
        sl(t2)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t1)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))
        sl(t1)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t1)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t1)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t1)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t1)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))
        
        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)
    
    def fair():

        # no fairinf on test file

        alt=df[col[2]]
        rate=df[col[19]]

        if(options[1]==True):

            zone=[]
            for i in alt:
                if(i>68000)&(i<70000):
                    zone.append(i)

            zone_rate=df[alt.isin(zone)][col[19]]
            moment=min(zone_rate)
            val=df[rate==moment]
            
            # using generated dataframe find relevant values
            alt=val.iloc[pos[0]]    
            rng=val.iloc[pos[1]]
            spd=val.iloc[pos[2]]
            dtv=val.iloc[pos[3]]
            rwt=val.iloc[pos[4]]
            row=val.index[pos[5]]
            
            # clock time generation
            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

            # data list
            data=[mile[1],nan,alt,rng,spd,dtv,rwt,row]

            # read out
            print('{} Fairing Separation (FAIR) {}{}'.format(cr[1],cr[0],nl))
            sl(t2)
            print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
            sl(t1)
            print('  - Range:        {:.3f}    km'.format(data[3]/km))
            sl(t1)
            print('  - Speed:        {:.3f}  m/s'.format(data[4]))
            sl(t1)
            print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
            sl(t1)
            print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
            sl(t1)
            print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
            sl(t1)
            print('  - Row:          {:.0f}{}'.format(data[7],nl))
        
        else:

            no_data[0]=mile[4]
            data=no_data
            print('{}! No Fairing Required !{}{}'.format(cr[1],cr[0],nl))

        # sql insert        imporve
        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}');
                    '''.format(schema_table,
                               pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                               data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        # raw data read out
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))

        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)

    def seco():
        
        v=df[col[5]]
        maxv=max(v)
        val=df[v==maxv]

        # using generated dataframe find relevant values
        alt=val.iloc[pos[0]]    
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        # data list
        data=[mile[5],nan,alt,rng,spd,dtv,rwt,row]

        # sql insert
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        # read out
        print('{} Second Engine Cut Off (SECO) {}{}'.format(cr[1],cr[0],nl))
        sl(t2)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t1)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))
        sl(t1)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t1)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t1)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t1)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t1)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))
        
        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)
    
    def oib():

        change=col[20]
        rate=df[change]
        r=rate.index
        
        # improve
        a=0

        b=(-1)
        c=r[b]
        d=0.25
        e=int(c-(c*d))

        x=0
        y=10

        lst=[]
        for i in rate:
            q=r[b]
            s=abs(i)
            if(q>e)&(i!=x)&(s>y):
                lst.append(i)
            a+=1
        burn=df[rate.isin(lst)][change]
        imp=max(burn)
        val=df[rate==imp]
        
        # using generated dataframe find relevant values
        alt=val.iloc[pos[0]]    
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        # data list
        data=[mile[6],nan,alt,rng,spd,dtv,rwt,row]

        # sql insert
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        # read out
        print('{} Orbital Insertion Burn (OIB) {}{}'.format(cr[1],cr[0],nl))
        sl(t2)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t1)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))
        sl(t1)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t1)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t1)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t1)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t1)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))
        
        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)
    
    def oibeco():

        change=col[20]
        rate=df[change]
        r=rate.index

        a=0

        b=(-1)
        c=r[b]
        d=0.25
        e=int(c-(c*d))

        x=0
        y=0.01
        z=0.9

        lst=[]
        for i in rate:
            q=r[a]
            s=abs(i)
            if(q>e)&(i!=x)&(s>y)&(s<z):
                lst.append(i)
            a+=1
        
        rt=df[rate.isin(lst)][change]
        rt=rt.drop(rt.index[-1:],axis=0)
        if (rt.index[0])==(raw[6][7]):
            print(True)
            rt=rt.drop(rt.index[:0],axis=0)
        cut=rt.iloc[-1]
        val=df[rate==cut]
        
        # using generated dataframe find relevant values
        alt=val.iloc[pos[0]]    
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        # data list
        data=[mile[7],nan,alt,rng,spd,dtv,rwt,row]

        # sql insert
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        # read out
        print('{} OIB Engine Cut Off (OIBECO) {}{}'.format(cr[1],cr[0],nl))
        sl(t2)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t1)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))
        sl(t1)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t1)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t1)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t1)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t1)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))
        
        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)

    def pay():

         # no payload release on test file
        
        cha=col[19]
        rate=df[cha]
        r=rate.index

        if(options[2]==True):
            a=0

            b=(-1)
            c=r[b]
            d=0.25
            e=int(c-(c*d))
            x=0
            y=0.0001

            lst=[]
            for i in rate:
                q=r[a]
                s=abs(i)
                if(q>e)&(i!=x)&(s>y):
                    lst.append(i)
                    
                a+=1
            rt=df[rate.isin(lst)][cha]
            moment=min(rt)
            val=df[rate==moment]
            print(val)

            # using generated dataframe find relevant values
            alt=val.iloc[pos[0]]    
            rng=val.iloc[pos[1]]
            spd=val.iloc[pos[2]]
            dtv=val.iloc[pos[3]]
            rwt=val.iloc[pos[4]]
            row=val.index[pos[5]]
            
            # clock time generation
            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

            # data list
            data=[mile[1],nan,alt,rng,spd,dtv,rwt,row]

            # read out
            print('{} Payload Release (PAYR) {}{}'.format(cr[1],cr[0],nl))
            sl(t2)
            print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
            sl(t1)
            print('  - Range:        {:.3f}    km'.format(data[3]/km))
            sl(t1)
            print('  - Speed:        {:.3f}  m/s'.format(data[4]))
            sl(t1)
            print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
            sl(t1)
            print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
            sl(t1)
            print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
            sl(t1)
            print('  - Row:          {:.0f}{}'.format(data[7],nl))
            # silent - callisto payload is technically its second stage
        else:
            no_data[0]=mile[9]
            data=no_data
            print('{}! No Payload Released !{}{}'.format(cr[1],cr[0],nl))

         # sql insert        imporve
        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}');
                    '''.format(schema_table,
                               pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                               data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        # raw data read out
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))

        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)
    
    def fin():

        # end of file

        time=df[col[0]]
        
        last=time.iloc[-1]
        val=df[time==last]

        # using generated dataframe find relevant values
        alt=val.iloc[pos[0]]    
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        # data list
        data=[mile[9],nan,alt,rng,spd,dtv,rwt,row]

        # sql insert
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        # read out
        print('{} End of Flight (EoF) {}{}'.format(cr[1],cr[0],nl))
        sl(t2)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t1)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))
        sl(t1)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t1)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t1)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t1)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t1)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        # sl(t2)
        # print('Raw Data: {}{}'.format(data,nl))
        
        # append to overall raw data list
        raw.append(data)
        sl(t4)

        return(data)        

    maxq()      #
    thup()      #
    beco()      #
    meco()      #
    fair()      #    
    seco()      #
    oib()       #
    oibeco()    #
    pay()       #
    fin()       #

    return(raw)

def run():
    flight_data()
    conn.commit()
    conn.close()
    # print(raw)

if __name__=="__main__":
    run()
from ColorOut import CO
from doNotGit.psqlConn import psql
import pandas as pd
import numpy as np
from time import sleep as sl
from psycopg2 import errors, errorcodes

# data processing for ksp flight telemetry
# by: marcus dechant

CO()

conn=psql()
data_temp='templates.data'
tele_temp='templates.telemetry'

nan=np.NaN
inf=np.inf

nl='\n'
km=1000
t=0.25
t1=t
t2=t*2
t3=t*3
t4=t*4
t5=3

raw=[]

def color():

    # normal
    nor=        '\033[0;37m'        # cr[0]
    # text
    red=        '\033[1;31m'        # cr[1]
    pur=        '\033[1;35m'        # cr[3]
    grn=        '\033[1;32m'        # cr[6]
    cyn=        '\033[1;36m'        # cr[7]
    # highlight
    hyel=        '\033[1;30;43m'    # cr[2]
    hcyn=        '\033[1;30;46m'    # cr[4]
    hgrn=        '\033[1;30;42m'    # cr[5]

    color=[nor,red,hyel,pur,hcyn,hgrn,grn,cyn]

    return(color)

def user():

    # ksp launch vehicle class based naming convention 
    # (class_mission_number)
    # where class is the family of launch vehicle, or in cases when multiple missions are grouped together as in the case of orbital construction (ex. hailmary, KOSS).
    # where mission refers to either the individual mission name ex. HM_CORE, HM_CST, Cache Alpha. Some refer to the payload while some are sequential.
    # where number is when payload named missions required more than one launch (ex. HM_Eng_4)
    file='Cache_Alpha'                                                                   # faux input
    # file=input('{}Flight Name?                                     : '.format(nl))     # user specific flight name (name of telmu csv data)
    fn=file.split("_")
    if str.lower(fn[0])=='cache':                                                               
        fn[0]='Callisto'
    flnm=('{} {}'.format(fn[0],fn[1]))
    
    # KSC Pad 1 - equatorial - Kerbal Space Center
    # DES Pad 2 - (-6) South - Dessert Airforce Base
    # WLS Pad 3 - 45 North - Woomerang Launch Site
    lpad='KSC Pad 1'                                                                     # faux input
    # lpad=input('Launch Pad?                                      : ')                  # user defines what launch comlex and pad where used                                                                                            

    bstc='y'                                                                             # faux input
    # bstc=input('Did the Launch Vehicle have Boosters? (y/n)      : ')                  # user defines if the launch vehicle had boosters

    payc='n'                                                                             # faux input
    # payc=input('Did the Launch Vehicle release a Payload? (y/n)  : ')                  # user defines if launch vehicle staged a payload

    fopt='n'                                                                             # faux input
    # fopt=input('Did the Launch Vehicle release a Payload? (y/n)  : ')                  # user defines if launch vehicle staged a fairing

    incl=[file,fn,lpad,bstc,payc,fopt,flnm]

    # print(nl)
    sl(t4)

    return(incl)

def source():

    incl=user()

    df=pd.read_csv(r'{}\{}.csv'.format(incl[1][0],incl[0]))    # extracts telmetry csv to dataframe
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

    return(col,df,incl)

def roc():

     # rate of change for stages, mass, and acceleration

    (col,df,incl)=source()

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

    c=0
    for f in col:                                   # remove all inf and nan, replace with zero
        if(f==t1[0] or f==t1[1] or f==t1[2]):
            rate=df[col[c]]
            rate.replace([inf,(-inf)],nan,inplace=True)
            rate.fillna(0,inplace=True)
        c+=1
    
    return(col,df,incl)

def flight_telemetry():

    # data from telemetry csv to postgres database
    
    cr=color()
    (col,df,user)=roc()
    fn=user[1]
    flnm=user[6]
    
    print('{}{} {} Launch Telemetry {}{}'.format(nl,cr[4],flnm,cr[0],nl))
    sl(t4)
    print('{} Connecting to PostgreSQL Database: [ksp] {}{}'.format(cr[2],cr[0],nl))
    sl(t4)
    
    cur=conn.cursor()
    sch=['callisto','hailmary']                         # schema list, based on LV naming conventions the LV class is the database schema
    # if(fn[0]=='Callisto' or fn[0]=='Cache'):          # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                               # set correct schema from list
    # if(fn[0]=='HM' or fn[0]=='Hailmary'):             # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                               # set correct schema from list
    schema='test'
    table='test4'
    # table='telemetry'                                 # flights table differs from the other as its a record of all flights in one program or project
    schema_table=("{}.{}".format(schema,table))         # set schema name for query
    prime_id=col[0]                                     # primary key
    
    while(True):                                                                            # telemetry table creation
        try:
            TBL=('''CREATE TABLE {} () INHERITS ({});
                 ALTER TABLE {} ADD PRIMARY KEY ({});
                 '''.format(schema_table,tele_temp,
                            schema_table,prime_id))                                         # create table command
            cur.execute(TBL)                                                                # execute sql command
            sl(t3)
            print('{} Creating Table: [ksp.{}] {}{}'.format(cr[5],schema_table,cr[0],nl))
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
                             '''.format(schema_table,tele_temp))                            # drop table command
                        cur.execute(DUP)                                                    # execute sql command
                        break                                                               # break while loop
                    else:
                        print('{}{} Process Canceled...{}{}'.format(nl,cr[1],cr[0],nl))     # user does not want to overwritte
                        exit()                                                              # exit program
                else:
                    print(' Incorrect Input...')                                            # neither yes or no
                    continue                                                                # continue while loop
            continue                                                                        # if table exists, delete and loop            

    print('{} Writing to table... {}{}'.format(cr[7],cr[0],nl))     # populate table
    for (i,r) in df.iterrows():                                     # iterate through rows
        a=tuple(r.values)                                           # turn each row into a tuple
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')          # sql command
        cur.execute(SQL, (a,))                                      # populate table
    sl(t5)
    print('{} Complete! {}{}'.format(cr[6],cr[0],nl))

    return(col,df,user)

def flight_info():
    cr=color()
    (col,df,user)=flight_telemetry()
    file=user[0]
    pad=user[2]
    flnm=user[6]

    a=(-1)                  
    b=1
    c=0
    d=[]                                    # for loop for general flight information
    lst=[0,2,3,5,6,17]                      # columns where either the final or inital value are flight information
    while c<len(col):                       # while loop to iterate through columns
        for itm in lst:                     # for loop to iterate through flight information list
            if(c==itm):                     # if while loop iteration equals flight info list value
                if c==6:                    # we need both max and min of mass
                    f=col[itm]
                    v=df[f].iloc[b]         # min or first value is the launch vehicles initial mass at launch
                    d.append(v) 
                    g=col[itm]              
                    w=df[g].iloc[a]         # max of last value is the "payload mass" or final vehicle mass
                    d.append(w)
                else:
                    f=col[itm]
                    v=df[f].iloc[a]         # the rest of the columns are last values, representing final readings
                    d.append(v)
        c+=1
           
    opts=[]                                 # for loop for launch vehicle options boolean translation
    options=[user[3],user[4],user[5]]       # user inputted y/n strings
    for o in options:                       
        if(o=='y'):                         
            opt=True                        # if user anwsered yes (y) = True
            opts.append(opt)
        else:
            opt=False                       # if user anwsered no (n) = False
            opts.append(opt)

    flin={'name'    :[file],
          'pad'     :[pad],
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
    
    cur=conn.cursor()                                   # postgres cursor
    
    # sch=['callisto','hailmary']                       # schema list, based on LV naming conventions the LV class is the database schema
    # if(fn[0]=='Callisto' or fn[0]=='Cache'):          # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                               # set correct schema from list
    # if(fn[0]=='HM' or fn[0]=='Hailmary'):             # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                               # set correct schema from list
    schema='test'
    table='test2'
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

    print('{} Flight Information {}{}'.format(cr[2],cr[0],nl))
    sl(t1)
    print('{} Database: ksp.{} {}{}'.format(cr[5],schema_table,cr[0],nl))
    sl(t1)                       
    print('{}  - Flight Name:              {} {}'.format(cr[1],flnm,cr[0]))
    sl(t2)
    print('  - Launch Complex/Pad:       {}'.format(data[1]))
    sl(t2)
    print('{}  - Flight Time:              {} {}'.format(cr[6],time,cr[0]))
    sl(t2)
    print('  - Flight Time (Raw):        {:.3f}  s'.format(data[2]))
    sl(t2)
    print('{}  - Final Altitude:           {:.3f}   km{}'.format(cr[7],(data[3]/km),cr[0]))
    sl(t2)
    print('  - Downrange:                {:.3f}  km'.format(data[4]/km))
    sl(t2)
    print('  - Orbital Velocity:         {:.4f}    km/s'.format(data[5]/km))    
    sl(t2)
    print('{}  - Launch Vehicle Mass:      {:.3f}   t{}'.format(cr[3],data[6],cr[0]))
    sl(t2)
    print('  - Payload Mass:             {:.3f}    t'.format(data[7]))
    sl(t2)
    print('  - Launch Vehicle DeltaV:    {:.0f}      m/s{}'.format(data[8],nl))
    sl(t3)
    # print('Raw Data: {}{}'.format(data,nl))
    # print('SQL: {}{}'.format(SQL,nl))
    # print('DataFrame{}{}{}'.format(nl,fl,nl))

    raw.append(data)
    sl(t4)
    
    return(col,df,
           fol,fl,
           raw,conn,
           flnm)

def flight_data():

    cr=color()
    (col,df,
     fol,fl,
     raw,conn,
     flnm)=flight_info()

    pos=[(0,2),(0,3),(0,5),
         (0,17),(0,0),(0)]
    
    pst=['milestone','special','altitude','range',
          'speed','delta','time','row']
    
    no_data=[None,nan,nan,nan,
             nan,nan,nan,-1]
    
    options=[fl[fol[9]].iloc[0],
             fl[fol[10]].iloc[0],
             fl[fol[11]].iloc[0]]
    
    stgs=df[col[18]]
    stage=[]
    for stg in stgs:
        if(stg!=0):
            stage.append(stg)

    cur=conn.cursor()

    sch=['callisto','hailmary']                     # schema list, based on LV naming conventions the LV class is the database schema
    # if(fn[0]=='Callisto' or fn[0]=='Cache'):        # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                             # set correct schema from list
    # if(fn[0]=='HM' or fn[0]=='Hailmary'):           # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                             # set correct schema from list
    schema='test'                                   # faux schema name
    
    # table='data'                                 # flights table differs from the other as its a record of all flights in one program or project
    table='test3'                                   # faux test name

    schema_table=('{}.{}'.format(schema,table))     #set schema name for query
    
    prime_id=pst[0]

    while(True):
        try:
            TBL=('''CREATE TABLE {} () INHERITS ({});
                    ALTER TABLE {} ADD PRIMARY KEY ({});
                '''.format(schema_table,data_temp,
                           schema_table, prime_id))
            cur.execute(TBL)
            break
        except(KeyboardInterrupt):
            break
        except:
            DEL=('''ROLLBACK;
                    DROP TABLE {};
                '''.format(schema_table,data_temp))
            cur.execute(DEL)
            continue

    print('{} Flight Milestones {}{}'.format(cr[2],cr[0],nl))
    sl(t1)
    print('{} Database: ksp.{} {}{}'.format(cr[5],schema_table,cr[0],nl))
    sl(t1)

    def maxq():
        
        Q=df[col[8]]
        maxQ=max(Q)
        val=df[Q==maxQ]
           
        alt=val.iloc[pos[0]]
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

        mile='maxq'
        
        data=[mile,
              maxQ,
              alt,
              rng,
              spd,
              dtv,
              rwt,
              row]

        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}');
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],
                           pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],
                           data[4],data[5],data[6],data[7]))              # sql insert for postgres
        cur.execute(INS)

        print('{} Maximum Dynamic Pressure (MAXQ) {}{}'.format(cr[1],cr[0],nl))
        sl(t3)
        print('{}  - Max Q:        {:.2f} Pa {}'.format(cr[3],data[1],cr[0]))  # max dynamic pressure in Pa
        sl(t2)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))            # altitude in km
        sl(t2)
        print('  - Range:        {:.3f}    km'.format(data[3]/km))            # downrange in km
        sl(t2)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))                # orbital speed in m/s
        sl(t2)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))                # orbital speed in m/s
        sl(t2)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))                 # raw time in seconds
        sl(t2)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))                        # formatted time
        sl(t2)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))                # row in dataframe
        sl(t3)
        print('Raw Data: {}{}'.format(data,nl))

        raw.append(data)
        sl(t4)
        return(data)
    
    def thup():
        
        time=df[col[0]]
        rate=df[col[20]]

        a=35
        b=50
        go=[]
        for c in time:
            if(c>a)&(c<b):
                go.append(c)
        try:
            up=df[time.isin(go)][rate]
            th=max(up)
            val=df[rate==th]

            alt=val.iloc[pos[0]]
            rng=val.iloc[pos[1]]
            spd=val.iloc[pos[2]]
            dtv=val.iloc[pos[3]]
            rwt=val.iloc[pos[4]]
            row=val.index[pos[5]]

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

            mile='thup'
            
            data=[mile,
                  nan,
                  alt,
                  rng,
                  spd,
                  dtv,
                  rwt,
                  row]
            
            print('{} Throttle Up (GO) {}{}'.format(cr[1],cr[0],nl))
            sl(t3)
            print('  - Alt:          {:.3f}    km'.format(data[2]/km))            # altitude in km
            sl(t2)
            print('  - Range:        {:.3f}    km'.format(data[3]/km))            # downrange in km
            sl(t2)
            print('  - Speed:        {:.3f}  m/s'.format(data[4]))                # orbital speed in m/s
            sl(t2)
            print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))                # orbital speed in m/s
            sl(t2)
            print('  - Time (Raw):   {:.3f}   s'.format(data[6]))                 # raw time in seconds
            sl(t2)
            print('  - Time:         {}'.format(time))                        # formatted time
            sl(t2)
            print('  - Row:          {:.0f}{}'.format(data[7],nl))                # row in dataframe
            sl(t3)
            print('Raw Data: {}{}'.format(data,nl))

        except(KeyError):
            no_data[0]='thup'
            data=no_data
            print('{}! No Throttle Up Found !{}{}'.format(cr[1],cr[0],nl))
            # sl(t3)
            # print('Raw Data: {}{}'.format(data,nl))
            
        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}')
                    ON CONFLICT DO NOTHING;
                    '''.format(schema_table,
                               pst[0],pst[1],pst[2],pst[3],
                               pst[4],pst[5],pst[6],pst[7],
                               data[0],data[1],data[2],data[3],
                               data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        raw.append(data)
        sl(t4)

        return(data)

    def beco():

        if(options[0]==True):

            stg=stage[0]
            val=df[stgs==stg]

            alt=val.iloc[pos[0]]
            rng=val.iloc[pos[1]]
            spd=val.iloc[pos[2]]
            dtv=val.iloc[pos[3]]
            rwt=val.iloc[pos[4]]
            row=val.index[pos[5]]

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

            mile='beco'

            data=[mile,
                  nan,
                  alt,
                  rng,
                  spd,
                  dtv,
                  rwt,
                  row]
            
            print('{} Booster Engine Cut Off (BECO) {}{}'.format(cr[1],cr[0],nl))
            sl(t3)
            print('{}  - Alt:          {:.3f}     km{}'.format(cr[7],(data[2]/km),cr[0]))
            sl(t2)
            print('  - Range:        {:.3f}     km'.format(data[3]/km))
            sl(t2)
            print('  - Speed:        {:.3f}   m/s'.format(data[4]))
            sl(t2)
            print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
            sl(t2)
            print('  - Time (Raw):   {:.3f}    s'.format(data[6]))
            sl(t2)
            print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
            sl(t2)
            print('  - Row:          {:.0f}{}'.format(data[7],nl))
            sl(t3)
            print('Raw Data: {}{}'.format(data,nl))
        
        else:

            no_data[0]='beco'
            data=no_data
            print('{}! Boosters Not Equipped !{}{}'.format(cr[1],cr[0],nl))
            sl(t3)
            print('Raw Data: {}{}'.format(data,nl))
            
        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}')
                ON CONFLICT DO NOTHING;
                '''.format(schema_table,
                            pst[0],pst[1],pst[2],pst[3],
                            pst[4],pst[5],pst[6],pst[7],
                            data[0],data[1],data[2],data[3],
                            data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        raw.append(data)
        sl(t4)

        return(data)

    def meco():

        if(options[0]==True):
            stg=stage[1]
        else:
            stg=stage[0]

        val=df[stgs==stg]

        alt=val.iloc[pos[0]]
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]

        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

        mile='meco'

        data=[mile,
                nan,
                alt,
                rng,
                spd,
                dtv,
                rwt,
                row]
        
        print('{} Main Engine Cut Off (MECO) {}{}'.format(cr[1],cr[0],nl))
        sl(t3)
        print('{}  - Alt:          {:.3f}     km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t2)
        print('  - Range:        {:.3f}     km'.format(data[3]/km))
        sl(t2)
        print('  - Speed:        {:.3f}   m/s'.format(data[4]))
        sl(t2)
        print('  - Delta V Used: {:.3f}   m/s'.format(data[5]))
        sl(t2)
        print('  - Time (Raw):   {:.3f}     s'.format(data[6]))
        sl(t2)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t2)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        
        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}')
                    ON CONFLICT DO NOTHING;
                    '''.format(schema_table,
                               pst[0],pst[1],pst[2],pst[3],
                               pst[4],pst[5],pst[6],pst[7],
                               data[0],data[1],data[2],data[3],
                               data[4],data[5],data[6],data[7]))
        cur.execute(INS)
        
        sl(t3)
        print('Raw Data: {}{}'.format(data,nl))
        
        raw.append(data)
        sl(t4)

        return(data)
    
    def fair():

        # no fairinf on test file

        if(options[1]==False):

            no_data[0]='fair'
            data=no_data
            print('{}! No Fairing Required !{}{}'.format(cr[1],cr[0],nl))
            # sl(t3)
            # print('Raw Data: {}{}'.format(data,nl))

        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}')
                    ON CONFLICT DO NOTHING;
                    '''.format(schema_table,
                            pst[0],pst[1],pst[2],pst[3],
                            pst[4],pst[5],pst[6],pst[7],
                            data[0],data[1],data[2],data[3],
                            data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        raw.append(data)
        sl(t4)

        return(data)

    def seco():
        
        v=df[col[5]]
        maxv=max(v)
        val=df[v==maxv]

        alt=val.iloc[pos[0]]
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

        mile='seco'
        
        data=[mile,
              nan,
              alt,
              rng,
              spd,
              dtv,
              rwt,
              row]

        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}')
                ON CONFLICT DO NOTHING;
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],
                           pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],
                           data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        print('{} Second Engine Cut Off (SECO) {}{}'.format(cr[1],cr[0],nl))
        sl(t3)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t2)
        print('  - Range:        {:.3f}   km'.format(data[3]/km))
        sl(t2)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t2)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t2)
        print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        sl(t2)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t2)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        sl(t3)
        print('Raw Data: {}{}'.format(data,nl))

        raw.append(data)
        sl(t4)

        return(data)
    
    def oib():

        change=col[20]
        rate=df[change]
        r=rate.index
        
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
        
        alt=val.iloc[pos[0]]
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

        mile='oib'
        
        data=[mile,
              nan,
              alt,
              rng,
              spd,
              dtv,
              rwt,
              row]

        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}')
                ON CONFLICT DO NOTHING;
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],
                           pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],
                           data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        print('{} Orbital Insertion Burn (OIB) {}{}'.format(cr[1],cr[0],nl))
        sl(t3)
        print('{}  - Alt:          {:.3f}   km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t2)
        print('  - Range:        {:.3f}  km'.format(data[3]/km))
        sl(t2)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t2)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t2)
        print('  - Time (Raw):   {:.3f}  s'.format(data[6]))
        sl(t2)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t2)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        sl(t3)
        print('Raw Data: {}{}'.format(data,nl))

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
        
        alt=val.iloc[pos[0]]
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

        mile='oibeco'
        
        data=[mile,
              nan,
              alt,
              rng,
              spd,
              dtv,
              rwt,
              row]

        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}')
                ON CONFLICT DO NOTHING;
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],
                           pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],
                           data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        print('{} Orbital Insertion Burn Engine Cut Off (OIBECO) {}{}'.format(cr[1],cr[0],nl))
        sl(t3)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t2)
        print('  - Range:        {:.3f}  km'.format(data[3]/km))
        sl(t2)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t2)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t2)
        print('  - Time (Raw):   {:.3f}  s'.format(data[6]))
        sl(t2)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t2)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        sl(t3)
        print('Raw Data: {}{}'.format(data,nl))

        raw.append(data)
        sl(t4)

        return(data)

    def pay():

         # no payload release on test file

        if(schema==sch[0]):
            no_data[0]='pay'
            data=no_data
            # silent - callisto payload is technically its second stage
        else:
            no_data[0]='pay'
            data=no_data
            print('{}! No Payload Released !{}{}'.format(cr[1],cr[0],nl))

            # sl(t3)
            # print('Raw Data: {}{}'.format(data,nl))

        INS=('''INSERT INTO 
                    {} 
                    ({},{},{},{},{},{},{},{})
                    VALUES 
                    ('{}','{}','{}','{}','{}','{}','{}','{}')
                    ON CONFLICT DO NOTHING;
                    '''.format(schema_table,
                            pst[0],pst[1],pst[2],pst[3],
                            pst[4],pst[5],pst[6],pst[7],
                            data[0],data[1],data[2],data[3],
                            data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        raw.append(data)
        sl(t4)

        return(data)
    
    def fin():

        # end of file

        time=df[col[0]]
        
        last=time.iloc[-1]
        val=df[time==last]

        alt=val.iloc[pos[0]]
        rng=val.iloc[pos[1]]
        spd=val.iloc[pos[2]]
        dtv=val.iloc[pos[3]]
        rwt=val.iloc[pos[4]]
        row=val.index[pos[5]]
        
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))

        mile='eof'
        
        data=[mile,
              nan,
              alt,
              rng,
              spd,
              dtv,
              rwt,
              row]

        INS=('''INSERT INTO 
                {} 
                ({},{},{},{},{},{},{},{})
                VALUES 
                ('{}','{}','{}','{}','{}','{}','{}','{}')
                ON CONFLICT DO NOTHING;
                '''.format(schema_table,
                           pst[0],pst[1],pst[2],pst[3],
                           pst[4],pst[5],pst[6],pst[7],
                           data[0],data[1],data[2],data[3],
                           data[4],data[5],data[6],data[7]))
        cur.execute(INS)

        print('{} End of Flight (EoF) {}{}'.format(cr[1],cr[0],nl))
        sl(t3)
        print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        sl(t2)
        print('  - Range:        {:.3f}  km'.format(data[3]/km))
        sl(t2)
        print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        sl(t2)
        print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        sl(t2)
        print('  - Time (Raw):   {:.3f}  s'.format(data[6]))
        sl(t2)
        print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        sl(t2)
        print('  - Row:          {:.0f}{}'.format(data[7],nl))
        sl(t3)
        print('Raw Data: {}{}'.format(data,nl))

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
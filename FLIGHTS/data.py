import pandas as pd
import numpy as np

from ColorOut import CO
from doNotGit.psqlConn import psql
from time import sleep as sl
from psycopg2 import errors, errorcodes
from statistics import mode
from os import system as sys

CO()

# data processing for ksp flight telemetry
# by: marcus dechant (marucs kerman)

# KSC launch vehicle design (LVD) has requested, stage fuel analysis functions

# global text 
nl='\n'

# global math
km=1000

# wait times
t1=0.250
t2=(t1*2)
t3=(t1*3)
t4=(t1*4)
t5=(t4*3)

raw=[]
war=[]

def test_params():  # [0,1,2,3,4,5]     [0]

    ver=3
    file='HM_CAT'      # test[0]
    pad='KSC PAD 1'         # test[1]
    schema='test'           # test[2]
    tele=(f'ttest{ver}')    # test[3]
    flight=(f'ftest')       # test[4]
    data=(f'dtest{ver}')    # test[5]
    
    # ts[0,1,2,3,4,5]
    test=[file,pad,schema,tele,flight,data]
    
    return(test)

def color(): # [0,1,2,3,4,5]    [1]     text colours

    # normal
    nor=        '\033[0;37m'        # cr[0]
    # text
    red=        '\033[1;31m'        # cr[1]
    pur=        '\033[1;35m'        # cr[2]
    grn=        '\033[1;32m'        # cr[3]
    cyn=        '\033[1;36m'        # cr[4]
    yel=        '\033[1;33m'        # cr[5]

    # cr[0,1,2,3,4,5]
    color=[nor,red,pur,grn,cyn,yel]                              

    return(color)

def start(): # Starting dialogue
    
    cr=color()
    ts=test_params()

    sys('cls')

    print(f'{nl} Kerbal Space Program Launch Telemetry Processing and Database Insertion')
    sl(t1)
    print(     ' Python/Postgres/Pandas/Numpy')
    sl(t1)
    print(     ' By: Marcus Dechant')
    sl(t2)

    return(cr,ts)

def user(): # [0,1,2,3,4,5]     [2,3,4]     user input 

    (cr,ts)=start()

    def flight_name(): # [2]   [0,1,2,3]   [3][0,1]

        # file_raw=ts[0] # test params

        # ksp launch vehicle class based naming convention 
        # (class_mission_number)
        # where class is the family of launch vehicle, or in cases when multiple missions are grouped together as in the case of orbital construction (ex. hailmary, KOSS).
        # where mission refers to either the individual mission name ex. HM_CORE, HM_CST, Cache Alpha. Some refer to the payload while some are sequential.
        # where number is when payload named missions required more than one launch (ex. HM_Eng_4)
        
        file_raw=input(f'{nl} Flight Name?                          : ')
        part=file_raw.split("_")                        # part[0,1]
        if(str.lower(part[0])=='cache'):                                                               
            part[0]='callisto'
            part[0]=part[0].capitalize()
        display_name=(' '.join((part[0],part[1])))
        store_name=(''.join((part[0],part[1])))
        
        lvn=[file_raw,display_name,store_name,part]     # lvn[0,1,2,3]  [3][0,1]       

        return(lvn)

    def launch_pad(): # [3]   [0,1,2]      [2][0,1]

        # lpad=ts[1] # test params

        # ready pads
        # KSC Pad 1 - equatorial - Kerbal Space Center
        # DES Pad 2 - (-6) South - Dessert Airforce Base
        # WLS Pad 3 - 45 North - Woomerang Launch Site
        
        lpad=input(' Launch Pad?                           : ')
        part=lpad.split(' ')                            # part[0,1,2]
        display_name=lpad
        store_name=(''.join((part[0],part[1],part[2])))

        pad=[display_name,store_name,part]              # pad[0,1,2]  [2][0,1,2]

        return(pad)
    
    lvn=flight_name()
    pad=launch_pad()
    lv_class=lvn[3][0]
    
    opts=[]
           
    while(True): # user defines if the launch vehicle had boosters
        user1=input(f'{nl} Did {lvn[1]} have boosters? (y/n)     : ')
        if(user1=='y')|(user1=='n'):
                opts.append(user1)
                break
        else:
            print(f' {cr[1]}Incorrect Input. Please enter (y/n).{cr[0]}{nl}')
            continue

    while(True): # user defines if the launch vehicle had a fairing
        user2=input(f' Did {lvn[1]} need a fairing? (y/n)    : ')
        if(user2=='y')|(user2=='n'):
            opts.append(user2)
            break
        else:
            print(f' {cr[1]}Incorrect Input. Please enter (y/n).{cr[0]}{nl}')
            continue
    
    while(True): # user defines if the launch vehicle released a payload
        user3=input(f' Did {lvn[1]} release a payload? (y/n) : ')
        if(user3=='y')|(user3=='n'):
            opts.append(user3)
            break
        else:
            print(f' {cr[1]}Incorrect Input. Please enter (y/n).{cr[0]}{nl}')
            continue

    while(True): # user defines if the Throttle Up was called
        user4=input(f' Did {lvn[1]} Throttle Up? (y/n)       : ')
        if(user4=='y')|(user4=='n'):
            opts.append(user4)
            break
        else:
            print(f' {cr[1]}Incorrect Input. Please enter (y/n).{cr[0]}{nl}')
            continue
    
    inc=[cr,ts,lvn,pad,opts]   
    # inc[0,1,2,3,4]                user output list
    # inc[0]    cr[0,1,2,3,4,5]     colour list
    # inc[1]    ts[0,1,2,3,4,5]     test list
    # inc[2]    lv[0,1,2,3]         lv name list
    #  lv[3]    n[0,1]              lv name part list
    # inc[3]    pd[0,1,2]           pad name list
    #  pd[2]    p[0,1]              pad name part list
    # inc[4]    us[0,1,2,3]         user questions

    sl(t3)

    return(inc)

def source(): # csv source

    inc=user()
    cr=inc[0]
    lv=inc[2]
    code=lv[0]
    clss=lv[3][0]

    print(f'{nl} Reading {lv[1]} Telemetry CSV file. {cr[4]}[{clss}\{code}.csv]{cr[0]}')
    df=pd.read_csv(r'{}\{}.csv'.format(clss,code))

    col=df.columns.values

    #col=df.columns.values
    
    # name[0-17]
    name=['time','stages','alt_sea_lvl','downrange','surf_velo','orbt_velo','mass','acclr',
          'q','aoa','aos','aod','alt_true','pitch','grav_loss','drag_loss','ster_loss','delta_v']

    a=0
    b=len(col)
    while(a<b):
        col[a]=name[a]
        a+=1
    sl(t4)

    return(col,df,inc)

def processing():

    (col,df,inc)=source()
    cr=inc[0]
    # test=inc[1]
    # schema=test[2]

    print(' Processing Data...')

    user_input=[]
    for i in inc[4]:
        if(i=='y'):
            opt=True
            user_input.append(opt)
        else:
            opt=False
            user_input.append(opt)

    names=['v_roc','m_roc','a_roc']
    a=0
    b=0
    for i in col:
        if(i==col[5])or(i==col[6])or(i==col[7]):
            series=df[col[a]]
            roc=series.diff()
            title=names[b]
            roc_iso=pd.DataFrame({title:roc})
            roc_iso.replace([np.NaN,np.inf,(-np.inf)],0,inplace=True)
            df=df.join(roc_iso,how='right')
            if(i==col[6]):
                m_roc=[]
                for ii in roc:
                    if ii!=0:
                        m_roc.append(round(ii,6))
                mass_roc_mode=mode(m_roc)
            b+=1
        if(i==col[0]):
            df[col[0]]=df[col[0]].round(3)
        a+=1
    col=df.columns.values

    for r in col:
        if(r==col[19]):
            df[col[19]]=df[col[19]].abs()

    print(f'{cr[3]} Complete!{cr[0]}{nl}')

    sl(t4)

    important_values=[mass_roc_mode]

    return(col,df,inc,important_values,user_input)    

def flight_telemetry():

    # data from telemetry csv to postgres database
    
    (col,df,inc,iv,user_input)=processing()
    cr=inc[0]
    sch=inc[2][3][0]
    tbl=inc[2][3][1]
    # test=inc[1]
    label='telemetry'
    table=('_'.join((tbl,label)))
    
    print(f' Connecting to PostgreSQL Database: {cr[4]}[ksp]{cr[0]}')
    conn=psql()
    cur=conn.cursor()
    sl(t3)
    print(f'{cr[3]} Connected!{cr[0]}{nl}')
    sl(t2)
    
    schema_table=('.'.join((sch,table))) # set schema name for query
    print(f' Creating Table: {cr[4]} [ksp.{schema_table}]{cr[0]}')
    sl(t2)
    
    while(True):
        try:
            TBL=(f'''CREATE TABLE {schema_table} () INHERITS ({'templates.telemetry'});
                     ALTER TABLE {schema_table} ADD COLUMN 
                     id SERIAL NOT NULL PRIMARY KEY UNIQUE;''') 
            cur.execute(TBL)
            break
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):
            while(True):                                        
                ovrw=input(f'{cr[1]} Table Exists. Overwrite? (y/n): {cr[0]} ')
                if(ovrw=='y')or(ovrw=='n'):
                    if(ovrw=='y'):
                        print(' Overwriting...')
                        sl(t2)
                        DUP=(f'''ROLLBACK;
                                DROP TABLE {schema_table};''')
                        cur.execute(DUP)
                        break
                    else:
                        print(f'{nl}{cr[1]} Process Canceled... {cr[0]}{nl}')
                        exit()
                else:
                    print(' Incorrect Input...') 
                    continue
            continue
    sl(t3)
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t2)

    print(' Populating Table... ')
    for (i,r) in df.iterrows():
        a=tuple(r.values)
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL, (a,)) 
    sl(t5)
    print(f'{cr[3]} Complete!{cr[0]}{nl}')

    ps=[cur,conn]
    
    # cur.close()
    # conn.commit()
    # conn.close()
    # exit ()

    return(col,df,inc,sch,tbl,ps,iv,user_input)

def flight_info():

    # overall flight information

    (col,df,inc,sch,tbl,ps,iv,us)=flight_telemetry()
    cr=inc[0]
    # test=inc[1]
    cur=ps[0]

    a=0
    val=[]
    lst2=[0,2,3,5,6,17]
    while a<len(col):
        for i in lst2:
            if(a==i):
                if(a==6):
                    val.append(df[col[i]].iloc[1]) 
                    val.append(df[col[i]].iloc[-1])
                else:
                    val.append(df[col[i]].iloc[-1] )
        a+=1

    col_str=['name','pad','time','altitude','drange','speed','lv_mass','pay_mass','delta_v','bst_q','pay_q','fst_q','tup_q']

    data=[inc[2][1],inc[3][0],val[0],val[1],val[1],val[3],val[4],val[5],val[6],us[0],us[1],us[2],us[3]]

    
    table='flights'
    schema_table=('.'.join((sch,table)))
    
    time=pd.to_timedelta(data[2],'sec')
    mm=time.components.minutes
    ss=time.components.seconds
    ms=time.components.milliseconds 
    time=(f'{mm:02d}:{ss:02d}.{ms:03d}')

    print(f' Flight Information')
    sl(t2)
    print(f'{cr[1]}  - Flight Name:              {inc[2][1]} {cr[0]}')
    sl(t1)
    print(       f'  - Launch Complex/Pad:       {inc[3][0]}')
    sl(t1)
    print(f'{cr[3]}  - Flight Time:              {time} {cr[0]}')
    sl(t1)
    print(       f'  - Flight Time (Raw):        {data[2]:.3f} s')
    sl(t1)
    print(f'{cr[4]}  - Final Altitude:           {data[3]/km:.3f} km {cr[0]}')
    sl(t1)
    print(       f'  - Downrange:                {data[4]/km:.3f} km')
    sl(t1)
    print(       f'  - Orbital Velocity:         {data[5]/km:.4f} km/s')    
    sl(t1)
    print(       f'  - Launch Vehicle Mass:      {data[6]:.3f} t')
    sl(t1)
    print(f'{cr[2]}  - Payload Mass:             {data[7]:.3f} t{cr[0]}')
    sl(t1)
    print(       f'  - Launch Vehicle DeltaV:    {data[8]:.0f} m/s{nl}')
    sl(t2)
    print(f' Writing to Table: {cr[4]}[ksp.{schema_table}]{cr[0]}')

    data[0]=inc[2][2]
    data[1]=inc[3][1]
    put=tuple(data)

    SQL=(f'''INSERT INTO {schema_table} VALUES %s ON CONFLICT DO NOTHING;''')
    cur.execute(SQL,(put,))

    sl(t3)
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t3)

    raw.append(data)

    return(col,df,inc,sch,tbl,ps,iv,us)

def flight_data():

    (col,df,inc,sch,tbl,ps,iv,user)=flight_info()
    cur=ps[0]
    conn=ps[1]
    cr=inc[0]
    # test=inc[1]

    col7=col[7]
    acc=df[col7]

    col3=col[3]
    alti=df[col3]

    karman=70000   

    col0=col[0]
    tim=df[col0]

    col18=col[18]
    v_roc=df[col18]
 
    col19=col[19]
    m_roc=df[col19]

    col20=col[20]
    a_roc=df[col20]

    # 012 |   0    1    2   3
    # yyy | beco,meco,fair,pay
    # nyy | meco,fair,pay
    # yyn | beco,meco,fair
    # nny | meco,pay
    # ynn | beco,meco
    # nyn | meco,fair
    # yny | beco,meco,pay
    # nnn | meco

    # booster check - waiting on new test
    # meco check - complete
    # fair check - complete
    # pay check -  complete

    mr=[]
    for i2 in m_roc:
        if(i2>1):
            mr.append(i2)    

    mile_pos=[           (0,2)      ,(0,3)  ,(0,5)  ,(0,6) ,(0,17) ,(0,0) ,(0)]
    mile_nan=[None       ,np.NaN    ,np.NaN ,np.NaN ,np.NaN,np.NaN ,np.NaN,-1   ,np.NaN]

    mile=['maxq','thup','beco','meco','fair','krml','seco','oib','oibeco','pay','eof']

    label='data'
    table=('_'.join((tbl,label)))
    schema_table=(f'.'.join((sch,table)))

    print(f' Creating Table: {cr[4]} [ksp.{schema_table}] {cr[0]}')
    sl(t2)
    while(True):
        try:
            TBL=(f'''CREATE TABLE {schema_table} () INHERITS ({'templates.data'});
                     ALTER TABLE {schema_table } ADD COLUMN 
                     id SERIAL NOT NULL PRIMARY KEY UNIQUE; ''')
            cur.execute(TBL)
            break
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):
            while(True):                                        
                ovrw=input(f'{cr[1]} Table Exists. Overwrite? (y/n): {cr[0]} ')
                if(ovrw=='y')or(ovrw=='n'):
                    if(ovrw=='y'):
                        print(' Overwriting...')
                        sl(t2)
                        DUP=(f'''ROLLBACK;
                                 DROP TABLE {schema_table};''')
                        cur.execute(DUP)
                        break
                    else:
                        print(f'{nl}{cr[1]} Process Canceled... {cr[0]}{nl}')
                        exit()
                else:
                    print(' Incorrect Input...')
                    continue
            continue
    sl(t2)            
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t4)
    print(f' {cr[1]}Flight Milestones{cr[0]}')
    sl(t2)
    print(f' Writing to Table {cr[4]}[ksp.{schema_table}]{cr[0]}{nl}')

    def maxq():     # 0

        col8=col[8]        
        Q=df[col8]
        maxQ=max(Q)
        val=df[Q==maxQ]

        mist=mile[0]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),maxQ])

        print(f'{cr[1]} Maximum Dynamic Pressure (MAXQ){cr[0]}')
        sl(t1)
        print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
        sl(t1)
        print(       f' - Downrange:               {(data[2]/km):.3f} km')
        sl(t1)
        print(       f' - Velocity:                {data[3]:.3f} m/s')
        sl(t1)
        print(       f' - Vehicle Mass:            {data[4]:.3f} t')
        sl(t1)
        print(       f' - Delta V Used:            {data[5]:.3f} m/s')
        sl(t1)
        print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
        sl(t1)
        print(       f' - Flight Time:             {time}')
        sl(t1)
        print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}')
        sl(t1)
        print(f'{cr[2]} - Dynamic Pressure (Q):    {data[8]:.3f} Pa{cr[0]}{nl}')

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)
        
        raw.append(data)
        war.append(data)

        return(data)
    
    def thup():     # 1
       
        if(user[3]==True):
            lst=[]
            for i in tim:
                if(i>35)&(i<60):
                    lst.append(i)                
            thup=max(df[tim.isin(lst)][col20])
            
            val=df[a_roc==thup]

            mist=mile[1]
            alt=val.iloc[mile_pos[0]]    
            rng=val.iloc[mile_pos[1]]
            spd=val.iloc[mile_pos[2]]
            mas=val.iloc[mile_pos[3]]
            dtv=val.iloc[mile_pos[4]]
            rwt=val.iloc[mile_pos[5]]
            row=val.index[mile_pos[6]]

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

            print(f'{cr[1]} Throttle Up (THUP){cr[0]}')
            sl(t1)
            print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
            sl(t1)
            print(       f' - Downrange:               {(data[2]/km):.3f} km')
            sl(t1)
            print(       f' - Velocity:                {data[3]:.3f} m/s')
            sl(t1)
            print(       f' - Vehicle Mass:            {data[4]:.3f} t')
            sl(t1)
            print(       f' - Delta V Used:            {data[5]:.3f} m/s')
            sl(t1)
            print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
            sl(t1)
            print(       f' - Flight Time:             {time}')
            sl(t1)
            print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')
            
        else:
            mile_nan[0]=mile[1]
            data=tuple(mile_nan)
            
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)
        
        raw.append(data)
        war.append(data)

        return(data)

    def beco():     # 2

        if(user[0]==True):

            val=df[m_roc==mr[0]]

            mist=mile[2]
            alt=val.iloc[mile_pos[0]]    
            rng=val.iloc[mile_pos[1]]
            spd=val.iloc[mile_pos[2]]
            mas=val.iloc[mile_pos[3]]
            dtv=val.iloc[mile_pos[4]]
            rwt=val.iloc[mile_pos[5]]
            row=val.index[mile_pos[6]]

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

            print(f'{cr[1]} Booster Engine Cut Off (BECO){cr[0]}')
            sl(t1)
            print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
            sl(t1)
            print(       f' - Downrange:               {(data[2]/km):.3f} km')
            sl(t1)
            print(       f' - Velocity:                {data[3]:.3f} m/s')
            sl(t1)
            print(       f' - Vehicle Mass:            {data[4]:.3f} t')
            sl(t1)
            print(       f' - Delta V Used:            {data[5]:.3f} m/s')
            sl(t1)
            print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
            sl(t1)
            print(       f' - Flight Time:             {time}')
            sl(t1)
            print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')

        else:
            mile_nan[0]=mile[2]
            data=tuple(mile_nan)
            
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))

        raw.append(data)
        war.append(data)

        return(data)

    def meco():     # 3

        if(user[0]==True):
            x=1
        else:
            x=0

        val=df[m_roc==mr[x]]

        mist=mile[3]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

        print(f'{cr[1]}Main Engine Cut Off (MECO){cr[0]}')
        sl(t1)
        print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
        sl(t1)
        print(       f' - Downrange:               {(data[2]/km):.3f} km')
        sl(t1)
        print(       f' - Velocity:                {data[3]:.3f} m/s')
        sl(t1)
        print(       f' - Vehicle Mass:            {data[4]:.3f} t')
        sl(t1)
        print(       f' - Delta V Used:            {data[5]:.3f} m/s')
        sl(t1)
        print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
        sl(t1)
        print(       f' - Flight Time:             {time}')
        sl(t1)
        print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)
        
        raw.append(data)
        war.append(data)

        return(data)
    
    def fair():     # 4

        if(user[1]==True):
            if(user[0]==True):
                x=2
            else:
                x=1
            val=df[m_roc==mr[x]]

            mist=mile[4]
            alt=val.iloc[mile_pos[0]]    
            rng=val.iloc[mile_pos[1]]
            spd=val.iloc[mile_pos[2]]
            mas=val.iloc[mile_pos[3]]
            dtv=val.iloc[mile_pos[4]]
            rwt=val.iloc[mile_pos[5]]
            row=val.index[mile_pos[6]]

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

            print(f'{cr[1]} Fairing Separation (FAIR){cr[0]}')
            sl(t1)
            print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
            sl(t1)
            print(       f' - Downrange:               {(data[2]/km):.3f} km')
            sl(t1)
            print(       f' - Velocity:                {data[3]:.3f} m/s')
            sl(t1)
            print(       f' - Vehicle Mass:            {data[4]:.3f} t')
            sl(t1)
            print(       f' - Delta V Used:            {data[5]:.3f} m/s')
            sl(t1)
            print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
            sl(t1)
            print(       f' - Flight Time:             {time}')
            sl(t1)
            print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')
                    
        else:
            mile_nan[0]=mile[4]
            data=tuple(mile_nan)
            
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)

    def seco():     # 5
        
        filt=df[(v_roc<0)&(alti>karman)&(a_roc>(-0.1))]
        num=filt.index[0]
        if(num>war[3][7]):
            vrd=filt['v_roc'].tolist()
            
            val=df[v_roc==vrd[0]]

            mist=mile[6]
            alt=val.iloc[mile_pos[0]]    
            rng=val.iloc[mile_pos[1]]
            spd=val.iloc[mile_pos[2]]
            mas=val.iloc[mile_pos[3]]
            dtv=val.iloc[mile_pos[4]]
            rwt=val.iloc[mile_pos[5]]
            row=val.index[mile_pos[6]]

            # if(war[3][7]>row):

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

            print(f'{cr[1]} Second Engine Cutt Off (SECO){cr[0]}')
            sl(t1)
            print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
            sl(t1)
            print(       f' - Downrange:               {(data[2]/km):.3f} km')
            sl(t1)
            print(       f' - Velocity:                {data[3]:.3f} m/s')
            sl(t1)
            print(       f' - Vehicle Mass:            {data[4]:.3f} t')
            sl(t1)
            print(       f' - Delta V Used:            {data[5]:.3f} m/s')
            sl(t1)
            print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
            sl(t1)
            print(       f' - Flight Time:             {time}')
            sl(t1)
            print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')
        
        else:
            mile_nan[0]=mile[6]
            data=tuple(mile_nan)

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)
        
        raw.append(data)
        war.append(data)

        return(data)

    def oib():      # 6

        a=war[5][7]
        if(a==(-1)):
            a=1000
        # b=(rate[-1]-(rate[-1]*0.25))
        
        right=v_roc.iloc[a:][v_roc.iloc[a:].gt(0)].tolist()
        val=df[v_roc==right[0]]
        
        mist=mile[7]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

        print(f'{cr[1]} Orbital Insertion Burn (OIB){cr[0]}')
        sl(t1)
        print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
        sl(t1)
        print(       f' - Downrange:               {(data[2]/km):.3f} km')
        sl(t1)
        print(       f' - Velocity:                {data[3]:.3f} m/s')
        sl(t1)
        print(       f' - Vehicle Mass:            {data[4]:.3f} t')
        sl(t1)
        print(       f' - Delta V Used:            {data[5]:.3f} m/s')
        sl(t1)
        print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
        sl(t1)
        print(       f' - Flight Time:             {time}')
        sl(t1)
        print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)
    
    def oibeco():   # 7

        rate=m_roc.index

        a=rate[war[6][7]]
        lst=[]
        right=m_roc.iloc[a:].index[m_roc.iloc[a:].eq(0)].tolist()
        for i in right:
            value=a_roc.iloc[i]
            if(value>(-1)):
                lst.append(value)
        val=df[a_roc==lst[0]]
                
        mist=mile[8]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

        print(f'{cr[1]} Orbital Insertion Burn Engine Cut Off (OIBECO){cr[0]}')
        sl(t1)
        print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
        sl(t1)
        print(       f' - Downrange:               {(data[2]/km):.3f} km')
        sl(t1)
        print(       f' - Velocity:                {data[3]:.3f} m/s')
        sl(t1)
        print(       f' - Vehicle Mass:            {data[4]:.3f} t')
        sl(t1)
        print(       f' - Delta V Used:            {data[5]:.3f} m/s')
        sl(t1)
        print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
        sl(t1)
        print(       f' - Flight Time:             {time}')
        sl(t1)
        print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)

    def pay():      # 8

        a=user[0]==False
        b=user[1]==False
        c=user[2]==True

        if(c):
            x=3
            if(a)|(b):
                x=2
            if(a)&(b):
                x=1

            val=df[m_roc==mr[x]]

            mist=mile[9]
            alt=val.iloc[mile_pos[0]]    
            rng=val.iloc[mile_pos[1]]
            spd=val.iloc[mile_pos[2]]
            mas=val.iloc[mile_pos[3]]
            dtv=val.iloc[mile_pos[4]]
            rwt=val.iloc[mile_pos[5]]
            row=val.index[mile_pos[6]]

            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

            print(f'{cr[1]} Payload Release (PAY){cr[0]}')
            sl(t1)
            print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
            sl(t1)
            print(       f' - Downrange:               {(data[2]/km):.3f} km')
            sl(t1)
            print(       f' - Velocity:                {data[3]:.3f} m/s')
            sl(t1)
            print(       f' - Vehicle Mass:            {data[4]:.3f} t')
            sl(t1)
            print(       f' - Delta V Used:            {data[5]:.3f} m/s')
            sl(t1)
            print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
            sl(t1)
            print(       f' - Flight Time:             {time}')
            sl(t1)
            print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')
        else:
            mile_nan[0]=mile[9]
            data=tuple(mile_nan)

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)
    
    def fin():      # 9

        # end of file

        val=df[tim==tim.iloc[-1]]

        mist=mile[10]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),np.NaN])

        print(f'{cr[1]} End of Flight (EoF){cr[0]}')
        sl(t1)
        print(f'{cr[4]} - Altitude:                {(data[1]/km):.3f} km{cr[0]}')
        sl(t1)
        print(       f' - Downrange:               {(data[2]/km):.3f} km')
        sl(t1)
        print(       f' - Velocity:                {data[3]:.3f} m/s')
        sl(t1)
        print(       f' - Vehicle Mass:            {data[4]:.3f} t')
        sl(t1)
        print(       f' - Delta V Used:            {data[5]:.3f} m/s')
        sl(t1)
        print(f'{cr[3]} - Raw Time:                {data[6]:.3f} s {cr[0]}')
        sl(t1)
        print(       f' - Flight Time:             {time}')
        sl(t1)
        print(f'{cr[5]} - Table Row:               {data[7]}{cr[0]}{nl}')

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)        

    def order():
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
    
    order()
    print(f'{cr[3]} Complete!{cr[0]}{nl}')

    return(raw,cur,conn)

def run():
    
    (raw,cur,conn)=flight_data()
    cur.close()
    conn.commit()
    conn.close()
    # print(war)
    # print(raw)

if __name__=="__main__":
    run()
    print(input(f' Process Complete!{nl} Press any Key to Continue...{nl}'))
    sys('cls')
    exit()
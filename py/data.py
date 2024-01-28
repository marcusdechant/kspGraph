import pandas as pd
import numpy as np

from ColorOut import CO
from psqlConn import psql
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

# def test_params():  # [0,1,2,3,4,5]

#     ver=3
#     file='HM_CAT'           # test[0]
#     pad='KSC PAD 1'         # test[1]
#     schema='test'           # test[2]
#     tele=(f'ttest{ver}')    # test[3]
#     flight=(f'ftest')       # test[4]
#     data=(f'dtest{ver}')    # test[5]
    
#     # ts[0,1,2,3,4,5]
#     test=[file,pad,schema,tele,flight,data]
    
#     return(test)

def color(): # inc[0]       text colour options

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

def start(): #              Start dialogue
    
    cr=color()

    sys('cls')

    print(f'{nl} Kerbal Space Program Launch Telemetry Processing and Database Insertion')
    sl(t1)
    print(     ' Python/Postgres/Pandas/Numpy')
    sl(t1)
    print(     ' By: Marcus Dechant')
    sl(t2)

    return(cr)

def user(): # inc[1,2,3]    user input/flight info 

    (cr)=start()
    
    def flight_name(): # lv [0,1,2,3][frp]  flight name
        
        # ksp launch vehicle class based naming convention 
        # (class_mission_number)
        # where class is the family of launch vehicle, or in cases when multiple missions are grouped together as in the case of orbital construction (ex. hailmary, KOSS).
        # where mission refers to either the individual mission name ex. HM_CORE, HM_CST, Cache Alpha. Some refer to the payload while some are sequential.
        # where number is when payload named missions required more than one launch (ex. HM_Eng_4)
        
        fr=input(f'{nl} Flight Name?                          : ')
        frp=fr.split('_')   # frp [0,1] OR [0,1,2]
        if(len(frp)==3): # numbered conventions
            alpha=(frp[1],frp[2])
            bravo=(frp[0],frp[1],frp[2])
            tbl=('_'.join(alpha))
            fdis=(' '.join(bravo))
            fstr=(''.join(bravo))
        else:
            tbl=frp[1]
            char=(frp[0],frp[1])
            fdis=(' '.join(char))
            fstr=(''.join(char))
        
        lv=[fr,fdis,fstr,frp,tbl] # lv [0,1,2,3,4][frp]
        
        # fr    : class_name_number
        # fdis  : class name number
        # fstr  : classnamenumber
        # frp   : [class,name,number]
        # tbl   : name_number
                        
        return(lv)

    def launch_pad(): # pd [0,1,2] [lpp]    pad name

        # ready pads
        # KSC Pad 1 - equatorial - Kerbal Space Center
        # DES Pad 2 - (-6) South - Dessert Airforce Base
        # WLS Pad 3 - 45 North - Woomerang Launch Site
        
        lp=input(' Launch Pad?                           : ')
        lpp=lp.split(' ')   # lpp [0,1,2]
        alpha=(lpp[0],lpp[1],lpp[2])
        lstr=(''.join(alpha))
        
        pd=[lp,lstr,lpp]    # pd [0,1,2][lpp]
        
        # lp    : complex_pad_number
        # lstr  : complexpadnumber
        # lpp   : [complex,pad,number]
        
        return(pd)
    
    lvn=flight_name()
    pad=launch_pad()
    
    opts=[]
    
    lst=[f'{nl} Did {lvn[1]} have boosters? (y/n)     : ',
             f' Did {lvn[1]} need a fairing? (y/n)    : ',
             f' Did {lvn[1]} release a payload? (y/n) : ',
             f' Did {lvn[1]} Throttle Up? (y/n)       : ']
    a=0
    while(a<len(lst)):
        while(True):
            user=input(lst[a])
            if(user=='y')|(user=='n'):
                opts.append(user)
                break
            else:
                print(f' {cr[1]}Incorrect Input. Please enter (y/n).{cr[0]}{nl}')
                continue
        a+=1

    inc=[cr,lvn,pad,opts]
    
    # cr    :   [0][0,1,2,3,4,5]
    # lvn   :   [1][0,1,2,3,4][0,1,2]
    # pad   :   [2][0,1,2][0,1,2]
    # opts  :   [3][0,1,2,3]
    
    sl(t4)
    
    return(inc)

def source(): #             csv source

    inc=user()
    cr=inc[0]
    file=inc[1][0]
    folder=inc[1][3][0]

    print(f'{nl} Reading {inc[1][1]} Telemetry CSV file. {cr[4]}[{folder}\{file}.csv]{cr[0]}')
    df=pd.read_csv(r'{}\{}.csv'.format(folder,file))

    col=df.columns.values

    # column renaming
    name=['time','stages','alt_sea_lvl','downrange','surf_velo','orbt_velo','mass','acclr',
          'q','aoa','aos','aod','alt_true','pitch','grav_loss','drag_loss','ster_loss','delta_v']
    a=0
    b=len(col)
    while(a<b):
        col[a]=name[a]
        a+=1
    
    sl(t4)

    return(col,df,inc)

def processing(): #         data processing
    
    (col,df,inc)=source()
    cr=inc[0]

    print(' Processing Data...')

    # ui to bool
    ui=[]
    for i1 in inc[3]: 
        if(i1=='y'):
            opt=True
            ui.append(opt)
        else:
            opt=False
            ui.append(opt)
    
    # rate of change columns
    names=['v_roc','m_roc','a_roc']
    a=0
    b=0
    for i2 in col: 
        if(i2==col[5])|(i2==col[6])|(i2==col[7]):
            roc=df[col[a]].diff()
            title=names[b]
            roc_iso=pd.DataFrame({title:roc})
            roc_iso.replace([np.NaN,np.inf,(-np.inf)],0,inplace=True)
            df=df.join(roc_iso,how='right')
            b+=1
        a+=1
    col=df.columns.values

    # df column adjustments
    for i3 in col: 
        if(i3==col[0]):
            df[col[0]]=df[col[0]].round(3)
        if(i3==col[19]):
            df[col[19]]=df[col[19]].abs()

    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t4)

    return(col,df,inc,ui)    

def flight_telemetry():

    # data from telemetry csv to postgres database
    
    (col,df,inc,ui)=processing()
    cr=inc[0]
    tbl=inc[1][4]
    sch=inc[1][3][0]
        
    print(f' Connecting to PostgreSQL Database: {cr[4]}[ksp]{cr[0]}')
    conn=psql()
    cur=conn.cursor()
    sl(t3)
    print(f'{cr[3]} Connected!{cr[0]}{nl}')
    sl(t2)
    
    label='telemetry'
    table=('_'.join((tbl,label)))
    schema_table=('.'.join((sch,table)))
    
    print(f' Creating Table: {cr[4]} [ksp.{schema_table}]{cr[0]}')
    sl(t2)
    
    while(True): # telemetry table creation
        try:
            TBL=(f'''CREATE TABLE {schema_table} () INHERITS ({'templates.telemetry'});
                     ALTER TABLE {schema_table} ADD COLUMN 
                     id SERIAL NOT NULL PRIMARY KEY UNIQUE;''') 
            cur.execute(TBL)
            break
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):
            while(True):                                        
                ovrw=input(f'{cr[1]} Table Exists. Overwrite? (y/n): {cr[0]} ')
                if(ovrw=='y')|(ovrw=='n'):
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
    
    for (i,r) in df.iterrows(): # data insertion
        a=tuple(r.values)
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL, (a,)) 
    
    sl(t5)
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    
    ps=[cur,conn]
    
    return(col,df,inc,ps,ui)

def flight_info():

    # overall flight information

    (col,df,inc,ps,us)=flight_telemetry()
    cr=inc[0]
    cur=ps[0]
    
    sch=inc[1][3][0]
    
    table='flights'
    schema_table=('.'.join((sch,table)))
    
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

    data=[inc[1][1],inc[2][0],val[0],val[1],val[1],val[3],val[4],val[5],val[6],us[0],us[1],us[2],us[3]]
   
    time=pd.to_timedelta(data[2],'sec')
    mm=time.components.minutes
    ss=time.components.seconds
    ms=time.components.milliseconds 
    time=(f'{mm:02d}:{ss:02d}.{ms:03d}')

    print(f' Flight Information')
    sl(t2)
    print(f'{cr[1]}  - Flight Name:              {inc[1][1]} {cr[0]}')
    sl(t1)
    print(       f'  - Launch Complex/Pad:       {inc[2][0]}')
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

    data[0]=inc[1][2]
    data[1]=inc[2][1]
    put=tuple(data)

    SQL=(f'''INSERT INTO {schema_table} VALUES %s ON CONFLICT DO NOTHING;''')
    cur.execute(SQL,(put,))

    sl(t3)
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t3)

    raw.append(data)

    return(col,df,inc,ps,us)

def flight_data():

    (col,df,inc,ps,user)=flight_info()
    cur=ps[0]
    conn=ps[1]
    cr=inc[0]
    
    tbl=inc[1][4]
    sch=inc[1][3][0]

    col7=col[7]
    acc=df[col7]

    col3=col[3]
    alti=df[col3]

    col0=col[0]
    tim=df[col0]

    col18=col[18]
    v_roc=df[col18]
 
    col19=col[19]
    m_roc=df[col19]

    col20=col[20]
    a_roc=df[col20]
    
    karman=70000

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
                if(ovrw=='y')|(ovrw=='n'):
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

if(__name__=="__main__"):
    run()
    print(input(f' Process Complete!{nl} Press any Key to Continue...{nl}'))
    sys('cls')
    exit()
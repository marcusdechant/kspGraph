from ColorOut import CO
from doNotGit.psqlConn import psql
import pandas as pd
import numpy as np
from time import sleep as sl
from psycopg2 import errors, errorcodes
from statistics import mode

CO()

# data processing for ksp flight telemetry
# by: marcus dechant

# global text 
nl='\n'

# global math
km=1000

# time variables
tyme=0.25
t1=0.250
t2=0.500
t3=0.750
t4=1
t5=3

man=[np.NaN,np.inf] # non existent data manipulation    man[0,1]

temp=['templates.data','templates.telemetry'] # database template tables    temp[0,1]

raw=[]
war=[]

def test_params():  # [0,1,2,3,4,5]     [0]

    ver=3
    file='HM_CAT'           # test[0]
    pad='KSC PAD 1'         # test[1]
    schema='test'           # test[2]
    tele=(f'ttest{ver}')    # test[3]
    flight=(f'ftest')       # test[4]
    data=(f'dtest{ver}')    # test[5]
    
    # ts[0,1,2,3,4,5]
    test=[file,pad,schema,tele,flight,data]
    
    return(test)

def color(): # [0,1,2,3,4,5]    [1]     text colours

    # complete
    # formated and edited
    # comments complete

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

    # complete
    # formatted and edited
    # waiting on comments
    
    cr=color()
    ts=test_params()

    print(f'{nl} Kerbal Space Program Launch Telemetry Processing and Database Insertion')
    sl(t1)
    print(' Python/Postgres')
    sl(t1)
    print(' By: Marcus Dechant')
    sl(t2)

    return(cr,ts)

def user(): # [0,1,2,3,4,5]     [2,3,4]     user input

    # complete
    # formatted and edited
    # inital comments complete
    # waiting on final comments   

    (cr,ts)=start()

    def flight_name(): # [2]   [0,1,2,3]   [3][0,1]

        # complete
        # formatted and edited
        # inital comments complete
        # waiting on final comments

        file_raw=ts[0] # test params

        # ksp launch vehicle class based naming convention 
        # (class_mission_number)
        # where class is the family of launch vehicle, or in cases when multiple missions are grouped together as in the case of orbital construction (ex. hailmary, KOSS).
        # where mission refers to either the individual mission name ex. HM_CORE, HM_CST, Cache Alpha. Some refer to the payload while some are sequential.
        # where number is when payload named missions required more than one launch (ex. HM_Eng_4)
        
        # file_raw=input(f'{nl} Flight Name?                                     : ')
        part=file_raw.split("_")                        # part[0,1]
        if(str.lower(part[0])=='cache'):                                                               
            part[0]='callisto'
        display_name=(' '.join((part[0],part[1])))
        store_name=(''.join((part[0],part[1])))
        
        lvn=[file_raw,display_name,store_name,part]     # lvn[0,1,2,3]  [3][0,1]       

        return(lvn)

    def launch_pad(): # [3]   [0,1,2]      [2][0,1]

        # complete
        # formatted and edited
        # inital comments complete
        # waiting on final comments

        lpad=ts[1] # test params

        # ready pads
        # KSC Pad 1 - equatorial - Kerbal Space Center
        # DES Pad 2 - (-6) South - Dessert Airforce Base
        # WLS Pad 3 - 45 North - Woomerang Launch Site
        
        # lpad=input(' Launch Pad?                                      : ')
        part=lpad.split(' ')                            # part[0,1,2]
        display_name=lpad
        store_name=(''.join((part[0],part[1],part[2])))

        pad=[display_name,store_name,part]              # pad[0,1,2]  [2][0,1,2]

        return(pad)
    
    lvn=flight_name()
    pad=launch_pad()
    lv_class=lvn[3][0]
    
    opts=[]
    if(lv_class.lower==lvn[3][0]): # Callisto preset factors
        opts.append('y') 
        opts.append('n')
        opts.append('n')
    else:       # user defines if the launch vehicle had boosters
        opts.append(input(f'{nl} Did the {lvn[1]} have boosters? (y/n)      : ')) 
                # user defines if launch vehicle staged a payload
        opts.append(input(    f' Did the {lvn[1]} need a fairing? (y/n)     : ')) 
                # user defines if launch vehicle staged a fairing
        opts.append(input(    f' Did the {lvn[1]} release a payload? (y/n)  : '))            
        
    lst=[]             # [4]    [0,1,2]
    for i in opts:      
        if(i=='y'):
            opt=True
            lst.append(opt)
        else:
            opt=False
            lst.append(opt)

    inc=[cr,ts,lvn,pad,lst]   
    # inc[0,1,2,3,4]                user output list
    # inc[0]    cr[0,1,2,3,4,5]     colour list
    # inc[1]    ts[0,1,2,3,4,5]     test list
    # inc[2]    lv[0,1,2,3]         lv name list
    #  lv[3]    n[0,1]              lv name part list
    # inc[3]    pd[0,1,2]           pad name list
    #  pd[2]    p[0,1]              pad name part list
    # inc[4]    us[0,1,2]           user questions

    return(inc)

def source(): # csv source

    # complete
    # formatted and edited
    # waiting on comments

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
    name=['time','stages','alt_sea_lvl','downrange','surf_vel','orbt_vel','mass','acclr',
          'q','aoa','aos','aod','alt_true','pitch','gls','dls','sls','delta_v']

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
    test=inc[1]
    schema=test[2]

    print(' Processing Data...')

    names=['v_roc','m_roc','a_roc']
    a=0
    b=0
    c=0
    for i in col:
        if(i==col[5])or(i==col[6])or(i==col[7]):
            series=df[col[a]]
            roc=series.diff()
            title=names[b]
            roc_iso=pd.DataFrame({title:roc})
            roc_iso.replace([man[0],man[1],(-man[1])],0,inplace=True)
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
    sl(t4)

    for r in col:
        if(r==col[19]):
            df[col[19]]=df[col[19]].abs()

    # determine schmea from file naming conventions
    sch=['callisto','hailmary']
    # if(sch=='Callisto' or sch=='Cache'): 
    #     schema=(sch[0])
    # if(sch=='HM' or sch=='Hailmary'):
    #     schema=(sch[1])

    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t2)
    important_values=[mass_roc_mode]
    return(col,df,inc,schema,important_values)    

def flight_telemetry():

    # data from telemetry csv to postgres database
    
    (col,df,inc,sch,iv)=processing()
    cr=inc[0]
    test=inc[1]
    
    print(f' Connecting to PostgreSQL Database: {cr[4]}[ksp]{cr[0]}')
    conn=psql()
    cur=conn.cursor()
    sl(t3)
    print(f'{cr[3]} Connected!{cr[0]}{nl}')
    sl(t2)
    
    schema_table=('.'.join((sch,test[3]))) # set schema name for query
    print(f' Creating Table: {cr[4]} [ksp.{schema_table}]{cr[0]}')
    sl(t2)
    
    while(True):                                                                            # telemetry table creation
        try:
            TBL=(f'''CREATE TABLE {schema_table} () INHERITS ({temp[1]});
                     ALTER TABLE {schema_table} ADD PRIMARY KEY (time);''')                                         # create table command
            cur.execute(TBL)                                                                # execute sql command
            break                                                                           # break while loop
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):                                  # if table already exists
            while(True):                                        
                ovrw=input(f'{cr[1]} Table Exists. Overwrite? (y/n): {cr[0]} ')  # ask user if they wish to overwrite
                ovry=(ovrw=='y')
                ovrx=(ovrw=='n')
                if(ovry)or(ovrx):
                    if(ovry):                                                               # user wishes to proceed
                        print(' Overwriting...')
                        sl(t2)
                        DUP=(f'''ROLLBACK;
                                DROP TABLE {schema_table};''')                            # drop table command
                        cur.execute(DUP)                                                    # execute sql command
                        break                                                               # break while loop
                    else:
                        print(f'{nl}{cr[1]} Process Canceled... {cr[0]}{nl}')     # user does not want to overwritte
                        exit()                                                              # exit program
                else:
                    print(' Incorrect Input...')                                            # neither yes or no
                    continue                                                                # continue while loop
            continue
    sl(t4)                                                                        # if table exists, delete and loop            
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t5)
    print(' Populating Table... ')
    for (i,r) in df.iterrows():                                     # iterate through rows
        a=tuple(r.values)                                           # turn each row into a tuple
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')          # sql command
        cur.execute(SQL, (a,)) 
    sl(t5)

    print(f'{cr[3]} Complete!{cr[0]}{nl}')

    ps=[cur,conn]

    return(col,df,inc,sch,ps,iv)

def flight_info():

    # overall flight information

    (col,df,inc,sch,ps,iv)=flight_telemetry()
    cr=inc[0]
    test=inc[1]
    cur=ps[0]


    a=0
    val=[]                                      # for loop for general flight information
    lst2=[0,2,3,5,6,17]                         # columns where either the final or inital value are flight information
    while a<len(col):                           # while loop to iterate through columns
        for i in lst2:                          # for loop to iterate through flight information list
            if(a==i):
                if(a==6):                       # if while loop iteration equals flight info list value
                    val.append(df[col[i]].iloc[1]) 
                    val.append(df[col[i]].iloc[-1])
                else:
                    val.append(df[col[i]].iloc[-1] )
        a+=1

    fl_in={'name'       :[inc[2][2]],
           'pad'        :[inc[3][1]],
           'time'       :[val[0]],
           'altitude'   :[val[1]],
           'range'      :[val[2]],
           'speed'      :[val[3]],
           'lv_mass'    :[val[4]],
           'pay_mass'   :[val[5]],
           'delta_v'    :[val[6]],
           'bst_q'      :[inc[4][0]],
           'pay_q'      :[inc[4][1]],
           'fst_q'      :[inc[4][2]]}

    fl=pd.DataFrame(fl_in)
    fol=fl.columns.values

    data=[fl[fol[0]][0],fl[fol[1]][0],fl[fol[2]][0],fl[fol[3]][0],
          fl[fol[4]][0],fl[fol[5]][0],fl[fol[6]][0],fl[fol[7]][0],
          fl[fol[8]][0],inc[4][0],inc[4][1],inc[4][2]]
    
    put=tuple(data)
          
    schema_table=('.'.join((sch,test[4])))         #set schema name for query)

    time=pd.to_timedelta(data[2],'sec')                     # translating raw time (seconds) into clock time for readability
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

    SQL=(f'''INSERT INTO {schema_table} VALUES %s ON CONFLICT DO NOTHING;''')
    cur.execute(SQL,(put,))

    sl(t3)
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t3)

    raw.append(data)

    return(col,df,fol,fl,inc,sch,ps,iv)

def flight_data():

    (col,df,fol,fl,inc,sch,ps,iv)=flight_info()
    cur=ps[0]
    conn=ps[1]
    cr=inc[0]
    test=inc[1]
    nan=man[0]
    user=inc[4]

    col3=col[3]
    alt=df[col3]

    karman=[]
    for i0 in alt:
        if(69500<i0<70500):
            karman.append(i0)

    col0=col[0]
    tim=df[col0]

    col18=col[18]
    v_roc=df[col18]
 
    col19=col[19]
    m_roc=df[col19]

    col20=col[20]
    a_roc=df[col20]

    # 012   | [0]   1    2    3
    # yyy   |       beco,meco,seco
    # nyy   |       meco,seco

    vr=[]
    vrr=[] # vrr[0]=lift off,   #vrr[1]=beco/meco,   vrr[2]=meco/seco   #vrr[3]=seco/na
    for i1 in v_roc:
        if(i1<0.01):
            vr.append(i1)
        elif vr:
            vrr.append(vr[0])
            vr=[]
    if vr:
        vrr.append(vr[0])

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
    mile_nan=[None       ,man[0]    ,man[0] ,man[0] ,man[0],man[0] ,man[0],-1   ,man[0]]

    mile=['maxq','thup','beco','meco','fair','krml','seco','oib','oibeco','pay','eof']

    schema_table=(f'.'.join((sch,test[5])))

    print(f' Creating Table: {cr[4]} [ksp.{schema_table}] {cr[0]}')
    sl(t2)
    while(True):                                                                            # telemetry table creation
        try:
            TBL=(f'''CREATE TABLE {schema_table} () INHERITS ({temp[0]});
                     ALTER TABLE {schema_table} ADD PRIMARY KEY (milestone);''')                                        # create table command
            cur.execute(TBL)                                                                # execute sql command
            break                                                                           # break while loop
        except(errors.lookup(errorcodes.DUPLICATE_TABLE)):                                  # if table already exists
            while(True):                                        
                ovrw=input(f'{cr[1]} Table Exists. Overwrite? (y/n): {cr[0]} ')  # ask user if they wish to overwrite
                if(ovrw=='y')or(ovrw=='n'):
                    if(ovrw=='y'):                                                               # user wishes to proceed
                        print(' Overwriting...')
                        sl(t2)
                        DUP=(f'''ROLLBACK;
                                 DROP TABLE {schema_table};''')                            # drop table command
                        cur.execute(DUP)                                                    # execute sql command
                        break                                                               # break while loop
                    else:
                        print(f'{nl}{cr[1]} Process Canceled... {cr[0]}{nl}')     # user does not want to overwritte
                        exit()                                                              # exit program
                else:
                    print(' Incorrect Input...')                                            # neither yes or no
                    continue                                                                # continue while loop
            continue
    sl(t4)                                                                        # if table exists, delete and loop            
    print(f'{cr[3]} Complete!{cr[0]}{nl}')
    sl(t3)
    print(f' Flight Milestones')
    sl(t1)
    print(f' Writing to Table {cr[4]}[ksp.{schema_table}]{cr[0]}{nl}')

    def maxq(): # 0

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

        # clock time generation
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
    
    def thup(): # 1
        
        time=df[col[0]]

        lst=[]
        for i in time:
            if(i>35)&(i<50):
                lst.append(i)
        try:                
            thup=max(df[time.isin(lst)][col20])
            
            val=df[a_roc==thup]

            mist=mile[1]
            alt=val.iloc[mile_pos[0]]    
            rng=val.iloc[mile_pos[1]]
            spd=val.iloc[mile_pos[2]]
            mas=val.iloc[mile_pos[3]]
            dtv=val.iloc[mile_pos[4]]
            rwt=val.iloc[mile_pos[5]]
            row=val.index[mile_pos[6]]

            # clock time generation
            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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
            
        except(KeyError):
            mile_nan[0]=mile[1]
            data=tuple(mile_nan)
            
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)
        
        raw.append(data)
        war.append(data)

        return(data)

    def beco(): # 2     NA for test 3

        # boosters on test file 1
        # boosters on test file 2x
        # no boosters on test file 3

        # boosters equipped
        # if(user[0]==True):

        #     stg=stage_list[0]
        #     val=df[stage==stg]

        #     print(val)
        #     exit()

        #     # using generated dataframe find relevant values
        #     alt=val.iloc[pos[0]]    
        #     rng=val.iloc[pos[1]]
        #     spd=val.iloc[pos[2]]
        #     dtv=val.iloc[pos[3]]
        #     rwt=val.iloc[pos[4]]
        #     row=val.index[pos[5]]
            
        #     # clock time generation
        #     td=pd.to_timedelta(rwt,'sec')
        #     mm=int(td.components.minutes)
        #     ss=int(td.components.seconds)
        #     ms=round(int(td.components.milliseconds), 3)
        #     time=('{}:{:02d}.{:03d}'.format(mm,ss,ms))  

        #     # data list
        #     data=[mile[2],nan,alt,rng,spd,dtv,rwt,row]

        #     # read out
        #     print('{} Booster Engine Cut Off (BECO) {}{}'.format(cr[1],cr[0],nl))
        #     sl(t2)
        #     print('{}  - Alt:          {:.3f}    km{}'.format(cr[7],(data[2]/km),cr[0]))
        #     sl(t1)
        #     print('  - Range:        {:.3f}    km'.format(data[3]/km))
        #     sl(t1)
        #     print('  - Speed:        {:.3f}  m/s'.format(data[4]))
        #     sl(t1)
        #     print('  - Delta V Used: {:.3f}  m/s'.format(data[5]))
        #     sl(t1)
        #     print('  - Time (Raw):   {:.3f}   s'.format(data[6]))
        #     sl(t1)
        #     print('{}  - Time:         {}{}'.format(cr[6],time,cr[0]))
        #     sl(t1)
        #     print('  - Row:          {:.0f}{}'.format(data[7],nl))
        
        # # no boosters equipped
        # else:
        mile_nan[0]=mile[2]
        data=tuple(mile_nan)
            
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))

        raw.append(data)
        war.append(data)

        return(data)

    def meco(): # 3

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

        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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
    
    def fair(): # 4

        # no fairinf on test file 1
        # no fairing on test file 2
        # fairing on test file 3

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

            # clock time generation
            td=pd.to_timedelta(rwt,'sec')
            mm=int(td.components.minutes)
            ss=int(td.components.seconds)
            ms=round(int(td.components.milliseconds), 3)
            time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

            data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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
            mile_nan[0]=mile[3]
            data=tuple(mile_nan)
            
        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)

    def seco(): # 5
        
        if(user[0]==True):
            x=3
        else:
            x=2

        val=df[v_roc==vrr[x]]

        mist=mile[6]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)
        
        raw.append(data)
        war.append(data)

        return(data)

    def oib():  # 6

        rate=v_roc.index
        
        x=0
        a=war[5][7]
        b=(rate[-1]-(rate[-1]*0.25))

        lst=[]
        for i in v_roc:
            c=rate[x]
            if(1<i!=0)&(a<c<b):
                lst.append(i)
            x+=1
        
        point=df[v_roc.isin(lst)][col18]
        value=point.iloc[0]
        val=df[v_roc==value]
        
        mist=mile[7]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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
    
    def oibeco(): # 7

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

        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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

    def pay(): # 8

         # no payload release on test file
        
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

        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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

        SQL=(f'''INSERT INTO {schema_table} VALUES %s;''')
        cur.execute(SQL,(data,))
        sl(t3)

        raw.append(data)
        war.append(data)

        return(data)
    
    def fin(): # 9

        # end of file
        tim=df[col0]

        val=df[tim==tim.iloc[-1]]

        mist=mile[10]
        alt=val.iloc[mile_pos[0]]    
        rng=val.iloc[mile_pos[1]]
        spd=val.iloc[mile_pos[2]]
        mas=val.iloc[mile_pos[3]]
        dtv=val.iloc[mile_pos[4]]
        rwt=val.iloc[mile_pos[5]]
        row=val.index[mile_pos[6]]

        # clock time generation
        td=pd.to_timedelta(rwt,'sec')
        mm=int(td.components.minutes)
        ss=int(td.components.seconds)
        ms=round(int(td.components.milliseconds), 3)
        time=('{:02d}:{:02d}.{:03d}'.format(mm,ss,ms))

        data=tuple([mist,alt,rng,spd,mas,dtv,rwt,int(row),nan])

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
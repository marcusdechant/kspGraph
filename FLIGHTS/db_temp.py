from ColorOut import CO
from psqlConn import psql # conn=psql()
import pandas as pd
import numpy as np
import psycopg2 as psy

CO # colour out

def color():

    normal='\033[0;37m'                     # cr[0]
    red_bold='\033[1;31m'                   # cr[1]
    yel_back_bold='\033[1;37;43m'           # cr[2]
    green='\033[0;32m'                      # cr[3]

    color=[normal,red_bold,yel_back_bold,green]

    return(color)

# data processing and database insertion

nl='\n'
km=1000

file='Cache_Alpha'                                                                    # faux input
# file=input('{}Flight Name?                                     : '.format(nl))     # user specific flight name (name of telmu csv data)
fn=file.split("_")
lpad='KSC Pad 1'                                                                     # faux input
# lpad=input('Launch Complex Pad?                              : ')                  # user defines what launch comlex and pad where used                                                                                            
bstc='y'                                                                        # faux input
# bstc=input('Did the Launch Vehicle have Boosters? (y/n)      : ')                  # user defines if the launch vehicle had boosters
payc='n'                                                                        # faux input
# payc=input('Did the Launch Vehicle release a Payload? (y/n)  : ')                  # user defines if launch vehicle staged payload
fopt='n'                                                                        # faux input
# fopt=input('Did the Launch Vehicle release a Payload? (y/n)  : ')                  # user defines if launch vehicle staged payload

raw=[]

def source():


    df=pd.read_csv(r'Cache\old\{}.csv'.format(file))    # extracts telmu csv to dataframe
    col=df.columns.values                               # defines columns as col[0,1,2,3,n]
    # print('{}{}{}'.format(nl,col,nl))                 # column name print out
    name=['TSM','STG','ASL','DRG','SFV','ORV',  
          'MASS','ACC','Q','AOA','AOS','AOD',
          'TRU','PIT','GLS','DLS','SLS','DVE']          # shortened column names
    a=0       
    while a<len(col):                                       # chnage column names to shorter names
        col[a]=name[a]
        a+=1
    # print('{}{}{}'.format(nl,col,nl))                 # new column name print out

    return(col,df)

def database():
    conn=psql()
    return(conn)

def roc():

    (col,df)=source()                       # return columns and dataframe from source()

    t1=['sROC','mRoC','aRoC']               # titles list
    a=0                                     # columns iterative starting point
    b=0                                     # titles iterative starting point
    c=0
    for i in col:                           # for loop through colummns
        if (i==col[1] or i==col[6] or i==col[7]):            # if i equals 1 (stages), 6 (mass), 7 acceleration
            ser=df[col[a]]                  # based on the iteration find the equivalent column
            num=[]                          # 
            for f in ser:                   # loop through each row in column
                num.append(f)               # isolate metrics from each row into a list
            ic=pd.Series(num)               # place isolated metrics into a series
            roc=ic.pct_change()             # find the rate of change of the series    
            roc=roc.fillna(0)               # replace any (nan) as (0)
            title=t1[b]                     # b will select the correct title from titles list
            idf=pd.DataFrame({title:roc})   # place the rate of change series into an isolated dataframe
            df=df.join(idf,how='right')     # join isolated dataframe to then original dataframe
            b+=1                            # titles iterative value
        a+=1                                # columns iterative value
    col=df.columns.values
    for f in col:
        if(f==t1[0] or f==t1[1] or f==t1[2]):
            rate=df[col[c]]
            rate.replace([np.inf,(-np.inf)],np.nan,inplace=True)
            rate.fillna(0,inplace=True)
        c+=1
    
    return(col,df)

def flight_info():

    cr=color()
    (col,df)=roc()
    conn=database()

    a=(-1)
    b=1
    c=0
    d=[]
    lst=[0,2,3,5,6,17]
    for e in col:
        for itm in lst:
            if(c==itm):
                if c==6:
                    ic=col[itm]
                    iv=df[ic].iloc[b]
                    d.append(iv)
                    fc=col[itm]
                    fv=df[fc].iloc[a]
                    d.append(fv)
                else:
                    f=col[itm]
                    v=df[f].iloc[a]
                    d.append(v)
        c+=1

    options=[bstc,payc,fopt]
    opts=[]
    for o in options:
        if(o=='y'):
            opt=True
            opts.append(opt)
        else:
            opt=False
            opts.append(opt)

    flin={'name'    :[file],
          'pad'     :[lpad],
          'time'    :[d[0]],               # total amount of time flight took in seconds
          'altitude':[d[1]],               # final altitude of flight in meters
          'range'   :[d[2]],               # final down range postiton of rocket
          'speed'   :[d[3]],               # final orbital speed of flight
          'lv_mass' :[d[4]],               # launch vehicle mass
          'pay_mass':[d[5]],               # payload mass
          'delta_v' :[d[6]],
          'bst_q'   :[opts[0]],
          'pay_q'   :[opts[1]],
          'fair_q'  :[opts[2]]}
    
    fl=pd.DataFrame(flin)                           # build dataframe for flight
    fol=fl.columns.values
    data=[file,
          lpad,
          fl[fol[2]][0],
          fl[fol[3]][0],
          fl[fol[4]][0],
          fl[fol[5]][0],
          fl[fol[6]][0],
          fl[fol[7]][0],
          fl[fol[8]][0],
          opts[0],
          opts[1],
          opts[2]]
    
    cur=conn.cursor()
    
    sch=['callisto','hailmary']
    if(fn[0]=='Callisto' or fn[0]=='Cache'):
        schema=(sch[0])
    if(fn[0]=='HM' or fn[0]=='Hailmary'):
        schema=(sch[1])
    table='test2'
    # table='flights'
    schema_table=('{}.{}'.format(schema,table))

    SQL=('''INSERT INTO {} VALUES ({})'''.format(schema_table,data))
    # cur.execute(SQL)

    time=pd.to_timedelta(data[2],'sec')
    mm=time.components.minutes
    ss=time.components.seconds
    ms=time.components.milliseconds
    time=('{}:{}.{}'.format(mm,ss,ms))

    flnm=('{} {}'.format(fn[0],fn[1]))

    print('{}{} {} Launch Telemetry {}{}'.format(nl,cr[2],flnm,cr[0],nl))
    print('{} Flight Information{}{}'.format(cr[1],cr[0],nl))
    print('  - Flight Name:              {}'.format(flnm))
    print('  - Launch Complex/Pad:       {}'.format(data[1]))
    print('  - Flight Time:              {}'.format(time))
    print('  - Flight Time (Raw):        {:.3f}  s'.format(data[2]))
    print('  - Final Altitude:           {:.3f}   km'.format(data[3]/km))
    print('  - Downrange:                {:.3f}  km'.format(data[4]/km))
    print('  - Orbital Velocity:         {:.3f}  m/s'.format(data[5]))
    print('  - Launch Vehicle Mass:      {:.3f}   t'.format(data[6]))
    print('  - Payload Mass:             {:.3f}    t'.format(data[7]))
    print('  - Launch Vehicle DeltaV:    {:.0f}      m/s{}'.format(data[8],nl))
    
    print('Raw Data: {}{}'.format(data,nl))

    print('SQL Query: {}{}'.format(SQL,nl))

    # print('DataFrame{}{}{}'.format(nl,fl,nl))

    raw.append(data)

    return(col,df,fol,fl,raw,conn,cur)

def flight_data():

    (col,df,fol,fl,raw,conn,cur)=flight_info()
    cr=color()
    
    fol4=fl[fol[4]]                               # booster option flight_info column 4
    fol5=fl[fol[5]]                               # payload option flight_info column 5
    fol6=fl[fol[6]]
    
    opts=[fol4.iloc[0],fol5.iloc[0],fol6.iloc[0]] # options list for easy passing - opts[0,1]

    stg=df[col[18]]                             # stage rate of change dataframe column 18

    stgs=[]                                       # list for isolated stage rate of change values
    for i in stg:                                 # 
        if(i!=0):                                 # stages in stage rate of change as non zero nunbers
            stgs.append(i)                        # add any non zero number to the list for comparison - stgs[a,b]
    
                                           # 1000m/1000 = 1km

    def maxq():
        
        Q=df[col[8]]                                # Q or dynamic pressure dataframe column 8

        maxQ=max(Q)                                 # max Q or maximum dynamic pressure is the highest value in the Q column
        alt=df[Q==maxQ].iloc[0,2]                   # data[2] - Altitude
        rng=df[Q==maxQ].iloc[0,3]                   # data[3] - Down Range
        spd=df[Q==maxQ].iloc[0,5]                   # data[4] - Speed
        rawt=df[Q==maxQ].iloc[0,0]                  # data[1] - time
        row=df[Q==maxQ].index[0]                    # data[5] - Row
        time_delta=pd.to_timedelta(rawt, "sec")     # convert raw time to a time delta
        ss=int(time_delta.components.seconds)       # isolate seconds
        ms=int(time_delta.components.milliseconds)  # isolate minutes
        time=('{}.{}'.format(ss,ms))                # combine isolated metrics
        data=[maxQ,alt,rng,spd,rawt,row]                                     # data[0,1,2,3,4]
        print('{}Maximum Dynamic Pressure{}'.format(cr[1],cr[0]))            # milestone 1 - MaxQ 
        print('{} - MaxQ:         {:.2f} Pa{}'.format(cr[3],data[0],cr[0]))  # max dynamic pressure in Pa
        print(' - Alt:          {:.3f}    km'.format(data[1]/km))            # altitude in km
        print(' - Range:        {:.3f}    km'.format(data[2]/km))            # downrange in km
        print(' - Speed:        {:.3f}  m/s'.format(data[3]))                # orbital speed in m/s
        print(' - Time:         {}'.format(time))                        # formatted time
        print(' - Time (Raw):   {:.3f}   s'.format(data[4]))                 # raw time in seconds
        print(' - Row:          {:.0f}{}'.format(data[5],nl))                # row in dataframe
        print('Raw Data: {}{}'.format(data,nl))                              # raw data print out

        return(data)

    def throttle_up():

        # not on test file

        time=df[col[0]]                
        rate=df[col[20]]

        a=35
        b=50
        go=[]
        for i in time:
            if(i>a and i<b):
                go.append(i)
        try:
            up=df[time.isin(go)][rate]
            th=max(up)
            alt=df[rate==th].iloc[0,2]
            rng=df[rate==th].iloc[0,3]
            spd=df[rate==th].iloc[0,5]
            time=df[rate==th].iloc[0,0]
            row=df[rate==th].index[0]
            data=[alt,rng,spd,time,row]
            print('{}Throttle Up{}'.format(cr[1],cr[0]))
            print(' - Alt:    {:.3f} km'.format(data[0]/km))
            print(' - Range:  {:.3f} km'.format(data[1]/km))
            print(' - Speed:  {:.3f} m/s'.format(data[2]))
            print(' - Time:   {:.2f} (ss.ms)'.format(data[3]))
            print(' - Row:    {:.0f}{}'.format(data[4],nl))
            print('Raw Data: {}{}'.format(data,nl))
        except(KeyError):
            data=[None,None,None,None,None]
            print('{}No Throttle Up Found{}{}'.format(cr[1],cr[0],nl))
            print('Raw Data: {}{}'.format(data,nl))

        return(data)

    def booster_engine_cut_off():
        if(opts[0]=='y'):
            a=stgs[0]
            alt=df[stg==a].iloc[0,2]
            rng=df[stg==stgs[0]].iloc[0,3]
            spd=df[stg==stgs[0]].iloc[0,5]
            tim=df[stg==stgs[0]].iloc[0,0]
            row=df[stg==stgs[0]].index[0]
            data=[alt,rng,spd,tim,row]
            print('{}Booster Separation{}'.format(cr[1],cr[0]))
            print(' - Alt:    {:.3f} km'.format(data[0]/km))
            print(' - Range:  {:.3f} km'.format(data[1]/km))
            print(' - Speed:  {:.3f} m/s'.format(data[2]))
            print(' - Time:   {:.2f} s'.format(data[3]))
            print(' - Row:    {:.0f}{}'.format(data[4],nl))
            print('Raw Data: {}{}'.format(data,nl))
        else:
            data=[None,None,None,None,None]
            print('{}No Boosters on Launch Vehicle{}{}'.format(cr[1],cr[0],nl))
            print('Raw Data: {}{}'.format(data,nl))
            
        return(data)

    def main_engine_cut_off():

        if opts[0]=='n':
            opt=stgs[0]
        if opts[0]=='y':
            opt=stgs[1]
        alt=df[stg==opt].iloc[0,2]
        rng=df[stg==opt].iloc[0,3]
        spd=df[stg==opt].iloc[0,5]
        tim=df[stg==opt].iloc[0,0]
        row=df[stg==opt].index[0]
        data=[alt,rng,spd,tim,row]
        print('{}First Stage Separation{}'.format(cr[1],cr[0]))
        print(' - Alt:    {:.3f} km'.format(data[0]/km))
        print(' - Range:  {:.3f} km'.format(data[1]/km))
        print(' - Speed:  {:.3f} m/s'.format(data[2]))
        print(' - Time:   {:.2f} s'.format(data[3]))
        print(' - Row:    {:.0f}{}'.format(data[4],nl))
        print('Raw Data: {}{}'.format(data,nl))

        return(data)

    def fairing_sept():

        # not on test file, issues testing due to lack of reference

        # change in mass

        # if opts[2]=='y':
        #     alt=df[col[2]]
        #     rate=df[col[19]]
        #     print(rate)

        #     range=[]
        #     for i in alt:
        #         if (i>=65000)&(i<=75000):
        #             range.append(i)
            
        #     try:
        #         sept=df[alt.isin(range)][rate]
        #         print(sept)

        #     except(KeyError):
        #         exit()
        # else:

        data=[None,None,None,None,None]
        print('{}No Fairing on Launch Vehicle{}{}'.format(cr[1],cr[0],nl))
        print('Raw Data: {}{}'.format(data,nl))
    
    def second_engine_cut_off():

        speed=df[col[5]]

        s=max(speed)
        alt=df[speed==s].iloc[0,2]
        rng=df[speed==s].iloc[0,3]
        spd=df[speed==s].iloc[0,5]
        tim=df[speed==s].iloc[0,0]
        row=df[speed==s].index[0]
        data=[alt,rng,spd,tim,row]
        print('{}Second Stage Engine Cut Off{}'.format(cr[1],cr[0]))
        print(' - Alt:    {:.3f} km'.format(data[0]/km))
        print(' - Range:  {:.3f} km'.format(data[1]/km))
        print(' - Speed:  {:.3f} m/s'.format(data[2]))
        print(' - Time:   {:.2f} s'.format(data[3]))
        print(' - Row:    {:.0f}{}'.format(data[4],nl))
        print('Raw Data: {}{}'.format(data,nl))

        return(data)

    def orbital_insertion_burn():

        col20=col[20]
        rate=df[col20]

        a=0
        b=1300
        c=10
        range=[]
        for i in rate:
            if(i!=0):
                if(rate.index[a]>b):
                    if(abs(i)>c):          
                        range.append(i)
            a+=1
        mrt=df[rate.isin(range)][col20]
        rt=max(mrt)
        alt=df[rate==rt].iloc[0,2]
        rng=df[rate==rt].iloc[0,3]
        spd=df[rate==rt].iloc[0,5]
        tim=df[rate==rt].iloc[0,0]
        row=df[rate==rt].index[0]
        data=[alt,rng,spd,tim,row]
        print('{}Orbital Insertion Burn{}'.format(cr[1],cr[0]))
        print(' - Alt:    {:.3f} km'.format(data[0]/km))
        print(' - Range:  {:.3f} km'.format(data[1]/km))
        print(' - Speed:  {:.3f} m/s'.format(data[2]))
        print(' - Time:   {:.2f} s'.format(data[3]))
        print(' - Row:    {:.0f}{}'.format(data[4],nl))
        print('Raw Data: {}{}'.format(data,nl))

        return(data)

    maxQ=maxq()
    maxQ
    thUp=throttle_up()
    thUp
    beco=booster_engine_cut_off()
    beco
    meco=main_engine_cut_off()
    meco
    fair=fairing_sept()
    fair
    seco=second_engine_cut_off()
    seco
    oib=orbital_insertion_burn()
    oib

if __name__=="__main__":
    flight_data()


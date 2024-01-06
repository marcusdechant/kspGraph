from ColorOut import CO
import pandas as pd
import numpy as np
import psycopg2 as psy

CO # colour out

def color():

    normal='\033[0;37m'             # cr[0]
    red_bold='\033[1;31m'           # cr[1]
    yel_back_bold='\033[1;37;43m' # cr[2]
    green='\033[0;32m'              # cr[3]

    color=[normal,red_bold,yel_back_bold,green]

    return(color)

# data processing for ksp flights

nl='\n' # new line var

# file=input('{}Flight Name?                                : '.format(nl)) # user specific flight name (name of telmu csv data)
# bstc=input('Did the Launch Vehicle have Boosters?       : ')              # user defines if the launch vehicle had boosters
# payc=input('Did the Launch Vehicle release a Payload?   : ')              # user defines if launch vehicle staged payload
# lpad=input('Launch Complex Pad?                         : ')              # user defines what launch comlex and pad where used

file='CacheAlpha'

def source():

    csv=(r'Cache\old\{}.csv'.format(file))          # extracts telmu csv to dataframe
    df=pd.read_csv(csv)                             # extracts telmu csv to dataframe
    col=df.columns.values                           # defines columns as col[0,1,2,3,n]
    # print('{}{}{}'.format(nl,col,nl))             # column name print out
    name=['TSM','STG','ASL','DRG','SFV','ORV',  
          'MASS','ACC','Q','AOA','AOS','AOD',
          'TRU','PIT','GLS','DLS','SLS','DVE']      # shortened column names
    a=0                 
    for i in col:                                   # chnage column names to shorter names
        col[a]=name[a]
        a+=1
    # print('{}{}{}'.format(nl,col,nl))             # new column name print out

    return(col,df)

def rates_of_change():

    (col,df)=source()                       # return columns and dataframe from source()
    titles=['sROC','mRoC','aRoC']           # titles list
    a=0                                     # columns iterative starting point
    b=0                                     # titles iterative starting point
    for i in col:                           # for loop through colummns
        if a==1 or a==6 or a==7:            # if i equals 1 (stages), 6 (mass), 7 acceleration
            c=(col[int(a)])                 # based on the iteration find the equivalent column
            ser=df[c]                       # open the series based on the column
            num=[]                          # 
            for f in ser:                   # loop through each row in column
                num.append(f)               # isolate metrics from each row into a list
            iser=pd.Series(num)             # place isolated metrics into a series
            roc=iser.pct_change()           # find the rate of change of the series    
            roc=roc.fillna(0)               # replace any (nan) as (0)
            title=titles[b]                 # b will select the correct title from titles list
            idf=pd.DataFrame({title:roc})   # place the rate of change series into an isolated dataframe
            df=df.join(idf,how='right')     # join isolated dataframe to then original dataframe
            b+=1                            # titles iterative value
        a+=1                                # columns iterative value
    col=df.columns.values

    return(col,df)

def initial_flight_data():

    cr=color()
    (col,df)=rates_of_change()              # use col and df from roc as it is updated
    flight_time=df[col[0]].iloc[-1]         # total amount of time flight took in seconds
    total_mass=df[col[6]].iloc[1]           # starting rocket mass
    final_mass=df[col[6]].iloc[-1]          # final rocket mass
    bstc='y'            # faux input
    payc='n'            # faux input
    lpad='KSC Pad 1'    # faux input
    # flight dataframe column titles 
    hn=['FLIGHT','TTIME','iMASS','FMASS','BSTQ','PAYQ','LPAD']
    # flight dataframe data for specific flight
    h=[file,flight_time,total_mass,final_mass,bstc,payc,lpad]
    dfin={hn[0]:[h[0]],
          hn[1]:[h[1]],
          hn[2]:[h[2]],
          hn[3]:[h[3]],
          hn[4]:[h[4]],
          hn[5]:[h[5]],
          hn[6]:[h[6]]}                     # dataframe series for flight
    fl=pd.DataFrame(dfin)                   # build dataframe for flight
    fl.to_csv(r'Flights.csv',index=False)   # add dataframe to flight csv (eventually postgres)
    raw_min=flight_time/60
    mins=int(raw_min)
    raw_sec=abs(raw_min-mins)
    sec=raw_sec*60
    print('{}{}{} Launch Telemetry{}{}'.format(nl,cr[2],file,cr[0],nl))
    print('{}Flight Info{}'.format(cr[1],cr[0]))
    print(' - Flight Time:           {}:{:.2f} (mm:ss.ms)'.format(mins,sec))
    print(' - Launch Vehicle Mass:   {:.0f} t'.format(total_mass))
    print(' - Payload Mass:          {:.2f} t'.format(final_mass))
    print(' - Launch Complex/Pad:    {}{}'.format(lpad,nl))
    print('Raw Data: {}{}'.format(h,nl))
    # print('DataFrame{}{}{}'.format(nl,fl,nl))

    return(col,df,fl)

def plots():

    (col,df,fl)=initial_flight_data()
    cr=color()

    qa=[fl[fl.columns[4]].iloc[0],fl[fl.columns[5]].iloc[0]]

    km=1000
    stg=df[col[18]]
    stgs=[]
    for i in stg:
        if(i!=0):
            stgs.append(i)

    def maxq():
        
        Q=df[col[8]]
        maxQ=max(Q)
        alt=df[Q==maxQ].iloc[0,2]       # data[2] - Altitude
        rng=df[Q==maxQ].iloc[0,3]       # data[3] - Down Range
        spd=df[Q==maxQ].iloc[0,5]       # data[4] - Speed
        time=df[Q==maxQ].iloc[0,0]      # data[1] - time
        row=df[Q==maxQ].index[0]        # data[5] - Row
        data=[maxQ,alt,rng,spd,time,row]
        print('{}Maximum Dynamic Pressure{}'.format(cr[1],cr[0]))
        print('{} - MaxQ:   {:.2f} Pa{}'.format(cr[3],data[0],cr[0]))
        print(' - Alt:    {:.3f} km'.format(data[1]/km))
        print(' - Range:  {:.3f} km'.format(data[2]/km))
        print(' - Speed:  {:.3f} m/s'.format(data[3]))
        print(' - Time:   {:.2f} s'.format(data[4]))
        print(' - Row:    {:.0f}{}'.format(data[5],nl))
        print('Raw Data: {}{}'.format(data,nl))

        return(data)

    def throttle_up():

        time=df[col[0]]                
        rate=df[col[20]]
        go=[]
        for a in time:
            if(a>35)and(a<50):
                go.append(a)
        try:
            up=df[time.isin(go)][rate]
            th=max(up)
            alt=df[rate==th].iloc[0,2]      #& data[0] - Altitude 
            rng=df[rate==th].iloc[0,3]      #& data[1] - Down Range
            spd=df[rate==th].iloc[0,5]      #& data[2] - Speed
            time=df[rate==th].iloc[0,0]     #& data[1] - Time
            row=df[rate==th].index[0]       #& data[5] - Row
            data=[alt,rng,spd,time,row]
            print('{}Throttle Up{}'.format(cr[1],cr[0]))
            print(' - Alt:    {:.3f} km'.format(data[0]/km))
            print(' - Range:  {:.3f} km'.format(data[1]/km))
            print(' - Speed:  {:.3f} m/s'.format(data[2]))
            print(' - Time:   {:.2f} (ss.ms)'.format(data[3]))
            print(' - Row:    {:.0f}{}'.format(data[4],nl))
            print('Raw Data: {}{}'.format(data,nl))
        except(KeyError):
            print('{}No Throttle Up Found{}{}'.format(cr[1],cr[0],nl))
            data=[None,None,None,None,None]

        return(data)

    def booster_engine_cut_off():
        
        if (qa[0])=='y':
            alt=df[stg==stgs[0]].iloc[0,2]      #& data[0] - Altitude 
            rng=df[stg==stgs[0]].iloc[0,3]      #& data[1] - Down Range
            spd=df[stg==stgs[0]].iloc[0,5]      #& data[2] - Speed
            tim=df[stg==stgs[0]].iloc[0,0]      #& data[1] - Time
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
            print('{}No Boosters on Launch Vehicle{}{}'.format(cr[1],cr[0],nl))
            data=[None,None,None,None,None]

        return(data)

    def main_engine_cut_off():

        if qa[0]=='n':
            opt=stgs[0]
        if qa[0]=='y':
            opt=stgs[1]
        alt=df[stg==opt].iloc[0,2]      #& data[0] - Altitude 
        rng=df[stg==opt].iloc[0,3]      #& data[1] - Down Range
        spd=df[stg==opt].iloc[0,5]      #& data[2] - Speed
        tim=df[stg==opt].iloc[0,0]      #& data[1] - Time
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

    maxQ=maxq()
    maxQ
    thUp=throttle_up()
    thUp
    beco=booster_engine_cut_off()
    beco
    meco=main_engine_cut_off()
    meco

if __name__=="__main__":
    plots()
exit()


import pandas as pd
import numpy as np

def ColOut():

    # makes exception hook colorful and easy to read. super helpful.

    import sys
    from IPython.core.ultratb import ColorTB

    sys.excepthook=ColorTB() 

ColOut()

# global variables
nl='\n' # new line escape
km=1000 # 1000m = 1km (use for output convertion)


# user input 
# csv=input(f'{nl}Flight Name?         : ')
csv='CAT1'

def color():

    normal='\033[0;37m'             # cr[0]
    red_bold='\033[1;31m'           # cr[1]
    yel_back_bold='\033[1;37;43m' # cr[2]
    green='\033[0;32m'              # cr[3]

    color=[normal,red_bold,yel_back_bold,green]

    return(color)

def source():

    # file paths/sources for both computers
    # C:\Users\marcu\Desktop\KSPworkshop\
    # C:\Users\Marcus\Desktop\KSPplotted\
    df=pd.read_csv(r'C:\Users\Marcus\Desktop\KSPplotted\kspGraph\FLIGHTS\HM\{}.csv'.format(csv)) # open flight telemetry csv
    
    col=df.columns.values # displays csv columns
    # print(f'{nl}{col}{nl}')

    return(df)

def stage():

    # identify stage separation points

    df=source()

    col1=(df.columns[1])                # col1 = stages column
    ser=df[col1]                        # s1s = stage 1 separation = stages column series

    stages=[]                           # using stage metrics we can find main stage seperation.
    for stgs in ser:                    # 
        stages.append(stgs)             # seperate (stg) for whole (df) into a list
    stage=pd.Series(stages)             # place (stg) in isolated series
    roc=stage.pct_change()              # find rate of change (roc) for (stg)
    stgTitle='stgRoC'
    stg=pd.DataFrame({stgTitle:roc})    # create an isolated (df) for (roc)
    df=df.join(stg,how='right')         # join (roc) to original (df)

    stgSept=(df[stgTitle])              # define (df) (stg) (roc) column
    
    stgList=[]
    for i in stgSept.iloc[1:]:          # ignoring the first, list all entries in (stg) (roc) column 
        if(i!=0):                       # ignore all that equal 0
            stgList.append(i)           # add every one that did not equal 0 to a list
    
    return(stgList,stgSept,df)          # cont. in (main engine cut off) and (fairing separation)

def acc_rate_of_change():

    df=source()

    acc=(df.columns[7])             #& acceleration column
    a_df=df[acc]                    #& a = acceleration series
    time=(df.columns[0])            #& time column
    TIME=df[time]                   #& t = time series


    a_list=[]                               #! to find the point of throttle up we need to find the acceleration rate of change.
    for a in a_df:                   
        a_list.append(a)                    #? isolate (a) for whole (df) into a list
    a_ser=pd.Series(a_list)                 #? place (a) in isolated series
    a_roc=a_ser.pct_change()                #? find rate of change (roc) for (a)
    roc_title='aRoC'
    roc_df=pd.DataFrame({roc_title:a_roc})  #? create an isolated (df) for (roc)
    df=df.join(roc_df,how='right')          #? join (roc) to original df

    return(df)

def max_q():

    # finding the point of maximum dynamic pressure or Q. The first mile stone of flight. throttling to full before this point is wasting energy by working too hard.
    # we want a slow moderate push through the thickest atmosphere. after this point the atmosphere begins to thin and the rocket experinces less dynamic pressure.

    cr=color()                      # cr[0,1,2,3]
    df=source()                     # df = dataframe

    col8=(df.columns[8])            # Q column
    Q=df[col8]                      # Q = Dynamic Pressure

    # finding point of max Q

    maxQ=max(Q)                     # data[0] - maxQ - the maximum value in the Q column

    # finding the matching values in all other relevant columns using max Q as reference

    alt=df[Q==maxQ].iloc[0,2]       # data[2] - Altitude
    rng=df[Q==maxQ].iloc[0,3]       # data[3] - Down Range
    spd=df[Q==maxQ].iloc[0,5]       # data[4] - Speed
    time=df[Q==maxQ].iloc[0,0]      # data[1] - time
    row=df[Q==maxQ].index[0]        # data[5] - Row
    
    # place values in a list

    data=[maxQ,alt,rng,spd,time,row]

    # print out of values for user

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

    # finding point of throttle up during inital ascent. Occurs shortly after Max Q around 35 to 50 seconds into fligt.
    
    cr=color()                      #& cr[0,1,2,3]
    df=acc_rate_of_change()         #& df = dataframe

    time=(df.columns[0])            #& time column
    TIME=df[time]                   #& t = time series

    # define rate of change column like acceleration and time.

    rate=(df.columns[18])           #& rate of change columnn
    RATE=df[rate]                   #& roc = rate of change series

    # finding point of throttle up (p_th)

    go=[]                           #! a signifigant change in acceleration from 35 ot 50 seconds into flight indicates the point of throttle up.
    for a in TIME:                  
        if(a<50)&(a>35):            #? time range for throttle up
            go.append(a)            #? isolate time range into list (go)
    up=df[TIME.isin(go)][rate]      #? using the time range find the matching (roc) range
    th=max(up)                      #? the maximum value to the (roc) range during the time range is the point of throttle up

    # finding the matching values in all other relevant columns using point of throttle up as reference

    alt=df[RATE==th].iloc[0,2]      #& data[0] - Altitude 
    rng=df[RATE==th].iloc[0,3]      #& data[1] - Down Range
    spd=df[RATE==th].iloc[0,5]      #& data[2] - Speed
    time=df[RATE==th].iloc[0,0]     #& data[1] - Time
    row=df[RATE==th].index[0]       #& data[5] - Row
    
    # place values in a list

    data=[alt,rng,spd,time,row]

    # print out of values for user

    print('{}Throttle Up{}'.format(cr[1],cr[0]))
    print(' - Alt:    {:.3f} km'.format(data[0]/km))
    print(' - Range:  {:.3f} km'.format(data[1]/km))
    print(' - Speed:  {:.3f} m/s'.format(data[2]))
    print(' - Time:   {:.2f} s'.format(data[3]))
    print(' - Row:    {:.0f}{}'.format(data[4],nl))
    print('Raw Data: {}{}'.format(data,nl))

    return(data)
    
def main_engine_cut_off():

    # main engine cut off or (meco) is defined in 3 steps
    # meco = main engine cut off
    # stg1 = first stage seperation
    # sses = second stage engine start
    # these three events occur in a short time frame.

    cr=color()                      #& cr[0,1,2,3]
    (n,stgSept,df)=stage()

    alt=df[stgSept==n[0]].iloc[0,2]      #& data[0] - Altitude 
    rng=df[stgSept==n[0]].iloc[0,3]      #& data[1] - Down Range
    spd=df[stgSept==n[0]].iloc[0,5]      #& data[2] - Speed
    tim=df[stgSept==n[0]].iloc[0,0]      #& data[1] - Time
    row=df[stgSept==n[0]].index[0]       #& data[5] - Row
    
    # place values in a list

    data=[alt,rng,spd,tim,row]

    # print out of values for user

    print('{}Main Engine Cut Off (MECO){}'.format(cr[1],cr[0]))
    print(' - Alt:    {:.3f} km'.format(data[0]/km))
    print(' - Range:  {:.3f} km'.format(data[1]/km))
    print(' - Speed:  {:.3f} m/s'.format(data[2]))
    print(' - Time:   {:.2f} s'.format(data[3]))
    print(' - Row:    {:.0f}{}'.format(data[4],nl))
    print('Raw Data: {}{}'.format(data,nl))
        
    return(data)

def fairing_separation():

    # main engine cut off or (meco) is defined in 3 steps
    # meco = main engine cut off
    # stg1 = first stage seperation
    # sses = second stage engine start
    # these three events occur in a short time frame.

    cr=color()                      #& cr[0,1,2,3]
    (n,stgSept,df)=stage()

    alt=df[stgSept==n[1]].iloc[0,2]      #& data[0] - Altitude 
    rng=df[stgSept==n[1]].iloc[0,3]      #& data[1] - Down Range
    spd=df[stgSept==n[1]].iloc[0,5]      #& data[2] - Speed
    tim=df[stgSept==n[1]].iloc[0,0]      #& data[1] - Time
    row=df[stgSept==n[1]].index[0]       #& data[5] - Row
    
    # place values in a list

    data=[alt,rng,spd,tim,row]

    # print out of values for user

    print('{}Fairing Separation{}'.format(cr[1],cr[0]))
    print(' - Alt:    {:.3f} km'.format(data[0]/km))
    print(' - Range:  {:.3f} km'.format(data[1]/km))
    print(' - Speed:  {:.3f} m/s'.format(data[2]))
    print(' - Time:   {:.2f} s'.format(data[3]))
    print(' - Row:    {:.0f}{}'.format(data[4],nl))
    print('Raw Data: {}{}'.format(data,nl))
        
    return(data)

cr=color()

print('{}{}{} Launch Telemetry{}{}'.format(nl,cr[2],csv,cr[0],nl))

maxq=max_q()                # maxq[0,1,2,3,4,5]
maxq

thup=throttle_up()          # thup[0,1,2,3,4]
thup

meco=main_engine_cut_off()
meco

fair=fairing_separation()
fair

exit()


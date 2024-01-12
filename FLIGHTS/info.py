from ColorOut import CO
from doNotGit.psqlConn import psql
import pandas as pd
import numpy as np

CO()
conn=psql()
nl='\n'
km=1000
raw=[]

def color():

    normal='\033[0;37m'                     # cr[0]
    red_bold='\033[1;31m'                   # cr[1]
    yel_back_bold='\033[1;37;43m'           # cr[2]
    green='\033[0;32m'                      # cr[3]

    color=[normal,red_bold,yel_back_bold,green]

    return(color)

# data processing for ksp

def user():

    # ksp launch vehicle class based naming convention 
    # (class_mission_number)
    # where class is the family of launch vehicle, or in cases when multiple missions are grouped together as in the case of orbital construction (ex. hailmary, KOSS).
    # where mission refers to either the individual mission name ex. HM_CORE, HM_CST, Cache Alpha. Some refer to the payload while some are sequential.
    # where number is when payload named missions required more than one launch (ex. HM_Eng_4)
    file='Cache_Alpha'                                                                   # faux input
    # file=input('{}Flight Name?                                     : '.format(nl))     # user specific flight name (name of telmu csv data)
    fn=file.split("_")
    print(fn[0])
    if str.lower(fn[0])=='cache':                                                               
        fn[0]='Callisto'
    print(file)
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

    incl=[file,fn,lpad,bstc,payc,fopt]

    return(incl)

def source():

    incl=user()

    df=pd.read_csv(r'{}\{}.csv'.format(incl[1][0],incl[0]))    # extracts telmetry csv to dataframe
    col=df.columns.values                                      # defines columns as col[0,1,2,3,n]
    # print('{}{}{}'.format(nl,col,nl))                        # column name print out
    name=['TSM','STG','ASL','DRG','SFV','ORV',  
          'MASS','ACC','Q','AOA','AOS','AOD',
          'TRU','PIT','GLS','DLS','SLS','DVE']                 # shortened column names
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
            rate.replace([np.inf,(-np.inf)],np.nan,inplace=True)
            rate.fillna(0,inplace=True)
        c+=1
    
    return(col,df,incl)

def flight_info():

    cr=color()
    (col,df,user)=roc()
    file=user[0]
    fn=user[1]
    pad=user[2]

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
          opts[2]]                          #flight info data
    
    cur=conn.cursor()                               # postgres cursor
    
    # sch=['callisto','hailmary']                     # schema list, based on LV naming conventions the LV class is the database schema
    # if(fn[0]=='Callisto' or fn[0]=='Cache'):        # Callisto or Cache Program Class LVs, similar in size and power, upgraded and refined. Orbital Supply Program.
    #     schema=(sch[0])                             # set correct schema from list
    # if(fn[0]=='HM' or fn[0]=='Hailmary'):           # HM or Hailmary Project Class LVs, differ in size & payload. Orbital Construction Project.
    #     schema=(sch[1])                             # set correct schema from list
    schema='test'                                   # faux schema name
    table='test2'                                   # faux test name
    # table='flights'                                 # flights table differs from the other as its a record of all flights in one program or project
    schema_table=('{}.{}'.format(schema,table))     #set schema name for query

    SQL=('''
         INSERT INTO 
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
                    data[8],data[9],data[10],data[11]))              # sql insert for postgres
    cur.execute(SQL)                                                 # execute postgres insert
     
    time=pd.to_timedelta(data[2],'sec')             # translating raw time (seconds) into clock time for readability
    mm=time.components.minutes                      # split minutes off
    ss=time.components.seconds                      # spilt seconds off
    ms=time.components.milliseconds                 # split milliseconds off
    time=('{}:{}.{}'.format(mm,ss,ms))              # reform

    flnm=('{} {}'.format(fn[0],fn[1]))              # refrom flight name for display

    print('{}{} {} Launch Telemetry {}{}'.format(nl,cr[2],flnm,cr[0],nl))           # flight telemetry readout title
    print('{} Flight Information{}{}'.format(cr[1],cr[0],nl))                       # flight info header 
    print('  - Flight Name:              {}'.format(flnm))                          # flight name
    print('  - Launch Complex/Pad:       {}'.format(data[1]))                       # Launch Complex and Pad
    print('  - Flight Time:              {}'.format(time))                          # total flight time in clock
    print('  - Flight Time (Raw):        {:.3f}  s'.format(data[2]))                # total flight time in seconds
    print('  - Final Altitude:           {:.3f}   km'.format(data[3]/km))           # final orbit in km
    print('  - Downrange:                {:.3f}  km'.format(data[4]/km))            # down range distance in km
    print('  - Orbital Velocity:         {:.4f}    km/s'.format(data[5]/km))        # orbital velocity in km/s    
    print('  - Launch Vehicle Mass:      {:.3f}   t'.format(data[6]))               # launch vehicle mass in tonnes
    print('  - Payload Mass:             {:.3f}    t'.format(data[7]))              # payload mass in tonnes
    print('  - Launch Vehicle DeltaV:    {:.0f}      m/s{}'.format(data[8],nl))     # launch vehicle Delta V (fuel) in m/s
    
    print('Database: ksp.{}'.format(schema_table))                                  # show database
    print('Raw Data: {}{}'.format(data,nl))                                         # show raw data
    # print('SQL: {}{}'.format(SQL,nl))                                               # show SQL insert
    # print('DataFrame{}{}{}'.format(nl,fl,nl))                                       # show dataframe

    raw.append(data)                                                              # append data to raw data list 

if __name__=="__main__":
    flight_info()               # run flight_info()
    conn.commit()               # commit postgres execution
    conn.close()                # close postgres connection once 
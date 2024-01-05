import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as tr

csv=input('Flight Name?     ')
pad=input('Pad Number?      ')
m=int(input('Vehicle Launch Mass?        '))
t=int(input('Flight Time?       '))

#File Path
def readcsv():
    mass=5
    if m>=250:
        mass=10
    time=60
    if t>=600:
        time=90
    if t>=1200:
        time=120
    df=pd.read_csv(r'C:\Users\Marcus\Desktop\KSPplotted\FLIGHTS\Cache\{}.csv'.format(csv))
    X1=   ['TimeSinceMark','T+ (s)',time]                    #X1[0,1,2]
    Ya=   ['SpeedSurface','Surface Speed (m/s)',100]          #Y2[0][0,1,2]
    Yb=   ['SpeedOrbital','Orbital Speed (m/s)',100]          #Y2[1][0,1,2]
    Y1=   ['AltitudeASL','Alt @ SLT (m)',1e4]                 #Y1[0,1,2]
    Y2=   [Ya,Yb]                                             #Y2[0,1][0,1,2]
    Y3=   ['Mass','Mass (t)',mass]                            #Y3[0,1,2]
    Q=    ['Q','Atmospheric Dynamic Pressure (kg/ms^2)',2000] #Q[0,1,2]
    return(df,X1,Y1,Y2,Y3,Q)

def strings():
    
    inputs=[]
    inputs.append('Max Q Row:   ')      #inputs[0]
    inputs.append('BECO Row:    ')      #inputs[1]
    inputs.append('MECO Row:    ')      #inputs[2]
    inputs.append('SECO Row:    ')      #inputs[3]
    inputs.append('TECO Row:    ')      #inputs[4]
    inputs.append('Fairing Row: ')      #inputs[5]
    inputs.append('OIB Row:     ')      #inputs[6]
    inputs.append('OIBECO Row:  ')      #inputs[7]
    inputs.append('Payload Row: ')      #inputs[8]
    inputs.append('Last Row:    ')      #inputs[9]

    section=[]
    section.append('SRBs')                          #section[0]
    section.append('Dynamic Pressure')              #section[1]
    section.append('Maximum Dynamic Pressure')      #section[2]
    section.append('Stage 1')                       #section[3]
    section.append('Stage 2')                       #section[4]
    section.append('Coast')                         #section[5]

    #REMOVE
    section.append('Third Stage Cut Off')           #section[6]
    #REMOVE

    #ADD
    section.append('Fairing Seperation')            #section[7]
    #ADD

    section.append('Orbital Insertion Burn')        #section[8]
    section.append('Space')                         #section[9]

    #ADD
    section.append('Payload Seperation')            #section[10]
    #ADD

    return(inputs,section)

def numbers():
    
    x=2

    (inputs,section)=strings()

    stg=[]
    name=[]
    opt=[]

    #LV_CONFIG_BOOSTER=input(options[0])     # yes
    
    #opt.append(LV_CONFIG_BOOSTER)           #opt[0]
    
    mes=(4-x)                   # 4
    stg.append(mes)             #stg[0]
    name.append(section[0])     #name[0] - Main Engine Start

    maxq=int(input(inputs[0]))  # 207
    stg.append(maxq-x)          #stg[1]
    name.append(section[1])     #name[1] - Dynamic Pressure
    name.append(section[2])     #name[2] - Max Dynamic Pressure
    
    beco=int(input(inputs[1]))  # 217
    stg.append(beco-x)          #stg[2]
    name.append(section[3])     #name[3] - Booster Engine Cut OFf

    meco=int(input(inputs[2]))  # 472
    stg.append(meco-x)          #stg[3]
    name.append(section[4])     #name[4] - Main Engine Cut Off
    
    seco=int(input(inputs[3]))  # 1020
    stg.append(seco-x)          #stg[4]
    name.append(section[5])     #name[5] - Second Engine Cut Off

    oib=int(input(inputs[6]))   # 1842
    stg.append(oib-x)           #stg[5]
    name.append(section[8])     #name[6] - Orbital Insertion Burn

    oibeco=int(input(inputs[7]))# 1916
    stg.append(oibeco-x)        #stg[6]
    name.append(section[9])     #name[7] - Orbit Insert Engine Cut Off

    end=int(input(inputs[9]))   # 2167
    stg.append(end-x)           #stg[7]

    return(stg,name,opt)

def color():
    #Flight Section Colours
    clr=[]
    clr.append('#00bf00') #green    clr[0]
    clr.append('#007fff') #blue     clr[1]
    clr.append('#ff0000') #red      clr[2]
    clr.append('#000000') #black    clr[3]
    clr.append('#daa520') #gold     clr[4]
    clr.append('#f08000') #orange   clr[5]
    clr.append('#aa4a44') #brick    clr[6]
    clr.append('#6082b6') #coast    clr[7]
    clr.append('#CC5500') #burnt    clr[8]
    clr.append('#702963') #space    clr[9]
    return(clr)

(df,X1,Y1,Y2,Y3,Q)=readcsv()
(stg,name,opt)=numbers()
(clr)=color()

sect1=(df.index>=stg[0])&(df.index<=stg[2]) #MES
s1=[sect1,name[0],clr[4]]

sect2=(df.index>=stg[2])&(df.index<=stg[3]) #BECO
s2=[sect2,name[3],clr[5]]                   #

sect3=(df.index>=stg[3])&(df.index<=stg[4]) #MECO
s3=[sect3,name[4],clr[6]]                   #

sect4=(df.index>=stg[4])&(df.index<=stg[5]) #SECO
s4=[sect4,name[5],clr[7]]                   #

sect5=(df.index>=stg[5])&(df.index<=stg[6]) #OIB
s5=[sect5,name[6],clr[8]]                   #

sect6=(df.index>=stg[6])&(df.index<=stg[7]) #OIBECO
s6=[sect6,name[7],clr[9]]

queue=(df.index>=stg[0])&(df.index<=stg[7]) #Q
que=[queue,name[1],clr[2]]        

maxq=(df.index==stg[1]) 
mql=('Max Q ({:.2f}) @ T+ {:.2f}s'.format(df.at[stg[1],Q[0]],df.at[stg[1],X1[0]]))
mq=[maxq,mql,clr[3]]

ms1=(df.index==stg[0])
ml1='MES @ T+ {:.2f}s'.format(df.at[stg[0],X1[0]])
m1=[ms1,ml1,clr[3]]

ms2=df.index==stg[2]
ml2='BECO @ T+ {:.2f}s'.format(df.at[stg[2],X1[0]])
m2=[ms2,ml2,clr[1]]

ms3=df.index==stg[3]
ml3='MECO @ T+ {:.2f}s'.format(df.at[stg[3],X1[0]])
m3=[ms3,ml3,clr[1]]

ms4=df.index==stg[4]
ml4='SECO @ T+ {:.2f}s'.format(df.at[stg[4],X1[0]])
m4=[ms4,ml4,clr[1]]

ms5=df.index==stg[5]
ml5='BURN @ T+ {:.2f}s'.format(df.at[stg[5],X1[0]])
m5=[ms5,ml5,clr[2]]

ms6=df.index==stg[6]
ml6='OIBECO @ T+ {:.2f}s'.format(df.at[stg[6],X1[0]])
m6=[ms6,ml6,clr[1]]
    
#figure setup
fig, ax = plt.subplots(nrows=1, ncols=3, figsize=(16,6))
#Chart 1 Plots - Altitude

ax[0].plot(df.loc[s1[0],X1[0]],df.loc[s1[0],Y1[0]],label=s1[1],color=s1[2],zorder=2)
ax[0].plot(df.loc[s2[0],X1[0]],df.loc[s2[0],Y1[0]],label=s2[1],color=s2[2],zorder=2)
ax[0].plot(df.loc[s3[0],X1[0]],df.loc[s3[0],Y1[0]],label=s3[1],color=s3[2],zorder=2)
ax[0].plot(df.loc[s4[0],X1[0]],df.loc[s4[0],Y1[0]],label=s4[1],color=s4[2],zorder=2)
ax[0].plot(df.loc[s5[0],X1[0]],df.loc[s5[0],Y1[0]],label=s5[1],color=s5[2],zorder=2)
ax[0].plot(df.loc[s6[0],X1[0]],df.loc[s6[0],Y1[0]],label=s6[1],color=s6[2],zorder=2)
ax[0].scatter(df.loc[m1[0],X1[0]],df.loc[m1[0],Y1[0]],marker='s',color=m1[2],zorder=2)
ax[0].scatter(df.loc[m2[0],X1[0]],df.loc[m2[0],Y1[0]],marker='o',color=m2[2],zorder=2)
ax[0].scatter(df.loc[m3[0],X1[0]],df.loc[m3[0],Y1[0]],marker='o',color=m3[2],zorder=2)
ax[0].scatter(df.loc[m4[0],X1[0]],df.loc[m4[0],Y1[0]],marker='o',color=m4[2],zorder=2)
ax[0].scatter(df.loc[m5[0],X1[0]],df.loc[m5[0],Y1[0]],marker='^',color=m5[2],zorder=2)
ax[0].scatter(df.loc[m6[0],X1[0]],df.loc[m6[0],Y1[0]],marker='o',color=m6[2],zorder=2)

#labels
ax[0].set_xlabel(X1[1])
ax[0].set_ylabel(Y1[1])
#Chart Options
ax[0].axhline(y=70e3, color=clr[2], linestyle=':')

ax[0].grid(True)
ax[0].set_ylim(bottom=0)
ax[0].autoscale(enable=True,axis='x',tight=True)
ax[0].xaxis.set_major_locator(tr.MultipleLocator(X1[2]))
ax[0].yaxis.set_major_locator(tr.MultipleLocator(Y1[2]))
ax[0].legend(loc='upper center',bbox_to_anchor=(0.5, -0.15),ncol=2,fancybox=True)

ax[1].plot(df.loc[s1[0],X1[0]],df.loc[s1[0],Y2[1][0]],label=s1[1],color=s1[2],zorder=2)
ax[1].plot(df.loc[s2[0],X1[0]],df.loc[s2[0],Y2[1][0]],label=s2[1],color=s2[2],zorder=2)
ax[1].plot(df.loc[s3[0],X1[0]],df.loc[s3[0],Y2[1][0]],label=s3[1],color=s3[2],zorder=2)
ax[1].plot(df.loc[s4[0],X1[0]],df.loc[s4[0],Y2[1][0]],label=s4[1],color=s4[2],zorder=2)
ax[1].plot(df.loc[s5[0],X1[0]],df.loc[s5[0],Y2[1][0]],label=s5[1],color=s5[2],zorder=2)
ax[1].plot(df.loc[s6[0],X1[0]],df.loc[s6[0],Y2[1][0]],label=s6[1],color=s6[2],zorder=2)

ax[1].scatter(df.loc[m1[0],X1[0]],df.loc[m1[0],Y2[1][0]],marker='s',color=m1[2],zorder=2)
ax[1].scatter(df.loc[m2[0],X1[0]],df.loc[m2[0],Y2[1][0]],marker='o',color=m2[2],zorder=2)
ax[1].scatter(df.loc[m3[0],X1[0]],df.loc[m3[0],Y2[1][0]],marker='o',color=m3[2],zorder=2)
ax[1].scatter(df.loc[m4[0],X1[0]],df.loc[m4[0],Y2[1][0]],marker='o',color=m4[2],zorder=2)
ax[1].scatter(df.loc[m5[0],X1[0]],df.loc[m5[0],Y2[1][0]],marker='^',color=m5[2],zorder=2)
ax[1].scatter(df.loc[m6[0],X1[0]],df.loc[m6[0],Y2[1][0]],marker='o',color=m6[2],zorder=2)

ax[1].set_xlabel(X1[1])
ax[1].set_ylabel(Y2[1][1])
ax[1].grid(True)
ax[1].set_ylim(bottom=0)
ax[1].autoscale(enable=True,axis='x',tight=True)
ax[1].xaxis.set_major_locator(tr.MultipleLocator(X1[2]))
ax[1].yaxis.set_major_locator(tr.MultipleLocator(Y2[1][2]))

q=ax[1].twinx()

q.scatter(df.loc[mq[0],X1[0]],df.loc[mq[0],Q[0]],marker='x',label=mq[1],color=mq[2],zorder=3)
q.plot(df.loc[que[0],X1[0]],df.loc[que[0],Q[0]],label=que[1],color=que[2],zorder=2)

q.set_ylabel(Q[1])
q.set_ylim(bottom=0)
q.autoscale(enable=True,axis='x',tight=True)
q.yaxis.set_major_locator(tr.MultipleLocator(Q[2]))
q.legend(loc='upper center',bbox_to_anchor=(0.5, -0.15),ncol=2,fancybox=True)

ax[2].plot(df.loc[s1[0],X1[0]],df.loc[s1[0],Y3[0]],color=s1[2],zorder=2)
ax[2].plot(df.loc[s2[0],X1[0]],df.loc[s2[0],Y3[0]],color=s2[2],zorder=2)
ax[2].plot(df.loc[s3[0],X1[0]],df.loc[s3[0],Y3[0]],color=s3[2],zorder=2)
ax[2].plot(df.loc[s4[0],X1[0]],df.loc[s4[0],Y3[0]],color=s4[2],zorder=2)
ax[2].plot(df.loc[s5[0],X1[0]],df.loc[s5[0],Y3[0]],color=s5[2],zorder=2)
ax[2].plot(df.loc[s6[0],X1[0]],df.loc[s6[0],Y3[0]],color=s6[2],zorder=2)
ax[2].scatter(df.loc[m1[0],X1[0]],df.loc[m1[0],Y3[0]],marker='s',label=m1[1],color=m1[2],zorder=2)
ax[2].scatter(df.loc[m2[0],X1[0]],df.loc[m2[0],Y3[0]],marker='o',label=m2[1],color=m2[2],zorder=2)
ax[2].scatter(df.loc[m3[0],X1[0]],df.loc[m3[0],Y3[0]],marker='o',label=m3[1],color=m3[2],zorder=2)
ax[2].scatter(df.loc[m4[0],X1[0]],df.loc[m4[0],Y3[0]],marker='o',label=m4[1],color=m4[2],zorder=2)
ax[2].scatter(df.loc[m5[0],X1[0]],df.loc[m5[0],Y3[0]],marker='^',label=m5[1],color=m5[2],zorder=2)
ax[2].scatter(df.loc[m6[0],X1[0]],df.loc[m6[0],Y3[0]],marker='o',label=m6[1],color=m6[2],zorder=2)

ax[2].set_xlabel(X1[1])
ax[2].set_ylabel(Y3[1])
ax[2].grid(True)
ax[2].set_ylim(bottom=0)
ax[2].autoscale(enable=True,axis='x',tight=True)
ax[2].xaxis.set_major_locator(tr.MultipleLocator(X1[2]))
ax[2].yaxis.set_major_locator(tr.MultipleLocator(Y3[2]))
ax[2].legend(loc='upper center',bbox_to_anchor=(0.5, -0.15),ncol=2,fancybox=True)

plt.tight_layout()
plt.title(f"{csv} Launch, {pad}", loc='center')
plt.subplots_adjust(top=0.9)
plt.show()
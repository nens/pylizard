import pandas as pd
import os
import pylizard2 as pylizard
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
#%% Domein
polygon = "POLYGON((6.7793542595117176 52.279991347034034,6.7793542595117176 52.290425076318826,6.808999017235293 52.290425076318826,6.808999017235293 52.279991347034034,6.7793542595117176 52.279991347034034))"

mean_min, mean_max = pd.Timestamp('2000-01-01'), pd.Timestamp('2022-01-01')
    

#%% Function to combine hand and diver measurements 
def h(uuid_hand, uuid_diver):
    if uuid_hand!='' and uuid_diver=='':
        return pylizard.get_timeseries(uuid_hand)
        print('1')
    elif uuid_diver!='' and uuid_hand=='':
        return pylizard.get_timeseries(uuid_diver)
        print('2')
    else:
        h = pd.concat([pylizard.get_timeseries(uuid_hand), pylizard.get_timeseries(uuid_diver)], axis=1)
        h.columns = ['hand', 'diver']
        
        h['head'] = h['diver']
        h.loc[h.index<h['diver'].dropna().index.min(), 'head'] = h.loc[h.index<h['diver'].dropna().index.min(), 'hand']
        
        return h['head']

#%% collect data binnen domein 
df = pylizard.polygon_to_groundwaterstations(polygon)
df = df.loc[(df['uuid_hand']!='')|(df['uuid_diver']!='')]
df.index.name='id'

head = {}
mean = {}
perc_50 = {}
tmin = {}
tmax = {}
for i, r in df.iterrows():
    try:
        headi = h(r['uuid_hand'], r['uuid_diver'])
        head[i] = headi.loc[abs(headi-headi.mean())<7.5*headi.std()]
        mean[i] = head[i].loc[(head[i].index>mean_min)&(head[i].index<mean_max)].mean()
        perc_50[i] = head[i].loc[(head[i].index>mean_min)&(head[i].index<mean_max)].quantile(.5)
        tmin[i] = head[i].index.min()
        tmax[i] = head[i].index.max()
        print(f"Geslaagd voor {i,r}")
    except:
        print(f"Mislukt voor {i,r}")
        continue
    
df['tmin'] = df.index.map(tmin)
df['tmax'] = df.index.map(tmax)
df['mean_{}_{}'.format(mean_min.strftime('%Y%m%d'), mean_max.strftime('%Y%m%d'))] = df.index.map(mean) 
df['perc_50_{}_{}'.format(mean_min.strftime('%Y%m%d'), mean_max.strftime('%Y%m%d'))] = df.index.map(perc_50) 

df.dropna(subset = ["tmin","tmax"], axis = 0, how = 'any', inplace = True)
#%% save files 
folder = r'c:\Overijssel\009_Hasselo\003_Data'
if not os.path.exists(folder):
    os.makedirs(folder)
    
df.to_csv(os.path.join(folder, 'metadata_Hasselo.csv'), index=False)

for k, v in head.items():
    v.to_csv(os.path.join(folder, '{}.csv'.format(k)), header=True)

#%% PER PEILBUIS INPUT
df_peilbuizen = pd.DataFrame([["28DL0073",1],
                              ["28DP0047",1],
                              ["28DP0047",2],
                              ["WD10P89",1],
                              ["WD10P90",1]
                              ["B33C0126",1]],
                             columns = ["peilbuis", "filter"])

mean_min, mean_max = pd.Timestamp('2012-12-31'), pd.Timestamp('2021-01-01') 
            #tijdstippen waar tussen de mean en P_50 worden berekend
            
tmin_fig, tmax_fig =  pd.Timestamp('2014-12-31'), pd.Timestamp('2022-08-01') 
dir_out = r"c:\Overijssel\006_VergunningswijzigingWierden\Figuren_2022\Peilbuizen"

#% metadata peilbuizen 
head = {}
mean = {}
head_P50 = {}
tmin = {}
tmax = {}

for idx, row in df_peilbuizen.iterrows():
    p = pylizard.Peilbuis(row["peilbuis"], row["filter"])

    headi = h(p.uuid_hand, p.uuid_diver)
    # headi[headi > 15] = np.nan
    head[idx] = headi.loc[abs(headi-headi.mean())<7.5*headi.std()]
    mean[idx] = head[idx].loc[(head[idx].index>mean_min)&(head[idx].index<mean_max)].mean()
    head_P50[idx] = head[idx].loc[(head[idx].index>mean_min)&(head[idx].index<mean_max)].quantile(.5)
    tmin[idx] = head[idx].index.min()
    tmax[idx] = head[idx].index.max()
    
    fig, axs = plt.subplots(figsize=(11,8))
    axs.plot(headi)
    axs.set_xlim(tmin_fig,tmax_fig)
    axs.axhline(p.surface_level, color = "darkred", linewidth = 1.5, linestyle = "--", label = "Maaiveld")
    axs.set_ylabel("Grondwaterstand (m+NAP)")
    axs.set_xlabel("")
    axs.grid()
    axs.legend( loc = "upper left")
            
    textstr = '\n'.join((
        r'$\mathrm{code}$: %s' % (p.code, ),
    r'$\mathrm{bkf}=%.2f$ m+NAP' % (p.surface_level - p.bkf, ),
    r'$\mathrm{okf}=%.2f$ m+NAP' % (p.surface_level - p.okf)))
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    axs.text( 0.8, 0.98, textstr, transform=axs.transAxes, fontsize=12,
             verticalalignment='top', bbox=props)
    myFmt = mdates.DateFormatter('%d-%m-%Y')
    axs.xaxis.set_major_formatter(myFmt)
    axs.set_ylim(int(headi.min()),p.surface_level+0.5)
    
    # fig.savefig(os.path.join(dir_out,p.code+"-"+ str(p.filt)+".png"))    
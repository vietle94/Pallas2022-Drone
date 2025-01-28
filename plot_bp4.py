import pandas as pd
import glob
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.dates as mdates
import string
# %%
def calculate_height(p0, p1, T0, T1):
    R = 287.05
    g = 9.80665
    height = R/g * ((T0 + T1)/2 + 273.15) * np.log(p0/p1)
    return height

def calculate_height_df(df, p, T):
    df_height = df.copy()
    df_height.dropna(subset=p, inplace=True)
    height = np.zeros_like(df_height[p])
    height[1:] = calculate_height(df_height[p][:-1].values,
                                  df_height[p][1:].values,
                                  df_height[T][:-1].values,
                                  df_height[T][1:].values)
    df_height['height'] = height
    df['height'] = df_height['height']
    df.replace({'height': np.nan}, 0, inplace=True)
    df['height'] = df['height'].cumsum()
    return df

particle_lab = ['b0_OPC-BP5', 'b1_OPC-BP5', 'b2_OPC-BP5', 'b3_OPC-BP5', 'b4_OPC-BP5',
    'b5_OPC-BP5', 'b6_OPC-BP5', 'b7_OPC-BP5', 'b8_OPC-BP5', 'b9_OPC-BP5',
    'b10_OPC-BP5', 'b11_OPC-BP5', 'b12_OPC-BP5', 'b13_OPC-BP5',
    'b14_OPC-BP5', 'b15_OPC-BP5', 'b16_OPC-BP5', 'b17_OPC-BP5',
    'b18_OPC-BP5', 'b19_OPC-BP5', 'b20_OPC-BP5', 'b21_OPC-BP5',
    'b22_OPC-BP5', 'b23_OPC-BP5']

# %%
file_dir = r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\Lake"
df = pd.DataFrame({})
for i, file in enumerate(glob.glob(file_dir + '/*.csv')):
    df_file = pd.read_csv(file)
    df_file['datetime'] = pd.to_datetime(df_file['datetime'])
    df_file = calculate_height_df(df_file, 'press_bme_BME_BP4', 'temp_bme_BME_BP4')
    df_file[df_file['height'] < 5] = np.nan
    df_file.dropna(subset=['height'], inplace=True)
    df_file.reset_index(drop=True, inplace=True)
    df_file['Total_conc'] = df_file[particle_lab].sum(axis=1) / \
          (df_file['FlowRate_OPC-BP5']/100) / (df_file['period_OPC-BP5']/100) # unit is cm^-3
    df_file = df_file[df_file.index < df_file['press_bme_BME_BP4'].idxmin()]
    df_file['color'] = mdates.date2num(df_file['datetime'][0])
    df = pd.concat([df, df_file], ignore_index=True)
# %%
fig, ax = plt.subplots(2, 2, figsize=(8, 6), sharey=True, constrained_layout=True)
for ax_, parameter, lab in zip(ax.flatten(),
                          ['temp_bme_BME_BP4', 'rh_bme_BME_BP4',
                           'sht85_dp_SHT85_BP4', 'Total_conc'],
                           [r'Temperature $(\degree C)$', 'RH (%)', r'Dew point $(\degree C)$', r'N $(cm^{-3})$']):
    p = ax_.scatter(df[parameter], df['height'],
                s=1,
                c=df['color'], cmap='viridis')
    ax_.grid()
    ax_.set_xlabel(lab)

ax[0, 0].set_ylabel('Height a.g.l (m)')
ax[1, 0].set_ylabel('Height a.g.l (m)')
ax_.set_xscale('log')
for n, ax_ in enumerate(ax.flatten()):
    ax_.text(-0.0, 1.05, '(' + string.ascii_lowercase[n] + ')',
        transform=ax_.transAxes, size=12)
cbar = fig.colorbar(p, ax=ax)
cbar.ax.yaxis.set_major_locator(mdates.DayLocator(interval=7))
cbar.ax.yaxis.set_major_formatter(mdates.DateFormatter(r'%d/%m'))
fig.savefig(r"C:\Users\le\OneDrive - Ilmatieteen laitos\My_articles\2024\Pallas\BP4_profile.png", dpi=600,
            bbox_inches='tight')

# %%

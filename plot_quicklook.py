import pandas as pd
import matplotlib.pyplot as plt
import glob
import numpy as np
import os
import matplotlib.dates as mdates

data_dir = [r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\data_ready/Lake/",
            r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\data_ready/Parking/"]

# %%
for site in data_dir:
    for file in glob.glob(site + '*.csv'):
        file_name = os.path.basename(file).replace('csv', 'png')
        df = pd.read_csv(file)
        df['datetime(UTC)'] = pd.to_datetime(df['datetime(UTC)'])
        df.replace(-9999.9, np.nan, inplace=True)

        fig, ax = plt.subplots(4, 1, figsize=(8, 6), sharex=True, constrained_layout=True)
        ax[0].plot(df['datetime(UTC)'], df['altitude(m)'], '.', label='altitude(m)')
        ax[1].plot(df['datetime(UTC)'], df['P_bme280(Pa)'], '.', label='P_bme280(Pa)')
        ax[2].plot(df['datetime(UTC)'], df['wind_speed(m/s)'], '.', label='wind_speed(m/s)')
        ax[3].plot(df['datetime(UTC)'], df['total_conc(cm-3)'], '.', label='total_conc(cm-3)')
        ax[3].set_xlabel('datetime(UTC)')
        for ax_ in ax.flatten():
            ax_.legend()
            ax_.grid()
        ax_.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        fig.savefig(site + file_name, bbox_inches='tight', dpi=600)
        plt.close(fig)

# %%

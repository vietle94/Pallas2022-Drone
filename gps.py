import glob
import pandas as pd

gps_path = r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\flight_record\positioning/"
gps_files = glob.glob(gps_path + '*.csv')

df = pd.concat([pd.read_csv(file) for file in gps_files], ignore_index=True)
df = df[['datetime(utc)', 'latitude', 'longitude', 'altitude(meters)']]
df['datetime(utc)'] = pd.to_datetime(df['datetime(utc)'])
df = df.sort_values('datetime(utc)')
df = df.reset_index(drop=True)
df_save = df.set_index('datetime(utc)').resample('1s').mean()
df_save = df_save.reset_index()
df_save = df_save.dropna()

df_save.to_csv(r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\flight_record/" 
          + 'gps_merged.csv', index=False)

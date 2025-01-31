import glob
import pandas as pd
from functools import reduce
from UAVision.mavic import preprocess
import xarray as xr
import re

gps_path = r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\flight_record/gps_merged.csv"
wind_path = r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\flight_record/wind_merged.csv"

gps = pd.read_csv(gps_path)
wind = pd.read_csv(wind_path)

gps["datetime(utc)"] = pd.to_datetime(gps["datetime(utc)"]) + pd.Timedelta(hours=2)
gps.rename(columns={"datetime(utc)": "datetime"}, inplace=True)
wind = wind[["datetime", "Wind Speed", "Wind Direction"]]
wind["datetime"] = pd.to_datetime(wind["datetime"]) - pd.Timedelta(hours=1)
mid_bin = (preprocess.n3_binedges[1:] + preprocess.n3_binedges[:-1]) / 2

save_dir = [
    r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\data_ready/Lake/",
    r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\data_ready/Parking/",
]
data_dir = [
    r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\Lake",
    r"C:\Users\le\OneDrive - Ilmatieteen laitos\Campaigns\Pace2022\BP4\DATA\Parking",
]

for site, save_path in zip(data_dir, save_dir):
    files = glob.glob(site + "/*.csv")
    for file in files:
        df = pd.read_csv(file)
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.merge(gps, on="datetime", how="left")
        df = df.merge(wind, on="datetime", how="left")
        df_conc, df_dNdlog = preprocess.calculate_concentration(
            df,
            [
                "b0_OPC-BP5",
                "b1_OPC-BP5",
                "b2_OPC-BP5",
                "b3_OPC-BP5",
                "b4_OPC-BP5",
                "b5_OPC-BP5",
                "b6_OPC-BP5",
                "b7_OPC-BP5",
                "b8_OPC-BP5",
                "b9_OPC-BP5",
                "b10_OPC-BP5",
                "b11_OPC-BP5",
                "b12_OPC-BP5",
                "b13_OPC-BP5",
                "b14_OPC-BP5",
                "b15_OPC-BP5",
                "b16_OPC-BP5",
                "b17_OPC-BP5",
                "b18_OPC-BP5",
                "b19_OPC-BP5",
                "b20_OPC-BP5",
                "b21_OPC-BP5",
                "b22_OPC-BP5",
                "b23_OPC-BP5",
            ],
            "FlowRate_OPC-BP5",
            "period_OPC-BP5",
        )
        df_conc.columns = [f"bin{x}_conc(cm-3)" for x in range(24)]
        df_conc["total_conc(cm-3)"] = df_conc.sum(axis=1)
        df_dNdlog.columns = [f"bin{x}_dNdlogDp(cm-3)" for x in range(24)]
        df1 = df[
            [
                "datetime",
                "latitude",
                "longitude",
                "altitude(meters)",
                "temp_bme_BME_BP4",
                "press_bme_BME_BP4",
                "rh_bme_BME_BP4",
                "sht85_temp_SHT85_BP4",
                "sht85_rh_SHT85_BP4",
                "sht85_dp_SHT85_BP4",
                "Wind Speed",
                "Wind Direction",
            ]
        ]
        df1.rename(
            columns={
                "datetime": "datetime(UTC)",
                "latitude": "latitude(deg)",
                "longitude": "longitude(deg)",
                "altitude(meters)": "altitude(m)",
                "press_bme_BME_BP4": "P_bme280(Pa)",
                "temp_bme_BME_BP4": "T_bme280(C)",
                "rh_bme_BME_BP4": "RH_bme280(%)",
                "sht85_temp_SHT85_BP4": "T_sht85(C)",
                "sht85_rh_SHT85_BP4": "RH_sht85(%)",
                "sht85_dp_SHT85_BP4": "DP_sht85(C)",
                "Wind Speed": "wind_speed(m/s)",
                "Wind Direction": "wind_direction(deg)",
            },
            inplace=True,
        )
        df2 = df[
            [
                "PMA(ugm3)_OPC-BP5",
                "PMB(ugm3)_OPC-BP5",
                "PMC(ugm3)_OPC-BP5",
                "TempdegC_OPC-BP5",
                "RH(%)_OPC-BP5",
                "FlowRate_OPC-BP5",
                "period_OPC-BP5",
            ]
        ]
        df2.rename(
            columns={
                "PMA(ugm3)_OPC-BP5": "pm1(ug/m3)",
                "PMB(ugm3)_OPC-BP5": "pm25(ug/m3)",
                "PMC(ugm3)_OPC-BP5": "pm10(ug/m3)",
                "TempdegC_OPC-BP5": "T_flow(C)",
                "RH(%)_OPC-BP5": "RH_flow(%)",
                "FlowRate_OPC-BP5": "flow_rate(cm3/s)",
                "period_OPC-BP5": "period(s)",
            },
            inplace=True,
        )
        df2["flow_rate(cm3/s)"] = df2["flow_rate(cm3/s)"] / 100
        df2["period(s)"] = df2["period(s)"] / 100
        df_save = reduce(
            lambda x, y: pd.merge(x, y, left_index=True, right_index=True),
            [df1, df_conc, df_dNdlog, df2],
        )

        # Find the lag between raspberry pi and mavic
        if (
            df_save["latitude(deg)"].count() > 1
        ):  # only calculate lag if there is gps data
            df_save = (
                df_save.set_index("datetime(UTC)").resample("1s").mean().reset_index()
            )
            lag = preprocess.calculate_lag(
                df_save, "altitude(m)", "P_bme280(Pa)", 300
            )  # search for lag up to 300s
            non_gps_wind = [
                x
                for x in df_save.columns
                if x
                not in [
                    "datetime(UTC)",
                    "latitude(deg)",
                    "longitude(deg)",
                    "altitude(m)",
                    "wind_speed(m/s)",
                    "wind_direction(deg)",
                ]
            ]
            # The end of the shifted columns is removed, doesn't mattter since the end of the data with no gps is not used
            df_save[non_gps_wind] = df_save[non_gps_wind].shift(lag)

        # Remove first and last rows with NaN values in altitude
        first_idx = df_save["altitude(m)"].first_valid_index()
        last_idx = df_save["altitude(m)"].last_valid_index()
        df_save = df_save.loc[first_idx:last_idx]
        df_save.fillna(-9999.9, inplace=True)
        save_time = df_save.iloc[0]["datetime(UTC)"].strftime("%Y%m%d.%H%M")
        df_save.to_csv(save_path + "FMI.DBP.b1." + save_time + ".csv", index=False)
        print(f"Saved {save_time}.csv")

        # save ncdf file
        df_xr = xr.Dataset.from_dataframe(df_save.set_index("datetime(UTC)"))
        df_xr["concentration"] = xr.concat(
            [df_xr[f"bin{x}_conc(cm-3)"] for x in range(24)], mid_bin
        ).T
        df_xr["concentration_dNdlogDp"] = xr.concat(
            [df_xr[f"bin{x}_dNdlogDp(cm-3)"] for x in range(24)], mid_bin
        ).T
        df_xr = df_xr.rename({"concat_dim": "bin", "datetime(UTC)": "datetime"})
        df_xr = df_xr[
            [
                "latitude(deg)",
                "longitude(deg)",
                "altitude(m)",
                "T_bme280(C)",
                "P_bme280(Pa)",
                "RH_bme280(%)",
                "T_sht85(C)",
                "RH_sht85(%)",
                "DP_sht85(C)",
                "wind_speed(m/s)",
                "wind_direction(deg)",
                "concentration",
                "total_conc(cm-3)",
                "concentration_dNdlogDp",
                "pm1(ug/m3)",
                "pm25(ug/m3)",
                "pm10(ug/m3)",
                "T_flow(C)",
                "RH_flow(%)",
                "flow_rate(cm3/s)",
                "period(s)",
            ]
        ]
        df_xr = df_xr.rename({x:re.sub(r"\(.*\)", "", x) for x in list(df_xr.data_vars)})

        df_xr['datetime'].attrs = {'long_name': 'datetime in UTC', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['bin'].attrs = {'units': 'cm-3', 'long_name': 'centers of the size bins', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['latitude'].attrs = {'units': 'degree', 'long_name': 'latitude', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['longitude'].attrs = {'units': 'degree', 'long_name': 'longitude', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['altitude'].attrs = {'units': 'm', 'long_name': 'altitude', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['T_bme280'].attrs = {'units': 'degree Celsius ', 'long_name': 'Temperature measured by BME280', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['P_bme280'].attrs = {'units': 'hPa ', 'long_name': 'Pressure measured by BME280', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['RH_bme280'].attrs = {'units': '% ', 'long_name': 'Relative humidity measured by BME280', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['T_sht85'].attrs = {'units': 'degree Celsius', 'long_name': 'Temperature measured by SHT85', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['RH_sht85'].attrs = {'units': '%', 'long_name': 'Temperature measured by SHT85', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['DP_sht85'].attrs = {'units': 'degree Celsius', 'long_name': 'Dewpoint temperature measured by SHT85', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['wind_speed'].attrs = {'units': 'm/s', 'long_name': 'Wind speed', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['wind_direction'].attrs = {'units': 'degree', 'long_name': 'Wind direction', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['concentration'].attrs = {'units': 'cm-3', 'long_name': 'Particle concentration', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['total_conc'].attrs = {'units': 'cm-3', 'long_name': 'Total Particle concentration', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['concentration_dNdlogDp'].attrs = {'units': 'cm-3', 'long_name': 'Normalized concentration in dNdlogDp', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['pm1'].attrs = {'units': 'µg m−3', 'long_name': 'Particulate Matter with diameters less than 1µm', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['pm25'].attrs = {'units': 'µg m−3', 'long_name': 'Particulate Matter with diameters less than 2.5µm', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['pm10'].attrs = {'units': 'µg m−3', 'long_name': 'Particulate Matter with diameters less than 10µm', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['T_flow'].attrs = {'units': 'degree Celsius', 'long_name': 'Temperature of the OPC sample inlet', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['RH_flow'].attrs = {'units': '%', 'long_name': 'Relative humidity of the OPC sample inlet', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['flow_rate'].attrs = {'units': 'cm3/s', 'long_name': 'Flow rate of the OPC sample inlet', '_FillValue': -9999.9, 'processing_level': 'b1'}
        df_xr['period'].attrs = {'units': 's', 'long_name': 'Sampling time of the OPC sample inlet', '_FillValue': -9999.9, 'processing_level': 'b1'}

        df_xr.attrs['platform_name'] = "Mavic 2 Pro"
        df_xr.attrs['institution'] = "Finnish Meteorological Institute"
        df_xr.attrs['source'] = "PaCE2022 campaign"
        df_xr.attrs['processing_level'] = "b1"

        df_xr.to_netcdf(save_path + "FMI.DBP.b1." + save_time + ".nc")
        print(f"Saved {save_time}.nc")
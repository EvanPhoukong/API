import requests, sys, urllib3, arcpy, random, time, threading, pickle, csv
import pandas as pd
from pathlib import Path

#C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3
#"SQL Database - C:\Users\ephoukong\OneDrive - City of Stockton\Documents\ArcGIS\Projects\ZipCodes\SQLServer-COS-DB-01-GISDATA.sde"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

arcpy.env.workspace = r"C:\Users\ephoukong\OneDrive - City of Stockton\Documents\ArcGIS\Projects\ZipCodes\SQLServer-COS-DB-01-GISDATA.sde" #----PLEASE REPLACE LINE WITH ABSOLUTE DATABASE PATH----
arcpy.env.overwriteOutput = True
layer = arcpy.env.workspace + r'\GISDATA.DBO.Addresses'
field = ['FullAddress', "Zipcode"]

csv_path = r"C:\Users\ephoukong\OneDrive - City of Stockton\API\found.csv"
df = pd.read_csv(csv_path)

check_addrs = r"C:\Users\ephoukong\OneDrive - City of Stockton\API\Check_Addresses.xlsx"
df2 = pd.read_excel(check_addrs)

address_col = "FullAddress"
zip_col = "ZIPCode"

addresses_to_update = set(df["streetAddressAbbreviation"].dropna().astype(str).str.strip())
correct = 0 
incorrect = 0

df["FullAddress"] = df2['FullAddress'].to_numpy()[:len(df)]
df["old_zips"] = df2['Zipcode'].to_numpy()[:len(df)]
df = df[['FullAddress',"old_zips", "ZIPCode"]]
print(df)

# Print the column of interest for rows where they match
matches = df[df["old_zips"] == df["ZIPCode"]]
# print(matches[["FullAddress", "ZIPCode", "old_zips"]])

# Count how many did NOT match
diffs = df[df["old_zips"] != df["ZIPCode"]]
# print(diffs[["FullAddress", "ZIPCode", "old_zips"]])


addresses_to_update = set(df["FullAddress"])
seen = set()

with arcpy.da.UpdateCursor(layer, field) as cursor:
    for row in cursor:
        addr = (str(row[0]))
        if addr in addresses_to_update:
            #Detect Duplicates
            # if addr in seen:
            #     print('Duplicate: ', addr)
            #     sys.exit()
            # seen.add(addr)
            uspsZip = str(df.loc[df[address_col] == addr, zip_col].iloc[0]).strip()
            sdeZip = str(row[1]).strip()

            if uspsZip == sdeZip:
                correct += 1
            else:
                incorrect += 1 
                row[1] = uspsZip
                #cursor.updateRow(row)
                print(f"{addr} -- USPS: {uspsZip}, SDE: {sdeZip}")

print(f"\nMatch: {correct}, Different: {incorrect}")
incorrect = (df["old_zips"] != df["ZIPCode"]).sum()
correct = (df["old_zips"] == df["ZIPCode"]).sum()
print(f"\nMatch: {correct}, Different: {incorrect}")

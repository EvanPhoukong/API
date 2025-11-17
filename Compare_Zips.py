import requests, sys, urllib3, arcpy, random, time, threading, pickle, csv
import pandas as pd
from pathlib import Path

#C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3
#"SQL Database - C:\Users\ephoukong\OneDrive - City of Stockton\Documents\ArcGIS\Projects\ZipCodes\SQLServer-COS-DB-01-GISDATA.sde"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

arcpy.env.workspace = r"C:\Users\ephoukong\OneDrive - City of Stockton\Documents\ArcGIS\Projects\ZipCodes\SQLServer-COS-DB-01-GISDATA.sde" #----PLEASE REPLACE LINE WITH ABSOLUTE DATABASE PATH----
arcpy.env.overwriteOutput = True
layer = arcpy.env.workspace + r'\GISDATA.DBO.Addresses'
field = ["OBJECTID", 'FullAddress', "Zipcode"]

#Read the address info retrieved from the API into a dataframe
csv_path = r"C:\Users\ephoukong\OneDrive - City of Stockton\API\found.csv"
df = pd.read_csv(csv_path)

#Read the excel table containg the original address information (from the database) into a dataframe
check_addrs = r"C:\Users\ephoukong\OneDrive - City of Stockton\API\Check_Addresses.xlsx"
df2 = pd.read_excel(check_addrs)

#Store the address and zip columns
address_col = "FullAddress"
zip_col = "ZIPCode"

#Initialize correct and inncorrect variables 
correct = 0 #Represents the number of addresses whose database zipcode matches the USPS zipcode
incorrect = 0 #Represents the number of addresses whose database zipcode does not match the USPS zipcode

#Create one dataframe that contains the FullAddresses, the original database zips, and the USPS Zips
df["FullAddress"] = df2['FullAddress'].to_numpy()[:len(df)]
df["old_zips"] = df2['Zipcode'].to_numpy()[:len(df)]
df = df[['FullAddress',"old_zips", "ZIPCode"]]
print(df)

# Print the column of interest for rows where they match
matches = df[df["old_zips"] == df["ZIPCode"]]
# print(matches[["FullAddress", "ZIPCode", "old_zips"]])

# Print the column of interest for rows where they did notmatch
diffs = df[df["old_zips"] != df["ZIPCode"]]
# print(diffs[["FullAddress", "ZIPCode", "old_zips"]])

#Store the addresses that have been queried by the API in the a set for quick searches
addresses_to_update = set(df["FullAddress"])
seen = set()
matched_oids = []

#Iterate through each row of the address layer and update the zips if they are different
with arcpy.da.UpdateCursor(layer, field) as cursor:
    for row in cursor:
        oid = row[0]
        addr = (str(row[1]))

        if addr in addresses_to_update: #Check if this address has been queried by the API

            #Detect Duplicates
            # if addr in seen:
            #     print('Duplicate: ', addr)
            #     sys.exit()
            # seen.add(addr)

            #Store the usps and database zips
            uspsZip = str(df.loc[df[address_col] == addr, zip_col].iloc[0]).strip()
            sdeZip = str(row[2]).strip()

            #Compare the usps and database zips, update the database zip if it differs
            if uspsZip == sdeZip:
                correct += 1
            else:
                # Collect OID for export later
                matched_oids.append(oid)

                #Update database
                row[2] = uspsZip
                #cursor.updateRow(row)

                incorrect += 1 
                print(f"{addr} -- USPS: {uspsZip}, SDE: {sdeZip}")

# Make a feature layer first
arcpy.MakeFeatureLayer_management(layer, "matched_lyr")
output_fc = r"C:\Users\ephoukong\OneDrive - City of Stockton\API\layer\Mismatch"

if matched_oids:
    # Build SQL for selecting the OIDs
    oid_field = arcpy.Describe(layer).oidFieldName
    oid_list_string = ",".join(map(str, matched_oids))
    where_clause = f"{oid_field} IN ({oid_list_string})"

    # Select and export
    arcpy.SelectLayerByAttribute_management("matched_lyr", "NEW_SELECTION", where_clause)
    arcpy.CopyFeatures_management("matched_lyr", output_fc)

    print(f"Exported {len(matched_oids)} features to {output_fc}")
else:
    print("No addresses matched. Nothing exported.")

arcpy.AddField_management(output_fc, "USPS_Zip", "TEXT", field_length=20)

with arcpy.da.UpdateCursor(output_fc, ["FullAddres", "USPS_Zip"]) as cursor:

    for row in cursor:
        addr = str(row[0]).strip()

        # lookup USPS data
        if addr in df[address_col].values:
            uspsZip = str(df.loc[df[address_col] == addr, zip_col].iloc[0]).strip()
            row[1] = uspsZip    # USPS_Zip

            cursor.updateRow(row)

#Count the numbers of addresses that had correct zipcode values (database zips matches USPS zips) and incorrect zipcode values
print(f"\nMatch: {correct}, Different: {incorrect}")
incorrect = (df["old_zips"] != df["ZIPCode"]).sum()
correct = (df["old_zips"] == df["ZIPCode"]).sum()
print(f"\nMatch: {correct}, Different: {incorrect}")

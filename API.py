import requests, sys, urllib3, arcpy, random, time, threading, pickle, csv
import pandas as pd
from pathlib import Path

#C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3
#"SQL Database - C:\Users\ephoukong\OneDrive - City of Stockton\Documents\ArcGIS\Projects\ZipCodes\SQLServer-COS-DB-01-GISDATA.sde"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

arcpy.env.workspace = r"C:\Users\ephoukong\OneDrive - City of Stockton\Documents\ArcGIS\Projects\ZipCodes\SQLServer-COS-DB-01-GISDATA.sde" #----PLEASE REPLACE LINE WITH ABSOLUTE DATABASE PATH----
arcpy.env.overwriteOutput = True
layer = arcpy.env.workspace + r'\GISDATA.DBO.Addresses'
field = "FullAddress"


stop_flag = False

def listen_for_stop():
    global stop_flag
    input("Press ENTER at any time to stop...\n")
    stop_flag = True


def generate_token():

    url = r"https://apis.usps.com/oauth2/v3/token"

    headers = {
        'UseAgenr-t': 'Mozilla/5.0',
        "Content-Type":"application/json"
    }

    params = {
        "grant_type": "client_credentials",
        # "client_id": "TwoIIfbOTGJfC4GeETh2kB4PpHnbApJNKZTbcp0oLQnMyXqe",
        # "client_secret": "BVPKQpYcCBnUxXmnxAr1cyBvUA80GmCRCqPJc5xXeEKCQp13knC7MqLCu2YwgI2B"
        "client_id": "2rctZeCgewKiG6DEyhgzWOmxjGPZ7KgPfUdrAl0GRASCtpjU",
        "client_secret": "N1vBbHb42hvctltAdbzvZzX4dpRTNCJckEb62LlvuB6DgYVtLo6uY8FYRG2PAhnq"
    }

    res = requests.post(url=url, json=params, headers=headers, verify=False)

    if res.status_code == 200:
        data = res.json()
        access_token = data.get("access_token")
        print("Access Token:", access_token)
        print("Expires in (seconds):", data.get("expires_in"))
        print()

    else:
        print("Error:", res.status_code, res.text)
        sys.exit()

    return access_token

def pause():

    for _ in range(61):
        if stop_flag:
            return True
        time.sleep(1)
    
    return False


def get_address(token, addrs):

    url = r"https://apis.usps.com/oauth2/v3/token"

    headers = {
        'UseAgenr-t': 'Mozilla/5.0',
        "Content-Type":"application/json"
    }

    params = {
        "grant_type": "client_credentials",
        # "client_id": "TwoIIfbOTGJfC4GeETh2kB4PpHnbApJNKZTbcp0oLQnMyXqe",
        # "client_secret": "BVPKQpYcCBnUxXmnxAr1cyBvUA80GmCRCqPJc5xXeEKCQp13knC7MqLCu2YwgI2B"
        "client_id": "2rctZeCgewKiG6DEyhgzWOmxjGPZ7KgPfUdrAl0GRASCtpjU",
        "client_secret": "N1vBbHb42hvctltAdbzvZzX4dpRTNCJckEb62LlvuB6DgYVtLo6uY8FYRG2PAhnq"
    }

    res = requests.post(url=url, json=params, headers=headers, verify=False)

    if res.status_code == 200:
        data = res.json()
        access_token = data.get("access_token")
        # print("Access Token:", access_token)
        # print("Expires in (seconds):", data.get("expires_in"))
        # print()

    else:
        print("Error:", res.status_code, res.text)
        sys.exit()

    return access_token


def get_addresses(token, addrs, seen):

    missing = found = 0

    url = r"https://apis.usps.com/addresses/v3/address"

    headers = {
        'UseAgenr-t': 'Mozilla/5.0',
        "Content-Type":"application/json",
        "Authorization": f"Bearer {token}"
    }

    params = {
    "streetAddress": None,
    "state": "CA",
    "city": "STOCKTON",
    }

    # Start the input listener thread
    print()
    threading.Thread(target=listen_for_stop, daemon=True).start()
    
    # pause()
    time.sleep(3)

    print("\nAddresses:")
    done = False

    for a in addrs:

        if done:
            break

        success = False

        while not success:
            
            print(a)
            params['streetAddress'] = a

            res = requests.get(url=url, params=params, headers=headers, verify=False)

            if res.status_code == 200:
                data = res.json()
                addr = data.get("address")
                print("Address Info:", addr)

                with open('found.csv', 'a+', newline='') as file:
                    writer = csv.DictWriter(file, fieldnames=addr.keys())

                    # Only write header if file is empty
                    if file.tell() == 0:
                        writer.writeheader()

                    # Write the dictionary as a new row
                    writer.writerow(addr)

                found += 1
                success = True

            else:
                # print("Error:", res.status_code, res.text)

                if res.status_code != 429:
                    with open('missing.txt', 'a+') as file:
                        file.write(f"{a}\n")
                    success = True
                    missing += 1  
                    print(f"Address not found: {a}")

                else:
                    print('Too many API queries. Retrying after one minute.')

            if pause():
                done = True
                break
    
    print(f"Found: {found}")
    print(f"Missing: {missing}\n")

    state = open('state', 'wb')
    pickle.dump(found + missing + seen, state)


def test_API(token):


    url = r"https://apis.usps.com/addresses/v3/address"

    headers = {
        'UseAgenr-t': 'Mozilla/5.0',
        "Content-Type":"application/json",
        "Authorization": f"Bearer {token}"
    }

    params = {
    "streetAddress": '1',
    "state": "CA",
    "city": "STOCKTON",
    }

    res = requests.get(url=url, params=params, headers=headers, verify=False)

    if res.status_code == 200:
        print('API CONNECTION SUCCESSFUL\n')

    else:
        print(f"API CONNECTION UNSUCCESSFUL - Please wait a minute to query the API again")
        sys.exit()

    

if __name__ == "__main__":

    # addrs = arcpy.da.TableToNumPyArray(layer, field)[field].tolist()

    addrs = pd.read_excel('Check_Addresses.xlsx')
    addrs = addrs['FullAddress']

    token = generate_token()
    # test_API(token)
    
    path = Path('state')
    if path.exists():
        state = open('state', 'rb')
        n = pickle.load(state)
    else:
        n = 0

    get_addresses(token, addrs[n:], n)
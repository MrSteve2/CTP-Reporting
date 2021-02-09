import pandas as pd
import urllib3 as urllib
import urllib.request as urllib2
import json
import glob
import IPython.display
import re


pd.options.display.max_columns = None

http = urllib.PoolManager()

# Load Facility Name to CMS ID json file
fac2CMS_file = 'IL_FacilityName_to_CMS_ID.json'
with open(fac2CMS_file) as f:
  ltc_name2cms_id = json.load(f) 

def getResponse(url):
    operUrl = http.request('GET', url)
    if(operUrl.status==200):
        data = operUrl.data
        jsonData = json.loads(data.decode('utf-8'))
    else:
        print("Error receiving data", operUrl.getcode())
    return jsonData

def facility2CMSNum (facilityName):
    regex = re.compile('\(\d\)')
    facilityName = regex.split(facilityName)[0].strip()
    if facilityName in ltc_name2cms_id:
        return ltc_name2cms_id[facilityName]
    else:
        return "No Match"
    
# df_facilities.reset_index(inplace=True) # Needed because used group by to get facility level data ToDo: COnsider moving this code up
# df_facilities['county-facName']= df_facilities['County'].str.upper() + '-' + df_facilities['FacilityName'].str.upper()
# df_facilities['CMS_ProvNum'] = df_facilities['county-facName'].apply(lambda x: facility2CMSNum(x))


def pull_IL_json_from_file(file):
    '''
    - Get IL data from JSON file
    
    Return: Reporting Date: str, DataFrame of Outbreak data: dict
    '''
    #Get IL data from JSON
    ltc_data = getResponse('https://idph.illinois.gov/DPHPublicInformation/api/covid/getltcdata')
    ltc_data_json = json.dumps(ltc_data)

    # Extract Reporting Data
    reporting_date = '%d-%02d-%02d' %(ltc_data['LastUpdateDate']['year'], ltc_data['LastUpdateDate']['month'], ltc_data['LastUpdateDate']['day'])

    #Saving a copy of source data 
    ltc_data_json = json.dumps(ltc_data)
    file = "Source_data/IL_" + reporting_date + "_LTC_data_Source.json"
    with open(file, "w") as f:
        f.write(ltc_data_json)
    
    # Get Reporting Date
    reporting_date = '%d-%02d-%02d' % (ltc_data['LastUpdateDate']['year'], ltc_data['LastUpdateDate']['month'], ltc_data['LastUpdateDate']['day'])

    return reporting_date, ltc_data

def pull_IL_json_from_web():
    '''
    - Get IL data from JSON
    - Store IL data in Source Data w/Date Stamp
    
    Return: Reporting Date: str, DataFrame of Outbreak data: dict
    '''
    #Get IL data from JSON
    ltc_data = getResponse('https://idph.illinois.gov/DPHPublicInformation/api/covid/getltcdata')
    ltc_data_json = json.dumps(ltc_data)

    # Extract Reporting Data
    reporting_date = '%d-%02d-%02d' %(ltc_data['LastUpdateDate']['year'], ltc_data['LastUpdateDate']['month'], ltc_data['LastUpdateDate']['day'])

    #Saving a copy of source data 
    ltc_data_json = json.dumps(ltc_data)
    file = "Source_data/IL_" + reporting_date + "_LTC_data_Source.json"
    with open(file, "w") as f:
        f.write(ltc_data_json)
    
    # Get Reporting Date
    reporting_date = '%d-%02d-%02d' % (ltc_data['LastUpdateDate']['year'], ltc_data['LastUpdateDate']['month'], ltc_data['LastUpdateDate']['day'])

    return reporting_date, ltc_data

def outbreak_df_from_file(outbreak_data, ltc_name2cms_id):
    """ From Json file:
        1) return DataFrame augmented and save to file
        2) return Summary data"""
    ltc_data = outbreak_data # TODO Refactor NAME
    

    
    # Extract Reporting Data
    reporting_date = '%d-%02d-%02d' %(ltc_data['LastUpdateDate']['year'], ltc_data['LastUpdateDate']['month'], ltc_data['LastUpdateDate']['day'])

    # Build DataFrame
    df = pd.DataFrame(ltc_data['FacilityValues'])
    df.insert(0, 'reporting_date', reporting_date)
    df['CFR'] = (df['deaths'] / df['confirmed_cases'])
    df['outbreaks'] = 1 # to allow counting # of outbreaks by Facility
    df['county-facName']= df['County'].str.upper() + '-' + df['FacilityName'].str.upper()
    df['CMS_ProvNum'] = df['county-facName'].apply(lambda x: facility2CMSNum(x))
    
    #Save Outbreak data to a file
    outbreak_file = 'Reporting_data/IL_' + reporting_date + '_Outbreaks_LTC_data_v4.csv'
    df.to_csv(outbreak_file, index = False)
    
    # Get summary data from feed - Note this may not match totals - ST-TODO: Check if summary data and totals from raw data match
    deaths = ltc_data['LTC_Reported_Cases']['deaths']
    confirmed_cases = ltc_data['LTC_Reported_Cases']['confirmed_cases']
    facility_cnt = len(df.groupby(['County', 'FacilityName']).size().reset_index().rename(columns={0:'count'}).sort_values(by='count', ascending=False))
    
    summary = {}
    summary['Date'] = reporting_date
    summary['Cases'] = confirmed_cases
    summary['Deaths'] = deaths
    summary['Outbreaks'] = df.reporting_date.value_counts()[0]
    summary['Open Outbreaks'] = df.status.value_counts()['Open']
    summary['Closed Outbreaks'] = df.status.value_counts()['Closed']
    summary['Facilities'] = facility_cnt
    
    return df, summary, reporting_date

def process_IL_dict(IL_data, ltc_name2cms_id, display_dfs=False, display_summary=True):
    '''Process a JSON file to:
       Inputs: 
           IL_data - Dictionary of outbreaks in IL for a particular date
           ltc_name2cms_id - Dictionary of Facility Names to CMS Federal Provider Numbers - Note can be more than one name for same number
           display_dfs - Flag to indicate whether or not to display top 10 values for each of the DataFrames
           display_summary - Flag to indicate whether or not to display Summary info
       Steps:
            1) Produce Summary Info
            2) Produce Outbreak file and dataframe
            3) Produce Facility file and dataframe
            4) Produce County file and dataframe
        
    '''
    [outbreak_df, summary, reporting_date] = outbreak_df_from_file(IL_data, ltc_name2cms_id)


    # Augment Outbreak DF to count open/closed
    outbreak_df['Closed_Outbreaks'] = outbreak_df['status'].apply(lambda x: 1 if x == "Closed" else 0)
    outbreak_df['Open_Outbreaks'] = outbreak_df['status'].apply(lambda x: 1 if x == "Open" else 0)

    
    # V2 - Remove cases of duplicate Outbreaks where they append (#) to end of Facility name to allow proper aggregation at the facility level
    regex = re.compile('\(\d\)')
    outbreak_df['FacilityName2'] = outbreak_df['FacilityName'].apply(lambda x: regex.split(x)[0].strip())

    
    # Save and Display Facility data
    df_facilities = outbreak_df.groupby(['County', 'FacilityName2', 'CMS_ProvNum']).sum()
    df_facilities['CFR'] = df_facilities['deaths'] / df_facilities['confirmed_cases']
    df_facilities['facilities'] = 1
    df_facilities.insert(0, 'ReportingDate', reporting_date)
    df_facilities.sort_values(by='confirmed_cases', ascending=False).to_csv('Reporting_data/IL_' + reporting_date + '_Facilities_LTC_data_v4.csv')


    summary['Facilities'] = len(df_facilities)
    if display_summary:
        for k,v in summary.items():
             print(k + ": " + str(v))
    
    # Save and Display County Level Data
    df_county = df_facilities.groupby(by=['County']).sum()
    df_county['CFR'] = (df_county['deaths'] / df_county['confirmed_cases'])
    df_county.insert(0, 'ReportingDate', reporting_date)
    filename = 'Reporting_data/IL_' + reporting_date + '_County_LTC_stats_v4.csv'
    df_county.sort_values('confirmed_cases', ascending=False).to_csv('Reporting_data/IL_' + reporting_date + '_County_LTC_stats_v4.csv')
    
    
    if display_dfs:
        print("\nOutbreak Data\n=============")
        display(outbreak_df.sort_values(by='deaths', ascending=False).head(10))
        print("\nFacility Data\n=============")
        display(df_facilities.sort_values('deaths', ascending=False).head(10))
        print("\nCounty Data\n===========")
        display(df_county.sort_values(by='confirmed_cases', ascending=False).head(10))

    return reporting_date, summary, outbreak_df, df_facilities, df_county
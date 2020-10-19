# %%

import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import io
import os
import re


# %%

if not os.path.exists('./outputs'):
    os.makedirs('./outputs')

# if os.path.isfile('./outputs/reference_df.csv'):
#     reference_df = pd.read_csv('./outputs/reference_df.csv', index_col=0)
# else:
reference_df = pd.DataFrame()
rawdata = {}

fullIndex_url = r"https://www.sec.gov/Archives/edgar/full-index/"

fullIndex = json.loads(requests.get(fullIndex_url + r"index.json").content)

for year in fullIndex['directory']['item']:

    if not bool(re.search(r"[0-9]{4}", year['href'])):
        continue
    
    year_url = fullIndex_url + year['href']
    yearIndex = json.loads(requests.get(year_url + r"index.json").content)
    print("Parsing data for " + year['name'] )
    
    for quarter in yearIndex['directory']['item']:
        
#         print(year['name']+quarter['name'])
        
        quarter_url = year_url + quarter['href']
        
        
        try:
        # Request by URL, decode, and read into pandas DF
            raw_df = pd.read_csv(io.StringIO(requests.get(quarter_url + r"master.idx").content.decode('utf-8')),
                    delimiter = '|',
                    skiprows = [0,1,2,3,4,5,6,7,8,10], # Standard format for .idx file
                    index_col=0
                    )
            
        # Insert raw data into dictionary for later use
            filtered_df = raw_df[raw_df["Form Type"].str.match(r"10-[QK]$")]
            filtered_df["Form"] = filtered_df["Form Type"].str.extract(r"([QK])")
            filtered_df = filtered_df.groupby('CIK').aggregate({'Form':', '.join, 'Company Name': 'first', 'Filename': 'first'})
            rawdata[ str(year['name'] + quarter['name']) ] = filtered_df
            
        # Pull only 10-Q & 10-K forms
            df = raw_df[raw_df['Form Type'].str.match(r"10-[QK]$")]["Form Type"].str.extract(r"([QK])")
            df = df.groupby("CIK").aggregate({0: ', '.join})
            df.columns = [ year['name']+ quarter['name']]
            reference_df = reference_df.join(df, how='outer')


        except:
        #     # Print error, and create empty DataFrame for join purposes
            print(year['name']+quarter['name'] + " could not be parsed.")
            # df = pd.DataFrame(index=['CIK'],columns=[ year['name']+ quarter['name']])

        


reference_df.to_csv(r"./outputs/reference_df.csv")
rawdatajson = {
    key: rawdata[key].to_dict(orient='index')
    for key in rawdata.keys()
}

with open('./outputs/rawdata_test.json', 'w') as f:
    json.dump(rawdatajson, f)

print('here')


# This chooses the CIK's that have a filing every quarter from 2000-2009
reference_df[pd.Series(reference_df.filter(regex=("200")).count(axis=1)==40)].filter(regex=("200")) 


        

        

        
        
    
    
# year = json.loads(requests.get(fullIndex_url + fullIndex['directory']['item'][0]['href'] + r"index.json").content)
# quarter = json.loads(requests.get(fullIndex + year['directory']['item'][0]['href'] +  + r"index.json").content)
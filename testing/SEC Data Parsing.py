import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import io

reference_df = pd.DataFrame()
rawdata = {}

fullIndex_url = r"https://www.sec.gov/Archives/edgar/full-index/"

fullIndex = json.loads(requests.get(fullIndex_url + r"index.json").content)

for year in fullIndex['directory']['item']:
    
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
            rawdata[ str(year['name'] + quarter['name']) ] = raw_df
            
            # Pull only 10-Q & 10-K forms
            df = raw_df[raw_df['Form Type'].str.match(r"10-[QK]$")]["Form Type"].str.extract(r"([QK])")
            df = df.groupby("CIK").aggregate({0: ', '.join})
            df.columns = [ year['name']+ quarter['name']]

            reference_df = df.join(reference_df)

        except:
            print(year['name']+quarter['name'] + " did not contain data.")
            df = pd.DataFrame(index=['CIK'],columns=[ year['name']+ quarter['name']])
        
        try:
            reference_df = df.join(reference_df, how='left')
        except:
            print('Could not join ' + year['name'] + quarter['name'])
#         print(reference_df)


        

        
        
    
    
# year = json.loads(requests.get(fullIndex_url + fullIndex['directory']['item'][0]['href'] + r"index.json").content)
# quarter = json.loads(requests.get(fullIndex + year['directory']['item'][0]['href'] +  + r"index.json").content)
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import urllib.request

proxy_support = urllib.request.ProxyHandler({'http': 'http://brd-customer-hl_ef20981d-zone-web_unlocker_test:r8yjzk22g9ep@brd.superproxy.io:22225',
                                             'https': 'http://brd-customer-hl_ef20981d-zone-web_unlocker_test:r8yjzk22g9ep@brd.superproxy.io:22225'})
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

url = "https://idx.co.id/secondary/get/esg?pageSize=10000"
with urllib.request.urlopen(url) as response:
    html = response.read()
    print(html)

data = json.loads(html)
esg_symbols = [entry['TickerCode'] for entry in data['result']]
print(esg_symbols)

esg_data_list = []
for symbol in esg_symbols:

    url = f'https://idx.co.id/secondary/get/esg/detail/{symbol}?language=en-us'
    with urllib.request.urlopen(url) as response:
        html = response.read()
        data = json.loads(html)

        esg_data = {
            'symbol': data['result'][0].get('TickerCode', '')+'.JK',
            'last_esg_update_date': pd.Timestamp(data['result'][0].get('LastUpdate', '')).strftime("%Y%m%d_%H%M%S"),
            'esg_score': data['result'][0].get('ESGScore', ''),
            'controversy_risk': int(data['result'][0].get('ControversyRiskScore', '')),
            'environment_risk_score': data['result'][0].get('EnvironmentRiskScore', ''),
            'social_risk_score': data['result'][0].get('SocialRiskScore', ''),
            'governance_risk_score': data['result'][0].get('GovernanceRiskScore', ''),
            'updated_on': pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        }


        print(esg_data)
        esg_data_list.append(esg_data)

    try:
        response = supabase.table('idx_esg_score').upsert(esg_data).execute()
        if response.data[0]['esg_score'] is not None:
            print(f"Upserted data for symbol: {symbol}")
        else:
            print(f"Updated ESG Score still None: {symbol}")
    except:
        print(f"Error upserting data for symbol: {symbol}")



# Create a DataFrame
df = pd.DataFrame(esg_data_list)
df.to_csv('esg2.csv', index = False)


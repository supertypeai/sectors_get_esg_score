from datetime import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
import json
import os
from dotenv import load_dotenv
load_dotenv()
import os
from supabase import create_client
import requests

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
chrome_options = Options()
options = [
    # "--headless",
    f"--user-agent={user_agent}",
]
for option in options:
    chrome_options.add_argument(option)

# service = Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install())
service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=service, options=chrome_options)
url = "https://idx.co.id/secondary/get/esg?pageSize=10000"
driver.get(url)


wait = WebDriverWait(driver,60)
pre_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'pre')))
wait.until(lambda driver: pre_element.text.strip() != '')

data = json.loads(pre_element.text)
esg_symbols = [entry['TickerCode'] for entry in data['result']]
print(esg_symbols)

esg_data_list = []
for symbol in esg_symbols:

    url = f'https://idx.co.id/secondary/get/esg/detail/{symbol}?language=en-us'
    driver.get(url)

    wait = WebDriverWait(driver,60)
    pre_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'pre')))
    wait.until(lambda driver: pre_element.text.strip() != '')

    data = json.loads(pre_element.text)

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
df.to_csv('esg.csv', index = False)


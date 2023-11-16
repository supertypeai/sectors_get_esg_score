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
for symbol in esg_symbols[:3]:

    url = f'https://idx.co.id/secondary/get/esg/detail/{symbol}?language=en-us'
    driver.get(url)

    wait = WebDriverWait(driver,60)
    pre_element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'pre')))
    wait.until(lambda driver: pre_element.text.strip() != '')

    data = json.loads(pre_element.text)

    esg_data = {
        'symbol': data['result'][0].get('TickerCode', ''),
        'last_esg_update_date': data['result'][0].get('LastUpdate', ''),
        'esg_score': data['result'][0].get('ESGScore', ''),
        'controversy_risk': data['result'][0].get('ControversyRiskScore', ''),
        'environment_risk_score': data['result'][0].get('EnvironmentRiskScore', ''),
        'social_risk_score': data['result'][0].get('SocialRiskScore', ''),
        'governance_risk_score': data['result'][0].get('GovernanceRiskScore', ''),
        'updated_on': datetime.now()
    }

    print(esg_data)

    esg_data_list.append(esg_data)
# Create a DataFrame
df = pd.DataFrame(esg_data_list)
df.to_csv('esg.csv', index = False)


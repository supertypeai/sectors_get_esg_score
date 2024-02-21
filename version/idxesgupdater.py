from datetime import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait


class IdxEsgUpdater:
    def __init__(self, chrome_driver_path="./chromedriver.exe"):
        self.new_data = {}
        self.chrome_driver_path = chrome_driver_path
        self.chrome_options = webdriver.ChromeOptions()
        # self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
    
    def update_esg_data(self):
        def retrieve_esg_symbols(driver):
            url = "https://idx.co.id/en/listed-companies/esg-score"
            driver.get(url)

            wait = WebDriverWait(driver, 20)
            select_element = wait.until(EC.visibility_of_element_located((By.NAME, "perPageSelect")))
            select = Select(select_element)
            select.select_by_value("-1")
            bs = BeautifulSoup(driver.page_source, "lxml")

            esg_symbols = []
            table = bs.find("table", id="vgt-table")
            tbody = table.find("tbody")
            for row in tbody.find_all("tr"):
                symbol = row.find("td").text.strip()
                esg_symbols.append(symbol)

            return pd.DataFrame({"symbol": esg_symbols})

        def retrieve_esg_data(driver, symbol):
            driver.get(f'https://idx.co.id/en/listed-companies/esg-score/{symbol}')
            wait = WebDriverWait(driver, 20)

            try:
                values = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//*[@class="text-value"]')))
                date_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div[2]/main/div[1]/div[2]/div/p/i')))

                date = date_element.text.replace("*Last Update: ", "")
                date = datetime.strptime(date, "%d %B %Y %H:%M:%S")
                
                esg = []

                for v in values:
                    if v.text != 'N/A':
                        esg.append(float(v.text))
                    else:
                        esg.append(np.nan)

                data = {
                    'symbol': [symbol],
                    'last_esg_update_date': [date],
                    'esg_score': [esg[0]],
                    'controversy_risk': [esg[1]],
                    'environment_risk_score': [esg[2]],
                    'social_risk_score': [esg[3]],
                    'governance_risk_score': [esg[4]],
                    'updated_on': datetime.now()
                }

                return pd.DataFrame(data)
            except:
                print(f"Failed to retrieve esg data for {symbol}")

        driver = webdriver.Chrome(service=Service(self.chrome_driver_path),options=self.chrome_options)

        try:
            esg_symbols_df = retrieve_esg_symbols(driver)
            esg_data = pd.concat([retrieve_esg_data(driver, symbol) for symbol in sorted(esg_symbols_df["symbol"])], ignore_index=True)

            esg_data["symbol"] = esg_data["symbol"].apply(lambda x: x + ".JK")   

            self.new_data["idx_esg"] = esg_data
        finally:
            driver.quit()

    def update_all_data(self, save_to_csv=True):
        self.update_esg_data()
        if save_to_csv:
            date_now = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            for file_prefix, data in self.new_data.items():
                data.to_csv(f"{file_prefix}_{date_now}.csv", index=False)


if __name__ == "__main__":
    updater = IdxEsgUpdater()
    updater.update_all_data()


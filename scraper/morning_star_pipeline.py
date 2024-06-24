import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
import ssl
import urllib.request
ssl._create_default_https_context = ssl._create_unverified_context

class MorningStarPipeline:
    """ Parameter explained : 
        industry        : input for industry categories, you can input categories like Banks, Automobile, Aerospace, etc.
                          for detail of industry, you can check filter in the MS Website on link : https://www.sustainalytics.com/esg-rating

        rating          : Input for ESG Score rating. ESG Score is divided into several categories.
                          0: Negiligible (0 - 10)
                          1: Low (10 - 20)
                          2: Medium (20 - 30)
                          3: High (30 - 40)
                          4: Severe (40+)

                          To used rating filter, you must input the number of rating, like 0, 1, 3, 4.
        
        name            : Input for company's name. you can input name like PT Vale Indonesia Tbk, 11 Bit Studios SA, etc.
                          you also can input the ticker like JKT:INCO, NAS:FLWS, etc.

        amount           : input for the amount of data you want to retrieve, like 10, 20, 30, 1000, etc

        country          : Input for certain country, like Indonesia, Singapura, Malaysia

        Note             : For retrieve all data on MS. you must input industry, rating, and filter with "" (empty-string, not whitespace)
    """
    def __init__(self, industry, rating,  name, amount, country, proxy):
        self.url = "https://www.sustainalytics.com/sustapi/companyratings/getcompanyratings"
        self.industry = industry
        self.rating = rating
        self.name = name
        self.amount = amount
        self.country = country
        self.proxy = proxy

    def get_company_name(self, soup):
        try :
            company_name = soup.find("div", {"class": "company-name"})
            # print(company_name)
            return company_name.text.strip()
        except:
            return None
    
    def get_esg_score(self, soup):
        try : 
            esg_score = soup.find("div", {"class": "risk-rating-score"})
            return float(esg_score.text.strip())
        except : 
            return None
        
    def get_industry_rank(self, soup) :
        try:
            industry_rank = soup.find("strong", {"class": "industry-group-position"})
            return int(industry_rank.text)
        except :
            return None 
    
    def get_total_companies_on_industry_categories(self, soup):
        try:
            industry_total = soup.find("span", {"class": "industry-group-positions-total"} )
            return int(industry_total.text) 
        except :
            return None 
        
    def get_global_rank(self, soup):
        try : 
            universe_rank = soup.find("strong", {"class": "universe-position"})
            return int(universe_rank.text)
        except :
            return None
        
    def get_total_companies_on_ms(self, soup):
        try : 
            universe_total = soup.find("span", {"class": "universe-positions-total"} )
            return int(universe_total.text)
        except :
            return None
        
    def get_last_update_date(self, soup):
        try : 
            last_update = soup.find_all("div",{"class":"update-date"})

            date_data_list = []

            for i in last_update:
                date_data = i.find("strong")
                date_data_list.append(date_data.text)

            return date_data_list
        except :
            return []
        
    def get_industy_group(self, soup):
        try :
            industry_group = soup.find("strong", {"class": "industry-group"})
            return industry_group.text 
        except :
            return None
        
    def get_country(self, soup):
        try :
            country = soup.find("strong", {"class": "country"})
            return country.text 
        except:
            return None
        
    def get_identifer(self, soup) :
        try :
            identifier = soup.find("strong", {"class": "identifier"})
            return identifier.text
        except:
            return None 
    
    def get_company_data(self, url):
        # print("testin")
        response = requests.get(url)
        company_data = dict()

        # proxy = os.environ.get("proxy")

        proxy_support = urllib.request.ProxyHandler({'http': self.proxy,'https': self.proxy})
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)

        with urllib.request.urlopen(url) as response:
            html = response.read()
            
            soup = BeautifulSoup(html, 'html.parser')

            # company_name = self.get_company_name(soup)

            # company_data["company_name"] = company_name

            esg_score = self.get_esg_score(soup)
            company_data["esg_score"] = esg_score

            industry_rank = self.get_industry_rank(soup)
            company_data["industry_rank"] = industry_rank

            industry_total = self.get_total_companies_on_industry_categories(soup)
            company_data["total_companies_on_industry"] = industry_total

            global_rank = self.get_global_rank(soup)
            company_data["global_rank"] = global_rank

            universe_total = self.get_total_companies_on_ms(soup)
            company_data["total_companies_on_ms"] = universe_total


            last_update_list = self.get_last_update_date(soup)

            if len(last_update_list) == 2:
                last_full_update = last_update_list[0]
                company_data["last_esg_full_update_date"] = last_full_update

                last_update = last_update_list[1]
                company_data["last_esg_update_date"] = last_update
            elif len(last_update_list) == 1 :
                # last_full_update = last_update_list[0]
                company_data["last_esg_full_update_date"] = None

                last_update = last_update_list[0]
                company_data["last_esg_update_date"] = last_update

            else :
                last_full_update = None
                company_data["last_esg_full_update_date"] = None

                last_update = None
                company_data["last_esg_update_date"] = None

            if company_data["last_esg_full_update_date"] is not None :
                date_obj = datetime.strptime(company_data["last_esg_full_update_date"], "%b %d, %Y")
                date_final = date_obj.strftime("%Y%m%d_%H%M%S")
                company_data["last_esg_full_update_date"] = date_final

            if company_data["last_esg_update_date"] is not None :
                date_obj = datetime.strptime(company_data["last_esg_update_date"], "%b %d, %Y")
                date_final = date_obj.strftime("%Y%m%d_%H%M%S")
                company_data["last_esg_update_date"] = date_final


            industry_group = self.get_industy_group(soup)
            company_data["industry_group"] = industry_group

            country = self.get_country(soup)
            company_data["country"] = country

            identifier = self.get_identifer(soup)
            company_data["symbol"] = identifier 

            return company_data


    #     # if response.status_code == 200 :

    #     # else :
    #     #     company_data["error"] = f'Failed to retrieve data: {response.status_code}'
    #     #     return company_data
        
    # def get_2(self):
    #     url = self.url
        

    #     # Data yang akan dikirim dalam POST request
    #     payload = {
    #         "industry" : self.industry,
    #         "rating" : self.rating,
    #         "filter" : self.name,
    #         "page": 1,
    #         "pageSize": self.amount,
    #         "resourcePackage": "Sustainalytics"
    #     }
    #     # Headers (opsional)
    #     headers = {
    #         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    #     }
    #     proxy = os.environ.get("proxy")

    #     proxies = {
    #         'http': proxy,
    #         'https': proxy,
    #         }
        
    #     print(proxies)

    #     # Mengirim POST request
    #     response = requests.post(url, data=payload, headers=headers, proxies=proxies)
    #     print(response.text)

    #     all_data_company = []
    #     # print("testing")


    #     if response.status_code == 200:
    #         soup = BeautifulSoup(response.text, 'html.parser')
            
    #         results = soup.find_all("div", {"class": "company-row"})

    #         for i in results: 
    #             href_a = i.find("a", attrs={'data-href': True})

    #             url = f"https://www.sustainalytics.com/esg-rating/{href_a['data-href']}"
    #             company_data = self.get_company_data(url)

    #             if len(self.country) > 0:
    #                 if company_data["country"] in self.country and company_data["symbol"] != "-":
    #                     all_data_company.append(company_data)

    #                     print(company_data["symbol"])
    #             else :
    #                 if company_data["symbol"] != "-":
    #                     all_data_company.append(company_data)

    #                     print(company_data["symbol"])
    #         return all_data_company

    #     else:
    #         return all_data_company

    def get(self):
        url = self.url
        ssl._create_default_https_context = ssl._create_unverified_context

        # Data yang akan dikirim dalam POST request
        data = {
            "industry" : self.industry,
            "rating" : self.rating,
            "filter" : self.name,
            "page": 1,
            "pageSize": self.amount,
            "resourcePackage": "Sustainalytics"
        }

        # proxy = os.environ.get("proxy")

        proxy_support = urllib.request.ProxyHandler({'http': self.proxy,'https': self.proxy})
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)

        # Mengkodekan data
        data_encoded = urllib.parse.urlencode(data).encode('utf-8')

        # Membuat objek Request
        request = urllib.request.Request(url, data=data_encoded)

        # Menambahkan header (jika diperlukan)
        request.add_header('Content-Type', 'application/x-www-form-urlencoded')

        all_data_company = []

        # Mengirim permintaan dan membaca respons
        try:
            with urllib.request.urlopen(request) as response:
                response_data = response.read()
                html = response_data.decode('utf-8')
                # print(response_data.decode('utf-8'))

                soup = BeautifulSoup(html, 'html.parser')
            
                results = soup.find_all("div", {"class": "company-row"})

                for i in results: 
                    href_a = i.find("a", attrs={'data-href': True})

                    url = f"https://www.sustainalytics.com/esg-rating/{href_a['data-href']}"
                    # print(url)

                    company_data = self.get_company_data(url)


                    if len(self.country) > 0:
                        if company_data["country"] in self.country and company_data["symbol"] != "-":
                            all_data_company.append(company_data)

                            print(company_data["symbol"])
                    else :
                        if company_data["symbol"] != "-":
                            all_data_company.append(company_data)

                            print(company_data["symbol"])
        except urllib.error.URLError as e:
            print(f'Error: {e.reason}')

        return all_data_company




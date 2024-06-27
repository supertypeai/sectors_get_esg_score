import pandas as pd
from scraper.morning_star_pipeline import MorningStarPipeline
import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
import ssl
import time
ssl._create_default_https_context = ssl._create_unverified_context
start_time = time.time()
from multiprocessing import Process

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

response_list_company = supabase.table('idx_esg_score').select('symbol').execute()

list_company = response_list_company.data
df_list_company = pd.DataFrame(list_company)

companies_in_id_list = list(df_list_company["symbol"])

companies_id = []
# print(companies_in_id_list)

for i in companies_in_id_list:
    get_symbol = i.split(".")
    symbol_identifier = "JKT:" + get_symbol[0]
    companies_id.append(symbol_identifier)

result_companies_with_esg_score = []

def scraping_ms1(companies_id):
    for i in companies_id: 
        
        print(f"[Start to retrieve - {i}]")

        proxy = os.environ.get("proxy")
        cookies = os.environ.get("cookies")
        # print(cookies)
        pipeline = MorningStarPipeline("", "", i, 10, ["Indonesia"], proxy, cookies)

        # # print(len(pipeline.get()))
        results = pipeline.get()
        # print(results)

        if len(results) > 0 :
            identifier = results[0]["symbol"]
            symb_split = identifier.split(":")
            symb = symb_split[1] + ".JK"
            results[0]["symbol"] = symb
    
            del results[0]['country']

            results[0]["updated_on"] = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            # response = supabase.table('idx_esg_score').upsert(results[0]).execute()
            # # print(response)
            # try :
            #     if response.data[0]['esg_score'] is not None:
            #         print(f"Upserted data for symbol: {results[0]['symbol']}")
            #     else:
            #         print(f"Updated ESG Score still None: {results[0]['symbol']}")
            # except:
            #     print(f"Error upserting data for symbol: {results[0]['symbol']}")

            # result_companies_with_esg_score.append(results[0])

        else :
            print(f"{i} haven't registered on MS")

        print(f"[End to retrieve - {i}]")

    df_esg_ms = pd.DataFrame(result_companies_with_esg_score)
    df_esg_ms.to_csv("id_esg_score1.csv", index = False)
    
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Waktu eksekusi: {execution_time} detik")

def scraping_ms2(companies_id):
    for i in companies_id: 
        
        print(f"[Start to retrieve - {i}]")

        proxy = os.environ.get("proxy")
        cookies = os.environ.get("cookies")
        # print(cookies)
        pipeline = MorningStarPipeline("", "", i, 10, ["Indonesia"], proxy, cookies)

        # # print(len(pipeline.get()))
        results = pipeline.get()
        # print(results)

        if len(results) > 0 :
            identifier = results[0]["symbol"]
            symb_split = identifier.split(":")
            symb = symb_split[1] + ".JK"
            results[0]["symbol"] = symb
    
            del results[0]['country']

            results[0]["updated_on"] = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            # response = supabase.table('idx_esg_score').upsert(results[0]).execute()
            # # print(response)
            # try :
            #     if response.data[0]['esg_score'] is not None:
            #         print(f"Upserted data for symbol: {results[0]['symbol']}")
            #     else:
            #         print(f"Updated ESG Score still None: {results[0]['symbol']}")
            # except:
            #     print(f"Error upserting data for symbol: {results[0]['symbol']}")

            # result_companies_with_esg_score.append(results[0])

        else :
            print(f"{i} haven't registered on MS")

        print(f"[End to retrieve - {i}]")

    df_esg_ms = pd.DataFrame(result_companies_with_esg_score)
    df_esg_ms.to_csv("id_esg_score2.csv", index = False)
    
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Waktu eksekusi: {execution_time} detik")

print(companies_id[0:5])
print(companies_id[5:10])
if __name__ == "__main__": 
    # scraping_ms1(companies_id[0:10])
    p1 = Process(target=scraping_ms1, args=(companies_id[0:5], ))
    p2 = Process(target=scraping_ms2, args=(companies_id[5:10], ))

    p1.start()
    p2.start()

    p1.join()
    p2.join()

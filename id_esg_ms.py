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

response_list_company = supabase.table('idx_company_profile').select('symbol').execute()

list_company = response_list_company.data
df_list_company = pd.DataFrame(list_company)

companies_in_id_list = list(df_list_company["symbol"])

companies_id = []
# print(companies_in_id_list)

for i in companies_in_id_list:
    get_symbol = i.split(".")
    symbol_identifier = "JKT:" + get_symbol[0]
    companies_id.append(symbol_identifier)

def scraping_ms1(companies_id):
    result_companies_with_esg_score = []
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
            response = supabase.table('idx_esg_score').upsert(results[0]).execute()
            # print(response)
            try :
                if response.data[0]['esg_score'] is not None:
                    print(f"Upserted data for symbol: {results[0]['symbol']}")
                else:
                    print(f"Updated ESG Score still None: {results[0]['symbol']}")
            except:
                print(f"Error upserting data for symbol: {results[0]['symbol']}")

            result_companies_with_esg_score.append(results[0])

        else :
            print(f"{i} haven't registered on MS")

        print(f"[End to retrieve - {i}]")

    df_esg_ms = pd.DataFrame(result_companies_with_esg_score)
    df_esg_ms.to_csv("id_esg_score1.csv", index = False)
    
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Waktu eksekusi: {execution_time} detik")

def scraping_ms2(companies_id):
    result_companies_with_esg_score = []
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
            response = supabase.table('idx_esg_score').upsert(results[0]).execute()
            # print(response)
            try :
                if response.data[0]['esg_score'] is not None:
                    print(f"Upserted data for symbol: {results[0]['symbol']}")
                else:
                    print(f"Updated ESG Score still None: {results[0]['symbol']}")
            except:
                print(f"Error upserting data for symbol: {results[0]['symbol']}")

            result_companies_with_esg_score.append(results[0])

        else :
            print(f"{i} haven't registered on MS")

        print(f"[End to retrieve - {i}]")

    df_esg_ms = pd.DataFrame(result_companies_with_esg_score)
    df_esg_ms.to_csv("id_esg_score2.csv", index = False)
            
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Waktu eksekusi: {execution_time} detik")
        
def scraping_ms3(companies_id):
    result_companies_with_esg_score = []
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
            response = supabase.table('idx_esg_score').upsert(results[0]).execute()
            # print(response)
            try :
                if response.data[0]['esg_score'] is not None:
                    print(f"Upserted data for symbol: {results[0]['symbol']}")
                else:
                    print(f"Updated ESG Score still None: {results[0]['symbol']}")
            except:
                print(f"Error upserting data for symbol: {results[0]['symbol']}")

            result_companies_with_esg_score.append(results[0])

        else :
            print(f"{i} haven't registered on MS")

        print(f"[End to retrieve - {i}]")
    
    df_esg_ms = pd.DataFrame(result_companies_with_esg_score)
    df_esg_ms.to_csv("id_esg_score3.csv", index = False)
            
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Waktu eksekusi: {execution_time} detik")

def scraping_ms4(companies_id):
    result_companies_with_esg_score = []
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
            response = supabase.table('idx_esg_score').upsert(results[0]).execute()
            # print(response)
            try :
                if response.data[0]['esg_score'] is not None:
                    print(f"Upserted data for symbol: {results[0]['symbol']}")
                else:
                    print(f"Updated ESG Score still None: {results[0]['symbol']}")
            except:
                print(f"Error upserting data for symbol: {results[0]['symbol']}")

            result_companies_with_esg_score.append(results[0])

        else :
            print(f"{i} haven't registered on MS")

        print(f"[End to retrieve - {i}]")

    
    df_esg_ms = pd.DataFrame(result_companies_with_esg_score)
    df_esg_ms.to_csv("id_esg_score4.csv", index = False)
            
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Waktu eksekusi: {execution_time} detik")

# print(companies_id[0:5])
# print(companies_id[5:10])

if __name__ == "__main__": 
    # scraping_ms1(companies_id[0:10])
    lenght_comp_id = len(companies_id)
    i1 = int(lenght_comp_id / 4)
    i2 = i1 + int(lenght_comp_id / 4)
    i3 = i2 + int(lenght_comp_id / 4)

    # print(lenght_comp_id)
    # print(i1)
    # print(i2)
    # print(i3)


    p1 = Process(target=scraping_ms1, args=(companies_id[0:i1], ))
    p2 = Process(target=scraping_ms2, args=(companies_id[i1:i2], ))
    p3 = Process(target=scraping_ms3, args=(companies_id[i2:i3], ))
    p4 = Process(target=scraping_ms4, args=(companies_id[i3:lenght_comp_id], ))

    p1.start()
    p2.start()
    p3.start()
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()

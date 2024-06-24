import pandas as pd
from scraper.morning_star_pipeline import MorningStarPipeline
import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

response_list_company = supabase.table('idx_company_profile').select('symbol').execute()

list_company = response_list_company.data
df_list_company = pd.DataFrame(list_company)

companies_in_id_list = list(df_list_company["symbol"])

companies_id = []

for i in companies_in_id_list:
    get_symbol = i.split(".")
    symbol_identifier = "JKT:" + get_symbol[0]
    companies_id.append(symbol_identifier)

result_companies_with_esg_score = []

for i in companies_id: 
    print(f"[Start to retrieve - {i}]")

    response = supabase.table('idx_esg_score').select('*').execute()

    # Mendapatkan data dalam bentuk dictionary
    data = response.data

    df_from_supabase = pd.DataFrame(data)
    df_idx = df_from_supabase.fillna("")
    proxy = os.environ.get("proxy")
    pipeline = MorningStarPipeline("", "", i, 10, ["Indonesia"], proxy)

    # # print(len(pipeline.get()))
    results = pipeline.get()
    # print(results)

    if len(results) > 0 :
        identifier = results[0]["symbol"]
        symb_split = identifier.split(":")
        symb = symb_split[1] + ".JK"
        results[0]["symbol"] = symb
        results[0]["controversy_risk"] = None
        results[0]["environment_risk_score"] = None
        results[0]["social_risk_score"] = None
        results[0]["governance_risk_score"] = None
        del results[0]['country']

        if  results[0]["symbol"] in list(df_idx["symbol"]):
            try :
                results[0]["controversy_risk"] = int(df_idx[df_idx["symbol"] ==  results[0]["symbol"]]["controversy_risk"].iloc[0])
            except:
                results[0]["controversy_risk"] = None
            
            try : 
                results[0]["environment_risk_score"] = float(df_idx[df_idx["symbol"] ==  results[0]["symbol"]]["environment_risk_score"].iloc[0])
            except :
                results[0]["environment_risk_score"] = None

            try :
                results[0]["social_risk_score"] = float(df_idx[df_idx["symbol"] ==  results[0]["symbol"]]["social_risk_score"].iloc[0])
            except :
                results[0]["social_risk_score"] = None

            try : 
                results[0]["governance_risk_score"] = float(df_idx[df_idx["symbol"] ==  results[0]["symbol"]]["governance_risk_score"].iloc[0])
            except :
                results[0]["governance_risk_score"] = None
            

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
df_esg_ms.to_csv("id_esg_score.csv", index = False)

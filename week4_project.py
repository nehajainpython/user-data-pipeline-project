import requests
import pandas as pd
import sqlite3
def get_data():
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    return response

def inspect_data(response):
    if response.status_code == 200:
        data = response.json()

        #print(type(data))        # list
        #print(data[0].keys())
        #print(type(data[0]["company"]))
        #print(type(data[0]["address"]))   # keys of first user

    elif response.status_code == 404:
        print("page is not found")

    elif response.status_code == 500:
        print("internal server error")

    else:
        print("data is not found")

    return data
def extract_data(data):
    df=pd.json_normalize(data)
    df.rename(columns=lambda x: x.replace('.', '_').lower(), inplace=True)
    return df

def main():
    response = get_data()
    data = inspect_data(response)
    df=extract_data(data)
    #print(df.columns)
    df.isna().sum()
    df.dropna(inplace=True)
    df.fillna("NA",inplace=True)
    df["id"]=df["id"].astype(int)
    df.drop_duplicates(inplace=True)
    user_df= df[["id","name","username","email","phone","website"]]
    company_df = df[["id","company_name","company_catchphrase","company_bs"]]
    address_df = df[["id","address_street","address_suite","address_city","address_zipcode","address_geo_lat","address_geo_lng"]]
    
    address_df.rename(columns={"id": "user_id"}, inplace=True)
    company_df.rename(columns={"id":"user_id"},inplace=True)
    conn = sqlite3.connect("users_project.db")
    user_df.to_sql("user",conn,if_exists="replace",index=False)
    address_df.to_sql("address",conn,if_exists="replace",index=False)
    company_df.to_sql("company",conn,if_exists="replace",index=False)
    query = "SELECT COUNT(*) FROM user"
    result = pd.read_sql(query, conn)
    print(result)
    
    #query = "PRAGMA table_info(address)"
    #print(pd.read_sql(query, conn))
    query="SELECT address_city, COUNT(*) as total  FROM address GROUP BY address_city "
    result = pd.read_sql(query, conn)
    #print(result)
    #query = "PRAGMA table_info(company)"
    #print(pd.read_sql(query, conn))
    query="SELECT u.name , c.company_name from user as u JOIN company as C on u.id==c.user_id "
    result = pd.read_sql(query, conn)
    #print(result)
    query="SELECT u.name, a.address_city, c.company_name FROM user u JOIN address a ON u.id = a.user_id JOIN company c ON u.id = c.user_id"
    result = pd.read_sql(query, conn)
    #print(result)
    query="SELECT count(*), company_name from company GROUP BY company_name "
    result = pd.read_sql(query, conn)
    print(result)
    query="SELECT u.name,a.address_city from user u JOIN address a ON u.id=a.user_id "
    result = pd.read_sql(query, conn)
    print(result)

    
    
    
    #print(df.head())
if __name__ == "__main__":
    main()
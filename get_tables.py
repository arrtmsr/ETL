from pathlib import Path, PurePath
import pandas as pd
import os

file_name = 'sample'
file_dir = Path(__file__).parent.resolve()
file_dir = PurePath(file_dir, file_name)

df = pd.read_csv(f'{file_dir}.csv', sep=';', dtype=str)


def get_tables(df: pd.DataFrame, file_name: str) -> None:
    
    """
    Getting table by category with Category and Vendor name.

    Source table columns: 
    Product code, Language, Product manufacturer code, Product name, Category, Features

    Exported table column: 
    Product code, Language, Product manufacturer code, Product name, Category, Vendor name, Features
    """    
    
    # Extracting merchID
    def extract_merch_id(string: str) -> str:
        features = str(string).split('; ')
        for i in features:
            if str(i).startswith('ID мерчанта'):
                return i.split('[')[-1].split(']')[0]

    # Convert merchID to vendor name
    def convert_to_vendor_name(value: str) -> str:

        vendor_data = {

                'merch1': '563dd299-4945-4fb1-bdfa-80c8dbaf185b',
                'merch2': 'd4450bd6-ee57-41a6-8cee-a0854729bd2c',
                'merch3': '9b0dd044-d8ac-40d4-a5d9-9c64338e1905',
                'merch4': 'd985be83-64c8-47b9-b849-c027413eac3c',
                'merch5': '3ea2f9b1-fe3a-42a9-95c4-13edd997b469',
                'merch6': '11d4a18e-d592-4032-944e-336805dc4f86',
                'merch7': 'f7e5dbb3-c7cb-43d7-9b65-e84d2c3cf449',
                'merch8': '4aca7441-c586-40fd-abda-1c8ebdd98f6a',
                'merch9': '5c8c0e14-8e9c-4aaa-9ed5-16449ed92837',
                'merch10': '13e489e8-1bf6-4ede-a2c5-893d4ce2a75f',
                'merch11': '7531cf4d-0cd2-4b57-8c7c-285a6636e1f3',
                'merch12': 'a911a830-29b0-436b-a44d-a2b365b898a4',
                'merch13': '6fe75067-0477-4d30-949d-d8209f00ef1f',
                'merch14': 'f7a36b51-895e-4dcd-945b-bfaeea4f8002',
                'merch15': '6ba6599b-fe70-4da3-abd0-51ca4e0836b0',
                'merch16': '511ed651-fed5-43b6-b4ce-2dc72f7105b7',
                'merch17': 'fa2bd42f-cdd5-436a-82ed-c5527eba0e0c',
                'merch18': 'c719458b-9f8f-4607-982d-62b5446654e0',
                'merch19': '7e8abcea-1f9a-4e0f-87c3-5a317e9c1340',

                }

        for vendor_name, merch_id in vendor_data.items():
            if value == merch_id:
                return vendor_name
    
    # Convert string to category name
    def get_category(string: str) -> str:

        categories = {

                    'Стулья': 'Каталог///Стулья', 
                    'Столы': 'Каталог///Столы',
                    # 'Ковры': 'Каталог///Ковры',
                    # 'Компьютерные кресла': 'Каталог///Кресла и пуфы///Компьютерные кресла',

                    }

        for category, startswith in categories.items():
            if str(string).startswith(startswith):
                return category

    # Query and save table by name
    def query_tables(df: pd.DataFrame, file_name: str) -> None:

        categories = {

                'Стулья': 'stulya', 
                'Столы': 'stoly',
                # 'Ковры': 'kovry',                
                # 'Компьютерные кресла': 'komp_kresla',
                
                }

        for name, export_name in categories.items():
            df.loc[df['Category'] == name].to_csv(f'{export_name}_{file_name}.csv', index=False, sep=';')    


    # Getting table with Vendor and Category name
    # TODO: rename Category to Category name
    df['merchID'] = df['Features'].apply(lambda x: extract_merch_id(x))
    df['merchID'] = df['merchID'].apply(lambda x: convert_to_vendor_name(x))
    df['Category'] = df['Category'].apply(lambda x: get_category(x))
    df.rename(columns={'merchID': 'Vendor name'}, inplace=True)
    
    # Changing columns order
    columns = df.columns.to_list()
    columns = columns[:-2] + [columns[-1], columns[-2]]
    df = df[columns]

    query_tables(df, file_name)


get_tables(df, file_name)
import json
import pandas as pd
from pathlib import Path, PurePath


file_name = 'data'
file_directory = Path(__file__).parent.resolve()
path_to_file = PurePath(file_directory, file_name)
path_to_features = str(file_directory).split('/get_features')[0]

df = pd.read_csv(f'{path_to_file}.csv', sep=';', dtype=str)


def get_features(df: pd.DataFrame, feature_group_name: str, export_format: str) -> None:

    """
    Getting table with features and cleared Features column.

    Source column names: Product code, Language, Product manufacturer code, Category, Vendor name, Features
    """

    # Getting features for selected category from JSON
    def select_features(name: str) -> dict:
  
        with open(f'{path_to_features}/features.json', 'r') as f:
            data = json.load(f)

        return data[name]
    
    # Extracting feature value by searching list and making columns
    def get_features(df: pd.DataFrame, features: dict) -> None:
        
        # Extract feature value
        def extract_feature_value(string: str, startswith: str) -> str:
            features = str(string).split('; ')
            for i in features:
                if i.startswith(startswith):
                    return i.split('[')[-1].split(']')[0]

        # Extracting and making columns
        for col_name, feature in features.items():
            df[col_name] = df['Features'].apply(lambda x: extract_feature_value(x, feature))

        return df

    # Clearing extracted values in Features column
    def clear_features(df: pd.DataFrame, features: dict) -> None:

        # Collecting and concat remain values
        def collect_remain_values(string: str, features: dict) -> pd.Series:
            values = str(string).split('; ')
            features = tuple(features.values())
            clear_values = [i for i in values if not i.startswith(features)]
            clear_values = '; '.join(map(str, clear_values))
            return clear_values

        df['Features'] = df['Features'].apply(lambda x: collect_remain_values(x, features))

        return df

    # Getting features for Category
    features = select_features(feature_group_name)

    # Getting table with features and cleared Features column
    df = get_features(df, features)
    df = clear_features(df, features)

    if 'Vendor name' in df.columns.to_list():
        df.drop(columns=['Category', 'Vendor name'], inplace=True)

    match export_format:
        case 'xls':
            df.to_excel('export_data.xlsx', index=False)
        case 'csv':
            df.to_csv('export_data.csv', sep=';', index=False)


get_features(df, 'stulya', 'xls')

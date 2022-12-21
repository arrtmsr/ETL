import json
import pandas as pd
from pathlib import Path, PurePath

file_name = 'export_data'

file_directory = Path(__file__).parent.resolve()
path_to_file = PurePath(file_directory, file_name)
path_to_features = str(file_directory).split('/get_features')[0]

df = pd.read_csv(f'{path_to_file}.csv', sep=';', dtype=str)


def concat_rows(df: pd.DataFrame, feature_group_name: str) -> None:

    """
    Returning updated Features column.

    Exported column names: Product code, Language, Product Manufacturer Code, Product name, Features
    """

    # Getting features for selected category from JSON
    def select_features(name: str) -> dict:
  
        with open(f'{path_to_features}/features.json', 'r') as f:
            data = json.load(f)

        return data[name]    


    # Getting features for Category
    features = select_features(feature_group_name)

    # Getting actual column names
    column_names = set(df.columns.to_list())
    all_features = set(features.keys())
    remain_column_names = list(all_features.intersection(column_names))

    remain_features = dict()

    # Getting actual features
    for key, value in features.items():
        if key in remain_column_names:
            remain_features.update({key: value})

    # Preparing feature values to system format
    df = df.fillna('')
    for key, value in remain_features.items():
        df[key] = df[key].apply(lambda x: str(value) + '[' + str(x) + ']; ')

    # Pivot columns to sting and selecting columns
    column_names = remain_features.keys()
    df['Updated features'] = df[column_names].apply(lambda x: ''.join(x.values.astype(str)), axis='columns')
    df = df.loc[:, ('Product code', 'Language', 'Product Manufacturer Code', 'Product name', 'Features', 'Updated features')]

    # Concat updated and source feature rows
    df['Features'] = df['Updated features'] + df['Features']
    df = df.loc[:, ('Product code', 'Language', 'Product Manufacturer Code', 'Product name', 'Features')]

    df.to_csv('return_data.csv', index=False, sep=';')


concat_rows(df, 'stulya')
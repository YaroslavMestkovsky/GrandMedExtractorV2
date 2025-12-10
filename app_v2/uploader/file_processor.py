import pandas as pd

class FileProcessor:
    def __init__(self):
        pass

    def read_csv(self, path, skiprows=0, encoding='utf-8', delimiter=',', **kwargs):
        return pd.read_csv(path, skiprows=skiprows, encoding=encoding, delimiter=delimiter, **kwargs)

    def preprocess(self, df):
        # Здесь будет дополнительная предобработка
        return df

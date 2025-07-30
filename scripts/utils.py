import pandas as pd

class GPSDataPreprocessor:

    def __init__(self, df):
        self.columns = ['taxi_id', 'date_time', 'longitude', 'latitude']
        if df.shape[1] != len(self.columns):
            raise ValueError(f"Expected {len(self.columns)} columns, but got {df.shape[1]}")
        self.df = df.copy()
        self.df.columns = self.columns

    def handle_nulls(self):
        self.df = self.df.dropna()

    def convert_timestamps(self):
        self.df['date_time'] = pd.to_datetime(self.df['date_time'], errors='coerce')
        self.df = self.df.dropna(subset=['date_time'])

    def validate_coordinates(self):
        # validating coordinates w.r.t Beijing city location
        before = len(self.df)
        self.df = self.df[
            (self.df['longitude'].between(110, 125)) &
            (self.df['latitude'].between(35, 45))
        ]
        after = len(self.df)
        print(f"Removed {before - after} rows with coordinates out of Beijing area")
        
    def sort_by_id_and_time(self):
        self.df = self.df.sort_values(by=['taxi_id', 'date_time'])

    def remove_duplicates(self):
        self.df = self.df.drop_duplicates()
        
    def add_end_flag(self):
        self.df['end_flag'] = 0  
        last_indices = self.df.groupby('taxi_id').tail(1).index
        self.df.loc[last_indices, 'end_flag'] = 1

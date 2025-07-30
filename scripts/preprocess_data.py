import os
import glob
import pandas as pd
from pandas.errors import EmptyDataError
from utils import GPSDataPreprocessor


raw_data_path_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
raw_data_path = os.path.join(raw_data_path_dir, 'raw_data_files')

csv_files = glob.glob(os.path.join(raw_data_path, '*.txt'))
total_files = len(csv_files)

df = pd.DataFrame()
skipped_files = []
nulls_per_col = 0

print(f"Processing {total_files} files...\n")

for i, file_path in enumerate(csv_files, 1):
    try:
        raw_contents = pd.read_csv(file_path, header=None)
        
    except EmptyDataError:
        skipped_files.append(f"Skipped {os.path.basename(file_path)}: File was empty (no data to parse).")
        percent_complete = (i / total_files) * 100
        print(f"\r  [{i}/{total_files}] {os.path.basename(file_path)} - {percent_complete:.1f}% complete", end='', flush=True)
        continue
    except Exception as e:
        print(f"\n  Error reading {file_path}: {e}")
        continue

    df = pd.concat([df, raw_contents], ignore_index=True)
    percent_complete = (i / total_files) * 100
    print(f"\r  [{i}/{total_files}] {os.path.basename(file_path)} - {percent_complete:.1f}% complete", end='', flush=True)
    
print("\n\n" + "\n".join(skipped_files))

if df is not None and not df.empty:
    try:
        print("DataFrame is being processed...")

        preprocessor = GPSDataPreprocessor(df)

        # Initial shape
        init_rows = preprocessor.df.shape[0]
        init_cols = preprocessor.df.shape[1]
        print(f"Initial shape: {init_rows} rows × {init_cols} columns")

        print("Converting timestamps...")
        preprocessor.convert_timestamps()

        print("Validating coordinates...")
        preprocessor.validate_coordinates()

        print("Sorting by taxi_id and date_time...")
        preprocessor.sort_by_id_and_time()

        print("Adding end_flag column...")
        preprocessor.add_end_flag()

        # Final shape
        final_rows = preprocessor.df.shape[0]
        final_cols = preprocessor.df.shape[1]
        print(f"Final shape: {final_rows} rows × {final_cols} columns")

        preprocessor.df = preprocessor.df.sort_values(by=['date_time'])

        # Save the final DataFrame
        preprocessor.df.to_csv(os.path.join(raw_data_path_dir, 'combined_taxis_data.csv'), index=False)
        print("Preprocessing complete. File saved as 'combined_taxis_data.csv'.")

    except Exception as e:
        print(f"Error during preprocessing: {e}")
else:
    print("No data to preprocess.")

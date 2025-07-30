import pandas as pd

def check_timestamp_order(filename):
    df = pd.read_csv(filename)

    # Convert timestamp column to datetime (adjust column name if needed)
    df['timestamp'] = pd.to_datetime(df['date_time'])

    # Group by taxi_id and check order
    unsorted_taxis = []
    for taxi_id, group in df.groupby('taxi_id'):
        if not group['timestamp'].is_monotonic_increasing:
            unsorted_taxis.append(taxi_id)

    if unsorted_taxis:
        print("The following Taxi IDs have unsorted timestamps:")
        for tid in unsorted_taxis:
            print(f"- {tid}")
    else:
        print("All Taxi IDs have timestamps in sorted order.")

# Example usage:
check_timestamp_order("../data/combined_taxis_data.csv")

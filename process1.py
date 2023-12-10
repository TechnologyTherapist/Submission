import pandas as pd
from datetime import datetime, timedelta
import os
import argparse

def process_gps_data(parquet_path, output_dir):
    # Check if the Parquet file exists
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet file not found at: {parquet_path}")

    # Load GPS data from Parquet file
    df = pd.read_parquet(parquet_path)

    # Sort data by unit and timestamp
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    # Initialize variables for trip identification
    current_unit = None
    trip_number = 0
    trip_data = []

    # Function to save trip data to a CSV file
    def save_trip_to_csv(unit, trip_number, trip_data):
        trip_df = pd.DataFrame(trip_data, columns=['latitude', 'longitude', 'timestamp'])
        trip_csv_path = os.path.join(output_dir, f'{unit}_{trip_number}.csv')
        trip_df.to_csv(trip_csv_path, index=False)

    # Iterate through rows to identify trips
    for index, row in df.iterrows():
        if current_unit is None or row['unit'] != current_unit:
            # Start a new trip for a new unit
            current_unit = row['unit']
            trip_number = 0
            trip_data = []

        if index > 0:
            # Calculate time difference between consecutive data points
            time_diff = datetime.strptime(row['timestamp'], '%Y-%m-%dT%H:%M:%SZ') - \
                        datetime.strptime(df.at[index - 1, 'timestamp'], '%Y-%m-%dT%H:%M:%SZ')

            if time_diff > timedelta(hours=7):
                # Start a new trip when the time difference is more than 7 hours
                save_trip_to_csv(current_unit, trip_number, trip_data)
                trip_number += 1
                trip_data = []

        # Append data to the current trip
        trip_data.append([row['latitude'], row['longitude'], row['timestamp']])

    # Save the last trip
    save_trip_to_csv(current_unit, trip_number, trip_data)

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process GPS data and save trip information to CSV files.')
    parser.add_argument('--to_process', type=str, help='Path to the Parquet file to be processed', required=True)
    parser.add_argument('--output_dir', type=str, help='Folder to store the resulting CSV files', required=True)

    # Parse command-line arguments
    args = parser.parse_args()

    # Ensure the output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    # Process GPS data and save trip information to CSV files
    process_gps_data(args.to_process, args.output_dir)

if __name__ == "__main__":
    main()

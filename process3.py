import os
import json
import csv
from datetime import datetime
import argparse

def process_json_files(json_folder, output_dir):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Create CSV file for consolidated data
    csv_file_path = os.path.join(output_dir, 'transformed_data.csv')
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end', 
                      'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type',
                      'entry_time', 'exit_time', 'tag_cost', 'cash_cost', 'license_plate_cost']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        # Process each JSON file in the input folder
        for file_name in os.listdir(json_folder):
            if file_name.endswith('.json'):
                file_path = os.path.join(json_folder, file_name)
                process_json_file(file_path, writer)

def process_json_file(file_path, csv_writer):
    with open(file_path, 'r', encoding='utf-8') as json_file:
        try:
            data = json.load(json_file)
            process_trip_data(data, csv_writer, file_path)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file '{file_path}': {e}")

def process_trip_data(trip_data, csv_writer, file_path):
    if 'tolls' in trip_data and trip_data['tolls']:
        unit = trip_data.get('unit', '')
        trip_id = os.path.splitext(os.path.basename(file_path))[0]

        for toll in trip_data['tolls']:
            entry_time = toll.get('entry_time', '')
            exit_time = toll.get('exit_time', '')

            # Convert timestamp strings to a specific format (modify as needed)
            entry_time = convert_timestamp(entry_time)
            exit_time = convert_timestamp(exit_time)

            csv_writer.writerow({
                'unit': unit,
                'trip_id': trip_id,
                'toll_loc_id_start': toll.get('toll_loc_id_start', ''),
                'toll_loc_id_end': toll.get('toll_loc_id_end', ''),
                'toll_loc_name_start': toll.get('toll_loc_name_start', ''),
                'toll_loc_name_end': toll.get('toll_loc_name_end', ''),
                'toll_system_type': toll.get('toll_system_type', ''),
                'entry_time': entry_time,
                'exit_time': exit_time,
                'tag_cost': toll.get('tag_cost', ''),
                'cash_cost': toll.get('cash_cost', ''),
                'license_plate_cost': toll.get('license_plate_cost', '')
            })

def convert_timestamp(timestamp_str):
    if timestamp_str:
        # Modify the format based on the actual timestamp format in the JSON data
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return ''

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Process toll information from JSON files and generate a CSV file.')
    parser.add_argument('--to_process', type=str, help='Path to the JSON responses folder', required=True)
    parser.add_argument('--output_dir', type=str, help='Folder to store the final CSV file', required=True)

    # Parse command-line arguments
    args = parser.parse_args()

    # Process JSON files and generate CSV file
    process_json_files(args.to_process, args.output_dir)

if __name__ == "__main__":
    main()

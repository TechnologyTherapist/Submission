import os
import requests
from dotenv import load_dotenv
import argparse

load_dotenv()  # Load environment variables from .env file

TOLLGURU_API_KEY = os.getenv('NpH97GRb493bmTrfPqG7NNQ8R99mG4J6')
TOLLGURU_API_URL = os.getenv('https://dashboard.tollguru.com/dashboard/access-key')

def send_request(file_path, output_dir):
    url = f'{TOLLGURU_API_URL}/toll/v2/gps-tracks-csv-upload?mapProvider=osrm&vehicleType=5AxlesTruck'
    headers = {'x-api-key': TOLLGURU_API_KEY, 'Content-Type': 'text/csv'}

    with open(file_path, 'rb') as file:
        response = requests.post(url, data=file, headers=headers)

    # Save JSON response to output directory
    output_file_path = os.path.join(output_dir, os.path.basename(file_path).replace('.csv', '.json'))
    with open(output_file_path, 'w') as json_file:
        json_file.write(response.text)

def process_files(csv_folder, output_dir):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Process each CSV file in the input folder
    for file_name in os.listdir(csv_folder):
        if file_name.endswith('.csv'):
            file_path = os.path.join(csv_folder, file_name)
            send_request(file_path, output_dir)

def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description='Send CSV files to TollGuru API and store JSON responses.')
    parser.add_argument('--to_process', type=str, help='Path to the CSV folder', required=True)
    parser.add_argument('--output_dir', type=str, help='Folder to store the resulting JSON files', required=True)

    # Parse command-line arguments
    args = parser.parse_args()

    # Process CSV files and send requests to TollGuru API
    process_files(args.to_process, args.output_dir)

if __name__ == "__main__":
    main()

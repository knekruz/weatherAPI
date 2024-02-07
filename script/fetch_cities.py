import requests
import json
import os
import subprocess

# Function to run HDFS command with print statements
def run_hdfs_command(cmd, input=None, capture_output=False):
    try:
        result = subprocess.run(['hdfs', 'dfs'] + cmd, input=input, text=True, check=capture_output, capture_output=capture_output)
        print(f"Command success: {' '.join(['hdfs', 'dfs'] + cmd)}")
        if capture_output:
            return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(['hdfs', 'dfs'] + cmd)}\nError: {e.stderr}")
        return None

# Check if HDFS is available
def check_hdfs_available():
    try:
        subprocess.run(['hdfs', 'dfs', '-ls', '/'], check=True)
        print("HDFS is running.")
        return True
    except subprocess.CalledProcessError:
        print("HDFS is not running.")
        return False

# Function to fetch cities data from HDFS for a country
def fetch_cities_data_hdfs(country_name):
    hdfs_file_path = f"/user/hadoop/weather/raw/{country_name}/cities.json"
    cities_data_json = run_hdfs_command(['-cat', hdfs_file_path], capture_output=True)
    if cities_data_json:
        try:
            return json.loads(cities_data_json)
        except json.JSONDecodeError:
            print(f"Failed to decode JSON for {country_name}")
            return {}
    else:
        return {}

# Your actual OpenWeather API key
API_KEY = 'b3953b26f69fadd91ece3e4ff4504503'  # Replace with your actual OpenWeather API key

if check_hdfs_available():
    # Load countries and cities from JSON
    with open('/home/hadoop/Desktop/ECBD/data/countries.json') as json_file:
        data = json.load(json_file)
    
    for country in data['countries']:
        country_name = country['name']
        country_code = country['code']
        
        # Attempt to fetch existing cities data from HDFS
        existing_cities_data = fetch_cities_data_hdfs(country_name)

        for city in country['cities']:
            if city not in existing_cities_data:
                # Make API call
                response = requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city},{country_code}&limit=1&appid={API_KEY}")
                geodata = response.json()
                if geodata:
                    # Here's where we adjust the format of each city data before saving
                    city_data_formatted = {
                        "name": city,
                        "lat": geodata[0]['lat'],
                        "lon": geodata[0]['lon'],
                        "country": geodata[0]['country'],
                        "state": geodata[0].get('state', ''),
                        "local_names": geodata[0].get('local_names', {})
                    }
                    existing_cities_data[city] = city_data_formatted  # Update with new city data
                    print(f"Retrieved data for {city}, {country_name}.")
                else:
                    print(f"No data found for {city}, {country_code}.")
            else:
                print(f"Data for {city} already exists in HDFS, skipping API call.")

        # Convert existing_cities_data to the desired list format
        cities_list = list(existing_cities_data.values())

        # Update local file
        local_dir = f"/home/hadoop/Desktop/ECBD/output/{country_name}"
        os.makedirs(local_dir, exist_ok=True)
        with open(f"{local_dir}/cities.json", 'w') as outfile:
            json.dump(cities_list, outfile, indent=4)
        print(f"Updated local data for {country_name}.")

        # Update HDFS file
        hdfs_dir = f"/user/hadoop/weather/raw/{country_name}"
        run_hdfs_command(['-mkdir', '-p', hdfs_dir])
        hdfs_file_path = f"{hdfs_dir}/cities.json"
        cities_json_str = json.dumps(cities_list, indent=4)
        run_hdfs_command(['-put', '-f', '-', hdfs_file_path], input=cities_json_str)
        print(f"Updated HDFS data for {country_name}.")
else:
    print("Please ensure HDFS is running and try again.")

import requests
from shapely.geometry import shape, Point
import pandas as pd
from rtree import index


# Set the API URL and initial parameters, replace with your target API endpoint
api_url = "YOUR_TARGET_API"
limit = 1000  # Number of records to fetch per request
offset = 0   # Initial offset
results = []

# Replace with your app token
app_token = "YOUR_APP_TOKEN"

# Define headers with the app token
headers = {
    'X-App-Token': app_token
}

# Function to fetch data with pagination
def fetch_data(api_url, headers, offset, limit):
    response = requests.get(api_url, headers=headers, params={"$offset": offset, "$limit": limit})

    # Check if the response contains rate limit headers
    rate_limit_headers = response.headers
    rate_limit_limit = rate_limit_headers.get('X-RateLimit-Limit')
    rate_limit_remaining = rate_limit_headers.get('X-RateLimit-Remaining')
    rate_limit_reset = rate_limit_headers.get('X-RateLimit-Reset')

    if rate_limit_limit and rate_limit_remaining and rate_limit_reset:
        print(f"Rate Limit: {rate_limit_limit}")
        print(f"Remaining Requests: {rate_limit_remaining}")
        print(f"Reset Time: {rate_limit_reset}")

    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch data from the API. Check the response and try again.")
        return None

# Fetch and process data with pagination
while True:
    data = fetch_data(api_url, headers, offset, limit)
    if data is None or not data:
        break
    results.extend(data)
    offset += limit

# Step 1: Retrieve GeoJSON Data from the URL
geojson_url = "https://services5.arcgis.com/GfwWNkhOj9bNBqoJ/arcgis/rest/services/NYC_City_Council_Districts/FeatureServer/0/query?where=1=1&outFields=*&outSR=4326&f=pgeojson"
response = requests.get(geojson_url)

# Check if the request was successful
if response.status_code == 200:
    geojson_data = response.json()
else:
    print("Failed to fetch GeoJSON data from the URL. Check the URL and try again.")
    geojson_data = None  # Set to None to handle the error later

# Create an R-tree index for council district bounding boxes
idx = index.Index()

# Initialize a list to store matched data
matched_data = []

# Initialize a list to store error records
error_records = []

if geojson_data and results:
    crash_df = pd.DataFrame(results)

    # Convert latitude and longitude to numeric
    crash_df['longitude'] = pd.to_numeric(crash_df['longitude'])
    crash_df['latitude'] = pd.to_numeric(crash_df['latitude'])

    # Determine the number of council districts dynamically
    num_districts = len(geojson_data.get('features', []))

    for i in range(num_districts):
        council_district_feature = geojson_data['features'][i]

        try:
            council_district_shape = shape(council_district_feature['geometry'])
            council_district_name = council_district_feature['properties']['CounDist']

            # Build the R-tree index with the bounding box of the council district
            idx.insert(i, council_district_shape.bounds)
        except Exception as e:
            print(f"Error processing council district {i}: {str(e)}")
            continue  # Skip this council district and move to the next one

        # Initialize a list for matched data in this district
        district_matched_data = []

        for _, crash_record in crash_df.iterrows():
            point = Point(crash_record['longitude'], crash_record['latitude'])

            if not point.is_empty:  # Check if the Point is not empty
                try:
                    if council_district_shape.contains(point):
                        # Add the 'CounDist' to the crash record
                        crash_record['CounDist'] = council_district_name
                        district_matched_data.append(crash_record)
                except Exception as e:
                    print(f"Error processing record {crash_record['open_data_crash_id']}: {str(e)}")
                    continue  # Skip this record and move to the next one

        if district_matched_data:
            # Save matching crash data for this district to a CSV file
            district_matched_df = pd.DataFrame(district_matched_data)
            file_name = f"matched_data_district_{i}_{council_district_name}.csv"
            district_matched_df.to_csv(file_name, index=False)
            print(f"Matching crash data for council district {council_district_name} has been saved as '{file_name}'")

        # Store records that did not cause errors
        matched_data.extend(district_matched_data)

    # Error handling: Store records that caused errors
    for _, crash_record in crash_df.iterrows():
        point = Point(crash_record['longitude'], crash_record['latitude'])
        error_flag = True

        if not point.is_empty:  # Check if the Point is not empty
            for i in idx.intersection((point.x, point.y, point.x, point.y)):
                council_district_feature = geojson_data['features'][i]
                council_district_shape = shape(council_district_feature['geometry'])

                try:
                    if council_district_shape.contains(point):
                        error_flag = False
                        break
                except Exception as e:
                    print(f"Error processing record {crash_record['open_data_crash_id']}: {str(e)}")

        if error_flag:
            error_records.append(crash_record)

    if error_records:
        error_df = pd.DataFrame(error_records)
        error_df.to_csv("error_data.csv", index=False)
        print("Error records have been saved as 'error_data.csv'")

# Export Overall Matching Data to a CSV file
if matched_data:
    matched_df = pd.DataFrame(matched_data)
    matched_df.to_csv("matched_data_overall.csv", index=False)
    print("Overall matching crash data has been saved as 'matched_data_overall.csv'")

if not matched_data:
    print("No matching crash data found for the specified council districts.")

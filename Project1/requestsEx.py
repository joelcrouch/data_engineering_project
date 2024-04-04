import requests
def get_breadcrumbs(vehicle_id):
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    response = requests.get(url)

    if response.status_code == 200:
        json_data = response.json()
        return json_data
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

def write_to_file(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file)
# maybe throw this into a main and then run it wiht the parser
# and write out the data to file
# also make a data structure such that the data for a vehicle will be written to that vehicles 'super" data, ie bus 1 will have data from dat 1, day2 etc
vehicle_id = "1003"  # Replace "123" with the actual vehicle ID
breadcrumbs_data = get_breadcrumbs(vehicle_id)
if breadcrumbs_data:
    print(breadcrumbs_data)
    filename = "breadcrumbs.json"
    write_to_file(breadcrumbs_data, filename)
    print(f"Data written to {filename} successfully.")
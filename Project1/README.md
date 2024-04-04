## BREADCRUMBS: TRIMET-A WINDY TRAIL

On a high level, the following are the goals for this portion of the project.  Trimet (Portland's public bus transit system)  publishes raw data regarding the location of buses throughout the day.  A simple web server provides access to one day's worth of trimet data called 'breadcrumb'. (leaving a trail)  You may access the data by using the following url
```
https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=<vehicle_id>

```

The vehicle id is assigned to each group of students and is found <here>(ADDLINK)   Replace <vehicle id>  with the correct id of the vehicle. You must get data for each vehicle assigned to the group.

#### REQUIREMENTS

    1. Create, configure and use a Google Cloud Platform (GCP) linux virtual machine (VM).
    2. Develop a simple python program to gather the data programmatically.
    3. Configure your VM running your gathering client to run daily. 
    4. Allocate and configure a message passing “topic” and “subscription” at Google Cloud Pub/Sub.
    5. Enhance your data gathering client to parse the breadcrumb data and publish individual JSON records. 

#### HOW DO YOU START?

##### GCP VM:  Create, configure, and use a Google Cloud Platform linux VM.
There are two methods to make a VM on google cloud platform(GCP).  Both methods were taken directly from Dr. Wu-Chang(1).  One way is to do it at the command line(eg: cloudshell in GCP):

```
bash
gcloud compute instances create course-vm \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20G \
  --zone=us-west1-b \
  --machine-type=e2-medium
```

The second way it to use the GCP gui(the web console on GCP). Here are the steps to do that:



##### Develop a simple python program to gather the data programmatically. 
 The data will probably be a bunch of data.  You will have to parse out the relevant data, grab the correct data, and then in a for-loop apply the function that will extract the correct and verified data. In a nutshell: get the data for each vehicle:'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id=<vehicle_id>'. So figure out what is in there.  Each vehicle will have some 'extra' data in there. Add each vehicle's data to a data structure.  Loop through each vehicles, data, and make a smaller more useful structure, then loop through and grab what you want.  Make it into a chart, possibly in a Jupyter notebook.
Testing: Make a dummy set of data, make sure you are getting what you think. For example, if the vehicle data includes vehicleId, dest, src, timeCurrent, ETANextStop, TimeLastStopAndStopID, routeId. You might only want vehicleId and timeCurrent and TimeLastStopAndStopId, and stopInfo(array of data) First you might need to store all the data(?), and then parse out the vehicleId and TimeLastStopAndStopId and stopInfo.  Grab the busId and location and stopInfo and figure out what the stop is.  This data will probably be in JSON format, so you will have to use jsonify.
This is what the bus_dictionary will look like:
```
python
bus_dict = {'vehicleID': 'timeCurrent', 'dest': 'src', 'stopInfo.stopId': 'stopInfo.stopNextId'}
```
Something like that. Then loop through the bus_dict and get the data you want.



##### Configure your VM running your gathering client to run daily.
Use cron and figure out how to have your vm turn on at 'x' time, and run your script. See https://man7.org/linux/man-pages/man8/cron.8.html.   There is probably a cron style tool on GCP such that it may be turned on at 'x-10' time, and then run the script, and then turn off the vm.

##### Allocate and configure a message passing “topic” and “subscription” at Google Cloud Pub/Sub.
Figure out the topic and subscription.  See Wu-Chang's lecture on subscription.


#### Enhance your data gathering client to parse the breadcrumb data and publish individual JSON records.
See the description of the script above.


REFERENCES:
1.  Lifted spuriously from Dr. Wu-Chang's 530 website;
https://codelabs.cs.pdx.edu/labs/C01.1_hw1/index.html?index=..%2F..cs430#2
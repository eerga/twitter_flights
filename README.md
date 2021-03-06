## Twitter & Plane Landing Correlation Assessment
---
## Project Goal
We decided to explore the possibility of correlation between the number of successfully landed commercial flights and overall Twitter activity.

### Where to use?
This project is to be run on Google Cloud Platform (GCP). It includes running a **Pub/Sub job**.

### Main function
In the twitter function, please use your Twitter access and token keys in main.py :)
If you need Twitter access and token keys, refer to this [link](https://developer.twitter.com/en/docs/apps/overview)

### Running the function
Twitter function contains all of the necessary files to run a Cloud Function on GCP. The function itself needs to be activated by running a "Test Function" feature with
the following request: `{"message":{"query":["American Airlines", "United Airlines", "Delta Airlines"],"limit":100,"projectId":"YOUR-GCP-PROJECT-ID",
"topic":"TOPIC-NAME-THAT-PERTAINS-TO-TWITTER-DATA","bucket":"BUCKET-NAME-THAT-PERTAINS-TO-TWITTER-DATA"}}`

Running the test function will extract 100 most recent tweets pertaining to airlines such as American Airlines, Delta, and United to analyze relationship 
between Twitter activity spikes and plane landings

### Twitter Schemas
The **schemas** used for Twitter Pub/Sub Topic and BigQuery output Twitter table are named *topicschema* and *tableschema*. 

### Nesting Correction for Twitter
After running the streaming job from pub/sub topic to BigQuery table, you'll notice that the data is not loaded correctly due to complex Twitter message nesting and the
error_records table has been generated. Run the following query in BigQuery: </br> `select payloadString from YOUR-PROJECT-NAME.YOUR-DATASET-NAME-YOUR-TABLE-NAME_error_record `.</br>
Click **"Save Results"** followed by selecting the option of **"CSV (local file)"**. Once you have the csv file stored on your local computer, run the *bigdata.ipynb* file to 
extract the most meaningful features from the tweets. As a result, you should end up with two CSV files: one is for Twitter users and one is for tweets.

Load the corrected csv files back onto a newly created corrected table. If you want to analyze both tweets and Twitter users, then create two empty csv files with a propertly
defined schema. Schema includes column names and appropriate variable types. 

### Flights data loading
Flights function contains all of the necessary files to run a Cloud Function on GCP. The function itself needs to be activated by running a "Test Function" feature with
the following request: `{"topic":"flights_api","separateLines":"True","debug":10}`

Running the test function will extract 200 most recent flights across the world, which includes features such as unique plane identifies number (icao24) and on_ground.

### Flights Schemas
The schemas used for flights_api Pub/Sub Topic and BigQuery output flights table are named topicschema and tableschema. 

### ML work
After running the streaming job from pub/sub topic to bigquery table, you can run Machine Learning algorithms mentioned in the Twitter & Ontime performance slides.

### Conclusion
Even though we did observe activity spikes for both flights and Twitter, the activity spikes did not overlay, which means that other variable(s) may better explain Twitter activity increase. Therefore, the analysis suggests that our hypothesis should be rejected as this marketing strategy is not supported by available data. 


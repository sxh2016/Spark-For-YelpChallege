# Spark-For-YelpChallege
Using Spark to analysis the big dataset offered by Yelp: This is a competition hold by yelp, and the link is https://www.yelp.com/dataset_challenge/dataset .
Because the dataset is too large to upload, so it is not contained in the folder. You can download it from the link above.
The main task I have done is as following:
##(1)Data Loading
The first step is load these .json files using SparkSQL, and build the tables. Then I get the target subset which I want by using conditions query, and I could do analysis based on this subset.

##(2)Text Recommendation
Even we can pick the target subset using conditiond query, the amount of the text files in the subset is still very huge. Usually there will be more than 1000 text files about a given topic.
So pick representa files from the subset is very useful.

For the yelp_review table and yelp_tip table, the main body is text files. So if I want to analysis these data, I must transfor these unstructured data into structred form. 
First, I filter those illegal words, stop words, and low frequent words, and using the TF-IDF method to transfor these text into vectors.

After the steps of data preprocessing, I use K-means method do the clustering, and pick several text with the nearst distace to each core to present the whole cluster. By this method, the amount of text files could reduced from 1000 to about 20, and these text could cover the
most information contained in the dataset and has least overlap with each other. Imagine that while user search about a given topic, these 20 files could appears in the first page.

##(3)Graph Analysis
Some times the graph is a very useful tool to describe the relationship between the element in the dataset. 
For the table of Yelp_User, the main body is the relation ship between these users, so I build the graph model for the users and do some interesting graph analysis using GraphX.
Finally, I impelement the data visualization for the result.

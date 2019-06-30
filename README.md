# ContentFul_DE_TEST

1). Config.yml file contains all the Token/Keys/Parameters that needs to be controlled overtime and can be changed later on. 
So for the sake of accessibility and security such parameters are stored in Config file, these are later on used in the code whereever is 
required.

2). Twitter_Influence_Extract.py contains all the functions for extracting, cleansing,persisting and accessing the twitter data. 
Sample calls are made in the Main function of the code to give an idea about the functionality of the code.

3). Comments are properly given whereever seem necessary

4). HighLevel_Entities.PNG , Entity_Relationships.PNG and DetailedSchema.PNG files contains the designed Twitter Data model. Provided solution revolves around this data please go through the data model to get a better understanding of solution

5). In this version of the project we have implemented some of the entities from Twitter data Model refferred to POINT 4 in this document. 
Data related to Base Tweet that includes all the basic fields related to tweet such a Creation time, Tweet Text, Tweet Id, Username, Retweet counts of the tweet etc. as well as data about the Tweet handle who actually posted the tweet.

6). MVP of the project till this phase is that we can extract and assign a score to several different Twitter influencers based upon their Social Media Influence. For example in this code i am calculating the No of followrs , No of Faves and Average Retweet per Tweet to get know the influence of that certain person. In future once i have collected considerable amount of data i can use this data to build QUEENBEE data science model for promoting products through such influencers.

# Demo Projects

## Introduction

First off, I would like to introduce you to my demonstration projects. This is where I will explicitly use the skills I have aquired over the course of my certificate program, and through my work experiences. There is currently one main part to this project folder: 'Transport-Weather'. This folder is the base for my project analyzing the effects of different weather related variables on bike ridership. Specifically I am using data from the Fremont Bridge bike and scooter counter. I chose this location given its importance as a commuter bicycle route between North Seattle and Downtown. This location could be a good representation of bike and scooter ridership for the city as a whole - or at least related. 

These projects are not fully complete - and they might never be. I plan to work on and expand these demonstrations as my technical skills and curiousity grow further. As of now, October 6th, 2026, I have completed the data retrieval and exploration notebook. 

## Transport and Weather Analysis 

### Data Aggregation and Exploration 

For this topic please refer to the notebook in the Transport-Weather subfolder titled 'data_exploration.ipynb'. 

My goal with this notebook is to aggregate data from two distinct apis (City of Seattle OpenData and OpenWeatherMap) and to prepare it for preprocessing and feature engineering. Those activities will occur in a further notebook. For this one, I would like to start by writing a short description of the notebook layout:

It starts with collecting bike data from the city of Seattle's OpenData portal, I then chose to drop irrelevant information and save just the counts and their repsective times. Counts for the total bridge, northbound traffic, and southbound traffic are all available in this dataset and are retained.

The next step is similar but a bit more complicated. For aggregating weather data I am using a partially free/partially paid api to gather historical weather data by hour for the city of Seattle. The limit for free calls is 1000 per day, with an extra 1000 paid calls done per day. The total queries needed sits around 6100, or just ever so slightly above the limit I have imposed over 72hrs. For this side of the data collection process I made a loop that goes through and calls query after query. Each query gives 20 hours of data, and also provides the query for previous and forward looking weather data. Using this logic, I created a loop that gathers calls daily until the limit is reached, saving the collected data regularly as a safeguard. When the api calls are completed, the data is then merged into a dataframe and saved to a .csv file.

The data collected for this project is available to view in the 'scraped_data' subfolder for this project. 
## Script to load data from the mutliple data sources (csv files) which were created from the data structuring post

## the scraping of data from meetup.com. Various multiple meaningful data dictionaries and data frames are created,

## having useful data mappings amongst the users ,groups, interests, and regions.



import numpy as np

import pandas as pd

from pprint import pprint



## 1. About_Us

with open('About_Us.csv','r') as f:

    about_list = f.readlines()

about_us = about_list[2:]

about_us = [x.strip('\n') if x != '\n' else x for x in about_us]



##For every unique group, this gives the number of descriptions associated with each group.

about_dict_1 = {}

for i in range(len(about_us)-1):

    if about_us[i] == '\n':

        temp_vals = [about_us[i+2]]

        j=i+3

        while about_us[j]!='\n':

            temp_vals.append(about_us[j])

            j += 1

        about_dict_1[about_us[i+1]] = temp_vals



## For every description, this gives the groups which are associated with each genre.

about_dict_2 = {}

for i in range(len(about_us)-1):

    if about_us[i]=='\n':

        j=i+2

        while (j<len(about_us) and about_us[j]!='\n'):

            if about_us[j].lower() not in about_dict_2.keys():

                about_dict_2[about_us[j].lower()]=[]

            about_dict_2[about_us[j].lower()].append(about_us[i+1])

            j+=1



## 2.  Members_Info

members_info = pd.read_csv('Members_Info.csv')

del members_info['Unnamed: 0']



## From the data scraped, there were a total of 50,709 members, out of which there are 46,994 unique members,

## each row having metadata for every member, like location, number of other groups, date of joining group



members_dict = {}

for i, rows in members_info.iterrows():

    if rows['Unique_ID'] not in members_dict.keys():

        members_dict[rows['Unique_ID']] = [rows['Group_Name']]

    else:

        members_dict[rows['Unique_ID']].append(rows['Group_Name'])



multiple_members_dict = {}

for items in members_dict:

    if len(members_dict[items]) > 1:

        multiple_members_dict[items] = members_dict[items]



for m in multiple_members_dict:

    multiple_members_dict[m] = list(np.unique(multiple_members_dict[m]))



## 3. Member_interests

with open('Members_Interests.csv', 'r') as f:

    members_list = f.readlines()

members_interests = members_list[2:]

members_interests = [x.strip('\n') if x != '\n' else x for x in members_interests]



##For every unique member, this gives the genre each member is interested in.

interests_dict_1 = {}

for i in range(len(members_interests) - 1):

    if members_interests[i] == '\n':

        temp_vals = [members_interests[i + 2]]

        j = i + 3

        while (j < len(members_interests) and members_interests[j] != '\n'):

            temp_vals.append(members_interests[j])

            j += 1

        interests_dict_1[members_interests[i + 1]] = temp_vals



## For every genre, this gives the members interested in each genre.

interests_dict_2 = {}

for i in range(len(members_interests) - 1):

    if members_interests[i] == '\n':

        j = i + 2

        while (j < len(members_interests) and members_interests[j] != '\n'):

            if members_interests[j].lower() not in interests_dict_2.keys():

                interests_dict_2[members_interests[j].lower()] = []

            interests_dict_2[members_interests[j].lower()].append(members_interests[i + 1])

            j += 1



first_few_interests_dict_2 = {k: interests_dict_2[k] for k in list(interests_dict_2)[:10]}





## 4. Members Other Groups

with open('Members_Other_Groups.csv', 'r') as f:

    members_others_list = f.readlines()

members_other_groups = members_others_list[2:]



members_other_groups = [x.strip('\n') if x != '\n' else x for x in members_other_groups]



##For every unique member, this gives the total groups each member is a part of.

other_groups_dict_1 = {}

for i in range(len(members_other_groups) - 1):

    if members_other_groups[i] == '\n':

        temp_vals = [members_other_groups[i + 2]]

        j = i + 3

        while (j < len(members_other_groups) and members_other_groups[j] != '\n'):

            temp_vals.append(members_other_groups[j])

            j += 1

        other_groups_dict_1[members_other_groups[i + 1]] = temp_vals



## For every group, this gives the total members which are a part of it.

other_groups_dict_2 = {}

for i in range(len(members_other_groups) - 1):

    if members_other_groups[i] == '\n':

        j = i + 2

        while (j < len(members_other_groups) and members_other_groups[j] != '\n'):

            if members_other_groups[j].lower() not in other_groups_dict_2.keys():

                other_groups_dict_2[members_other_groups[j].lower()] = []

            other_groups_dict_2[members_other_groups[j].lower()].append(members_other_groups[i + 1])

            j += 1



first_few_other_groups_dict_2 = {k: other_groups_dict_2[k] for k in list(other_groups_dict_2)[:10]}





## 5. scraped groups data

scraped_df = pd.read_csv('Scrapped_Groups_Data.csv')

del scraped_df['Unnamed: 0']



## From all the available urls in total, some of the meetup group urls were not found, which would most probably

## mean that the group might not be functional any longer. For the remaining meetup groups, scrapped all the data

## from these groups, such as location of the group, genre of the groups, founders, total members, and data of all

## the members, thier interests, etc.





#len(scraped_df.loc[scraped_df.Group_name == 'Url not working',:])



scraped_df = scraped_df.loc[scraped_df.Group_name!='Url not working',:].reset_index(drop=True)

scraped_df.Total_members = scraped_df.Total_members.str.replace(',','').astype('int')

scraped_df.Founding_date = pd.to_datetime(scraped_df.Founding_date)



scraped_df.loc[scraped_df.Past_meetups=='[]','Past_meetups'] = '0'

scraped_df.Past_meetups = scraped_df.Past_meetups.str.replace(',','').astype('int')





## possible insights:

## decreasing order, by number of past meetups.

scraped_df[['Group_name', 'Past_meetups']].sort_values(by='Past_meetups', ascending=False).reset_index(drop=True)



## decreasing order by founding date

scraped_df[['Group_name','Founding_date']].sort_values(by='Founding_date', ascending=False).reset_index(drop=True)



## decreasing order by total members :

scraped_df[['Group_name','Total_members']].sort_values(by='Total_members', ascending=False).reset_index(drop=True)



## Average number of members state wise

round(scraped_df.groupby(by='State',as_index=False).agg({'Total_members':'mean'}),2)



## Average number of members city wise

round(scraped_df.groupby(by=['City','State'],as_index=False).agg({'Total_members':'mean'}),2)

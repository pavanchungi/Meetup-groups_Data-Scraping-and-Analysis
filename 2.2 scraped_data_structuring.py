## This script loads all the scraped data saved in the pickle file, and organise and structure the data accordingly

## into separate csv files



import numpy as np

import pandas as pd

import re

import pickle



pickle_path = '/home/manish/Scraping/variable_state.pkl'

with open(pickle_path,'rb') as f:

    x,final_index,new_base_urls,meetup_group_list= pickle.load(f)



final_columns = ['Group_name','Past_meetups','City','State','Total_members','Group_Privacy','Founders','Founding_date']

df_prep = [x[0][:-2] if len(x) ==1 else ['Url not working'] for x in meetup_group_list]

new_df = pd.DataFrame(df_prep)

new_df.columns = final_columns



for i,x in enumerate(new_df.Past_meetups):

    if (x is not None):

        if(len(x)>0):

            new_df.Past_meetups[i] = re.findall(r'[\d,]+',x[0])[0]

            

for i,x in enumerate(new_df.Total_members):

    if (x is not None):

        if(len(x)>0):

            new_df.Total_members[i] = re.findall(r'[\d,]+',x)[0]

            

for i,x in enumerate(new_df.Founding_date):

    if (x is not None):

        if(len(x)>0):

            new_df.Founding_date[i] = x.replace('\n',' ').strip().replace('Founded ','')

            

about_us_dict = {}

for rows in meetup_group_list:

    if(len(rows)==1):

        about_us_dict[rows[0][0]] = rows[0][-2]

        

        

members_df_prep = []

temp = []

for rows in meetup_group_list:

    if(len(rows)==1):

        for r in rows[0][-1]:

            temp = [r[0]]

            temp.extend([rows[0][0],r[1],r[2],r[3],r[4],r[5]])

            members_df_prep.append(temp)

            

members_df = pd.DataFrame(members_df_prep)

members_df.columns = ['Unique_ID','Group_Name','Member_Name','City','State','Joining_date','Other_groups']

            

            

members_other_groups_dict = {}

for rows in meetup_group_list:

    if(len(rows)==1):

        for r in rows[0][-1]:

            #members_other_groups_dict[r[0]] = [rows[0][0]]

            members_other_groups_dict[r[0]] = r[6]

            

            

members_interests_dict = {}

for rows in meetup_group_list:

    if(len(rows)==1):

        for r in rows[0][-1]:

            #members_interests_dict[r[0]] = [rows[0][0]]

            members_interests_dict[r[0]] = r[7]





new_df.to_csv('/home/manish/Scraping/Scrapped_Groups_Data.csv')



with open('About_Us.csv', 'w') as f:

    f.write('\n\n\n')

    for key in about_us_dict.keys():

        f.write('{0}\n'.format(key))

        for val in about_us_dict[key]:

            f.write('{0}\n'.format(val))

        f.write('\n')

        

members_df.to_csv('/home/manish/Scraping/Members_Info.csv')

        

with open('Members_Other_Groups.csv', 'w') as f:

    f.write('\n\n\n')

    for key in members_other_groups_dict.keys():

        f.write('{0}\n'.format(key))

        if members_other_groups_dict[key] is not None:

            for val in members_other_groups_dict[key]:

                f.write('{0}\n'.format(val))

        f.write('\n')

        

with open('Members_Interests.csv', 'w') as f:

    f.write('\n\n\n')

    for key in members_interests_dict.keys():

        f.write('{0}\n'.format(key))

        if members_interests_dict[key] is not None:

            for val in members_interests_dict[key]:

                f.write('{0}\n'.format(val))

        f.write('\n')

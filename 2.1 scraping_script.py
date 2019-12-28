## Scraping script, written for gathering data about the groups in meetup.com, for capturing the various attributes:-

## 1. Data of the group, its founders, date of forming, total members, location, etc.

## 2. The genres/description associated with the groups.

## 3. Information about the members of the groups.

## 4. Individual interests of the members.

## 5. Other meetup groups, which a member of a particular group is also a part of.



import numpy as np

import pandas as pd

from bs4 import BeautifulSoup

import requests

import warnings

from time import sleep

import sys

from random import randint

import re

import pickle

import time

import os

import time

from IPython.display import clear_output

        

pd.options.display.max_rows=1000

warnings.filterwarnings('ignore')



file_path = '/home/manish/Data/meetup_groups_clean_28MAR2017.xlsx'

xls = pd.ExcelFile(file_path)

df_metadata = pd.read_excel(xls,sheet_name = 'META DATA')

df_comicmeetupmaster = pd.read_excel(xls,sheet_name = 'COMICMEETUP_MASTER')

df_latlong = pd.read_excel(xls, sheet_name = 'lat_long',header = None,names = ['CITY','STATE','LAT','LONG'])

df_founders = pd.read_excel(xls, sheet_name = 'FOUNDERS')

df_members = pd.read_excel(xls, sheet_name = 'MEMBERS', skip_blank_lines=False)



## Data Manipulation

df_metadata.columns = df_metadata.columns.str.replace(' ','_')

df_comicmeetupmaster.columns = df_comicmeetupmaster.columns.str.lstrip()

df_comicmeetupmaster.columns = df_comicmeetupmaster.columns.str.replace(' ','_')

df_latlong.columns = df_latlong.columns.str.replace(' ','_')

df_founders.columns = df_founders.columns.str.replace(' ','_')

df_members.columns = df_members.columns.str.replace(' ','_')





pickle_path = '/home/manish/Scraping/variable_state.pkl'

#os.remove(pickle_path)



base_urls = df_comicmeetupmaster.GROUP_URLs

base_urls = base_urls[:-1]

x_init = -1

new_base_urls_init = base_urls[x_init+1:]

final_index_init = 0

meetup_group_list_init = []



if not os.path.isfile(pickle_path):

    with open(pickle_path,'wb') as f:

        pickle.dump([x_init,final_index_init, new_base_urls_init,meetup_group_list_init],f)

        

if os.path.isfile(pickle_path):

    with open(pickle_path,'rb') as f:

        x,final_index,new_base_urls,meetup_group_list = pickle.load(f)

        

new_base_urls = new_base_urls[x+1:]

final_index += x+1



for no,url in enumerate(new_base_urls):

    #clear_output()

    print("Starting with Iteration no {0} of the URLs".format(final_index+no))

    grp_name = None; past_meetups = []; city = None; state = None; total_members = None; group_privacy=None; founder = [];

    founding_date = None; about_list = None; offset_upper_limit = []

    time.sleep(1)

    page = requests.get(url)

    soup = BeautifulSoup(page.content,'html.parser')

    

    if page.status_code == 200:

        try:

            final_list = []

            grp_name = soup.find('a',class_ = 'groupHomeHeader-groupNameLink').get_text()

            for temp in soup.find_all('h3', class_ = 'text--sectionTitle text--bold padding--bottom'):

                if('past meetups' in temp.get_text().lower()):

                    past_meetups.append(temp.get_text())

            city,state = soup.find('a', class_ = 'groupHomeHeaderInfo-cityLink').get_text().split(', ')

            total_members = soup.find('a', class_ = 'groupHomeHeaderInfo-memberLink').get_text()

            group_privacy = soup.find('span', class_ = 'infoToggle-label').get_text()

            

            member_url = url+'members'

            member_page = requests.get(member_url)

            member_soup = BeautifulSoup(member_page.content,'html.parser')

            

            found = member_soup.find('span',id = 'meta-leaders').find_all('a',class_ = 'memberinfo-widget')

            if len(found)>1:

                for f in found:

                    founder.append(f.get_text())

            else:

                founder = member_soup.find('span',id = 'meta-leaders').find('a',class_ = 'memberinfo-widget').get_text()



            founding_date = member_soup.find('div', class_ = 'small margin-bottom').get_text()



            about_us = member_soup.find('div',class_ = 'meta-topics-block small margin-bottom').find_all('a')

            about_list = []

            for a in about_us:

                about_list.append(a.get_text())



            offset_upper_limit = int(re.findall(r'/?offset=(\d+)\&',member_soup.find('a', class_ = 'nav-next')['href'])[0])

            total_members_list = []

            print('Extracting data of total {0}'.format(total_members))

            print_key = 0

            for n in np.arange(0,offset_upper_limit+20,20):

                memberships_url = member_url+'?offset=' + str(n) + '&sort=social&desc=1'

                total_member_page = requests.get(memberships_url)

                total_member_soup = BeautifulSoup(total_member_page.content,'html.parser')

                total_members_iterate = total_member_soup.find('ul',id = 'memberList').find_all('a', class_ = 'memName')

                for i,t in enumerate(total_members_iterate):

                    unique_member_id = None; member_name = None; unique_member_url = None; 

                    unique_member_id = int(re.findall(r'(?:members/)(\d+)',t['href'])[0])

                    member_name = t.get_text()

                    unique_member_url = t['href']

                    unique_member = requests.get(unique_member_url)

                    unique_member_soup = BeautifulSoup(unique_member.content,'html.parser')



                    unique_city = None; unique_state = None; unique_member_since = None

                    unique_member_number_of_other_groups=None; others = None; status = None

                    try:

                        unique_city = unique_member_soup.find('span',class_ = 'locality', itemprop = 'addressLocality').get_text()

                        unique_state = unique_member_soup.find('span',class_ = 'region', itemprop = 'addressRegion').get_text()

                        unique_member_since = unique_member_soup.find('div', id = 'D_memberProfileMeta').find_all('div', 

                                                            'D_memberProfileContentItem')[1].p.get_text()



                        try:

                            unique_member_number_of_other_groups = int(re.findall(r'\d+',

                                                unique_member_soup.find('div', class_ = 'D_memberProfileGroups').find('div', 

                                                class_ = 'block-list-header rounded-corner-top clearfix').find('h3', 

                                                class_ = 'big flush--bottom').get_text().strip('\n'))[0])

                        except:

                            status = 'Absent'



                        other_groups = None

                        if(status != 'Absent'):

                            others = unique_member_soup.find('div', class_ =  'see-more-groups').find('a',

                                                                                                      class_ = 'canDo')['href']



                            if(others):

                                other_groups_soup = BeautifulSoup(requests.get(others).content,'html.parser')

                                other_groups = other_groups_soup.find('ul', id = 'my-meetup-groups-list').find_all('li')

                            else:

                                other_groups = unique_member_soup.find('ul', id = 'my-meetup-groups-list').find_all('li')



                        unique_member_other_groups = []

                        if(other_groups):

                            for o in other_groups:

                                unique_member_other_groups.append(o.find('div', class_ = 'D_name bold').get_text())



                        unique_member_interests = None

                        unique_member_interests = [x.get_text() for x in unique_member_soup.find('div', 

                                                   class_ = 'interest-topics').find('ul',

                                                   id = 'memberTopicList').li.div.find_all('a')]

                    except:

                        pass





                        total_members_list.append([unique_member_id,member_name,unique_city, unique_state,

                                                   unique_member_since, unique_member_number_of_other_groups,

                                                   unique_member_other_groups, unique_member_interests])

                    print_key += 1

                    print('Total members done till now ---> {0}/{1}'.format(print_key,total_members),end = '\r')





            final_list.append([grp_name,past_meetups, city, state, total_members, group_privacy, founder, founding_date,

                               about_list, total_members_list])



            meetup_group_list.append(final_list)

        except:

            pass

            

    else:

        meetup_group_list.append('Url not working')



    x= no

    with open(pickle_path,'wb') as f:

        pickle.dump([x, final_index, new_base_urls, meetup_group_list],f)

    print("Done with Iteration no {0} of the URLs".format(final_index+no))

# Meetup-groups_Data-Scraping-and-Analysis

1.Descriptive Analysis of various Meetup-groups data from the website www.meetup.com and deriving meaningful insights from it.

2.1-2.3 Scraping the meetup website to collect and gather data for every group, about its founders, members, location, interests of the member of the groups, genres of groups, other groups a particular member is part of.

For the scraping, owing to the vast number of member URLs, for every Group, which the script had to hit, devised an ingenious mechanism for storing the state of the data captured and dumping them into pickle file, ensuring that on server failure or normal interruption of the script, the scraping would still resume from the last Group URL it visited, maintaining the continuity of the scraping.

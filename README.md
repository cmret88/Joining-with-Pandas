# Joining-with-Pandas

#### INNER JOIN ####
# Tables = DataFrame in this course
# merging = joining
# You will often have to join data tables together

# Chicago Ward data
wards = pd.read_csv('Ward_Offices.csv')
print(wards.head())
print(wards.shape)

# Census data
census = pd.read_csv('Ward_Census.csv')
print(census.head())
print(census.shape)

# Inner join
# the merge method takes the first DataFrame, wards, and merges it with the second DataFrame, census
# we use the 'on' argument to tell the method that we want to merge the two DataFrames on the ward column
# since the wards table is listed first, its columns will appear first in the output, followed by the columns from the census table
# this example returns a DataFrame with 50 rows and 9 columns where the returned rows have matching values for the ward column in both tables, called an Inner Join
# an Inner Join will only return rows that have matching values in both tables
wards_census = wards.merge(census, on = 'ward') 
print(wards_census.head(4))
print(wards_census.shape)

# if columns have the same name in both tables, they will be given suffixes in the output
# we can use the suffix argument of the merge method to control this behavior
# we provide a tuple where all the overlapping columns in the left table are given a suffix '_ward'
# those of the right will be given the suffix '_cen'
wards_census = wards.merge(census, on = 'ward', suffixes = ('_ward','_cen'))
print(wards_census.head())
print(wards_census.shape)

# Merge the taxi_owners and taxi_veh tables setting a suffix
taxi_own_veh = taxi_owners.merge(taxi_veh, on='vid', suffixes=('_own','_veh'))

# Print the value_counts to find the most popular fuel_type
print(taxi_own_veh['fuel_type'].value_counts())

# Print the first few rows of the wards_altered table to view the change 
print(wards_altered[['ward']].head())

# Print the first few rows of the census_altered table to view the change 
print(census_altered[['ward']].head())

# Merge the wards and census_altered tables on the ward column
wards_census_altered = wards.merge(census_altered, on = 'ward')

# One-to-many relationships
# one-to-one relationship -> every row in the left table is related to only one row in the right table
# example -> every row in the wards table is related to only one row in the census table, so there is only one row for ward 3 in each table

# one-to-many -> every row in the left table is related to one or more rows in the right table
# example -> within each ward, there are many businesses, we will merge the wards table with a table of licensed businesses in each ward
licenses = pd.read_csv('Business_Licenses.csv')
print(licenses.head())
print(licenses.shape)

ward_licenses = wards.merge(licenses, on = 'ward', suffixes = ('_ward','_lic'))
prince(ward_licenses.head())

# (50,4) 50 rows, 4 columns
print(wards.shape)

# (10000,9) 10000 rows, 9 columns after merging the tables 
print(ward_licenses.shape)

# Merge the licenses and biz_owners table on account
licenses_owners = licenses.merge(biz_owners, on = 'account')

# Group the results by title then count the number of accounts
counted_df = licenses_owners.groupby('title').agg({'account':'count'})
print(counted_df)

# Sort the counted_df in descending order
sorted_df = counted_df.sort_values('account', ascending = False)

# Use .head() method to print the first few rows of sorted_df
print(sorted_df.head())

#### MERGING MULTIPLE DATAFRAMES ####

# Theoretical merge
# If we merge the two tables only using the zip column, then the 60616 zip of Reggie's Bar from licenses table will be matched to multiple businesses with the same zip
# the output won't be what we want
# even if we merged on the address, we run the risk on having the incorrect output
grants_licenses = grants.merge(licenses, on = 'zip')
print(grants_licenses.loc[grants_licenses['business']=='Reggie's Bar & Grill',
                          ['grant','company','account','ward','business']]

# the best solution is to merge on both zip code and address
# we pass a list of column names we want to merge on to the 'on' argument
# columns from grants table are listed first
# however, when we merge on two columns, we are requiring that both the address and zip code between the tables to be matched in order for them to be linked in the merge

# Merging multiple tables
grants_licenses_ward = grants.merge(licenses, on =['address','zip'])\
                        .merge(wards, on='ward', suffixes=('_bus','_ward'))
grants_licenses_ward.head()

# Results
import matplotlib.pyplot as plt
grant_licenses_ward.groupby('ward').agg('sum').plot(kind='bar',y='grant')
plt.show()

# Further merging
# Three tables
df1.merge(df2, on = 'col')\
    .merge(df3, on = 'col')

# Four tables
df1.merge(df2, on = 'col')\
    .merge(df3, on = 'col')\
    .merge(df4, on = 'col')

#### Practice ####
# Merge the ridership and cal tables
ridership_cal = ridership.merge(cal, on = ['year','month','day'])

# Merge the ridership, cal, and stations tables
ridership_cal_stations = ridership.merge(cal, on=['year','month','day']) \
            				.merge(stations, on = 'station_id')

# Create a filter to filter ridership_cal_stations
filter_criteria = ((ridership_cal_stations['month'] == 7) 
                   & (ridership_cal_stations['day_type'] == 'Weekday') 
                   & (ridership_cal_stations['station_name'] == 'Wilson'))

# Use .loc and the filter to select for rides
print(ridership_cal_stations.loc[filter_criteria, 'rides'].sum())

# Merge licenses and zip_demo, on zip; and merge the wards on ward
licenses_zip_ward = licenses.merge(zip_demo, on = 'zip') \
            			.merge(wards, on = 'ward')

# Print the results by alderman and show median income
print(licenses_zip_ward.groupby('alderman').agg({'income':'median'}))

# Merge land_use and census and merge result with licenses including suffixes
land_cen_lic = land_use.merge(census, on = 'ward')\
                        .merge(licenses, on = 'ward', suffixes = ('_cen','_lic'))

# Group by ward, pop_2010, and vacant, then count the # of accounts
pop_vac_lic = land_cen_lic.groupby(['ward','pop_2010','vacant'], 
                                   as_index=False).agg({'account':'count'})

# Sort pop_vac_lic and print the results
sorted_pop_vac_lic = pop_vac_lic.sort_values(['vacant','account','pop_2010'], 
                                             ascending=[False,True,True])

# Print the top few rows of sorted_pop_vac_lic
print(sorted_pop_vac_lic.head())

#### LEFT JOIN ####
# Returns all rows from the left table and only those rows from the right table where column C matches them both
# We will use the Movies and Tagline databases for this lesson
movies = pd.read_csv('tmdb_movies.csv')
print(movies.head())
print(movies.shape)

taglines = pd.read.csv('tmdb_taglines.csv')
print(taglines.head())
print(taglines.shape)

# Merge with left join
# in this example, we list the movie table first and merge it to the taglines table on the ID column in both tables
# left join includes the argument 'how', where we are prompted to indicate how we want to join the tables (default 'how' is inner join)
# the resulting table shows a 1-to-1 merge; a left join will always return the same number of rows of the left table, if not more
movies_taglines = movies.merge(taglines, on = 'id', how = 'left')
print(movies_taglines.head())

# Merge the movies table with the financials table with a left join
movies_financials = movies.merge(financials, on='id', how='left')

# Count the number of rows in the budget column that are missing
number_of_missing_fin = movies_financials['budget'].isnull().sum()

# Print the number of movies missing financials
print(number_of_missing_fin)

#### OTHER JOINS ####
# Right Join
# Returns all of the rows from the right table and includes only those rows from the left table that have matching values
# Mirror opposite of left join
# In this example, we will use a right join to check that our movies table is not missing data
movies_to_genres = pd.read_csv('tmdb_movie_to_genres.csv')
tv_genre = movie_to_genres[movie_to_genres['genre'] == 'TV Movie'] # subsetting for TV Movie genre only
print(tv_genre)

# subsetting for TV Movie genre only
m = movie_to_genres['genre'] == 'TV Movie'
tv_genre = movie_to_genres[m]
print(tv_genre)

# Our goal is to merge it with the movies table
# Movies will be left table and tv_genre table will be right table

# Merging with Right Join
# how = 'right' tells Python to perform a right join
# 'left_on' and 'right_on' tell the merge which key columns from each table to merge the tables
# this is particularly useful when you have columns that contain the same type of values, but the column names are different
# think a.id_medicaid = b.id_medicaid_crnt in SQL
tv_movies = movies.merge(tv_genre, how = 'right', 
                          left_on = 'id", right_on = 'movie_id') 
print(tv_movies.head())

# Outer Join
# Returns all of the rows from both tables regardless if there is a match between the tables
# This often results in missing values
m = movies_to_genre['genre'] == 'Family'
family = movie_to_genre[m].head(3)

m = movie_to_genres['genre'] == 'Comedy'
comedy = 'movie_to_genres[m].head(3)

# in this merge, we list the fmaily table as the left table and merge it on the movie_id column
# how argument is set to outer
# we add suffixes to show what table the columns originated from
# results show every row is returned for both tables and we see some null values
family_comedy = family.merge(comedy, on = 'movie_id', how = 'outer',
                              suffixes = ('_fam', '_com'))
print(family_comedy)



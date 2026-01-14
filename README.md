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
# You can use Outer Join to find rows that do not have a match in the other table
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

scifi_only = action_scifi['genre_act'.isnull()]
# What is the issue?

# The problem here is with how .isnull() is being used. In pandas, .isnull() is a method that should be called on a pandas Series (like a column of a DataFrame), not on a string. In your code, 'genre_act'.isnull() tries to call .isnull() on the string 'genre_act', which causes the error: 'str' object has no attribute 'isnull'.

# To check for null values in the 'genre_act' column, you need to first select the column from the DataFrame (action_scifi['genre_act']), and then call .isnull() on that Series.

# Merge action_movies to the scifi_movies with right join
action_scifi = action_movies.merge(scifi_movies, on='movie_id', how='right',
                                   suffixes=('_act','_sci'))

# From action_scifi, select only the rows where the genre_act column is null
scifi_only = action_scifi[action_scifi['genre_act'].isnull()]

# Merge the movies and scifi_only tables with an inner join
movies_and_scifi_only = movies.merge(scifi_only, left_on = 'id', right_on = 'movie_id')

# Print the first few rows and shape of movies_and_scifi_only
print(movies_and_scifi_only.head())
print(movies_and_scifi_only.shape)

# Use right join to merge the movie_to_genres and pop_movies tables
genres_movies = movie_to_genres.merge(pop_movies, how='right', 
                                      left_on = 'movie_id', 
                                      right_on = 'id')

# Count the number of genres
genre_count = genres_movies.groupby('genre').agg({'id':'count'})

# Plot a bar chart of the genre_count
genre_count.plot(kind='bar')
plt.show()

# Merge iron_1_actors to iron_2_actors on id with outer join using suffixes
iron_1_and_2 = iron_1_actors.merge(iron_2_actors,
                                     on = 'id',
                                     how = 'outer',
                                     suffixes=('_1','_2'))

# Create an index that returns true if name_1 or name_2 are null
m = ((iron_1_and_2['name_1'].isnull()) | 
     (iron_1_and_2['name_2'].isnull()))

# Print the first few rows of iron_1_and_2
print(iron_1_and_2[m].head())

#### Merging a Table to Itself ####
# Also referred to as a self join
# Let's try merging a table to itself to create a new table whose rows show movies and their sequels
# the join rules still apply; we apply suffixes to the columns describe the original movie and the ones that describe the sequel
original_sequels = sequels.merge(sequels, left_on = 'sequel', right_on = 'id',
                                suffixes = ('_org','_seq'))
print(original_sequels.head())

# We can also select only the title_org and 'title_seq' columns, and we can see that we've successfully linked each movie to its sequel
print(original_sequels[,['title_org','title_seq']].head())

# When merging a table to itself, we can use the different types of joins we have already reviewed
original_sequels = sequels.merge(sequels, left_on = 'sequel, right_on = 'id',
                                  how = 'left', suffixes = ('_org','_seq'))
print(original_sequels.head())

# Common situations to merge a table to itself:
# hierarchical relationships, sequential relationships, graph data

# Merge the crews table to itself
crews_self_merged = crews.merge(crews, on='id', how='inner',
                                suffixes=('_dir','_crew'))

# Create a boolean index to select the appropriate rows
boolean_filter = ((crews_self_merged['job_dir'] == 'Director') & 
                  (crews_self_merged['job_crew'] != 'Director'))
direct_crews = crews_self_merged[boolean_filter]

# Print the first few rows of direct_crews
print(direct_crews.head())

#### Merging on indexes ####
# Setting an index
movies = pd.read_csv('tmdb_movies.csv', index_col = ['id'])
print(movies.head())

# Merge index sets
# the merge method automatically adjusts to accept index names or column names
# the returned table looks as before (when we set the index), except the 'id' is the index
movies_taglines - movies.merge(taglines, on = 'id', how = 'left')
print(movies_taglines.head())

# Multi-Index datasets
samuel = pd.read_csv('samuel.csv', index_col = ['movie_id','cast_id'])
print(samuel.head())

casts = pd.read_csv('casts.csv', index_col = ['movie_id', 'cast_id'])
print(casts.head())

# Multi-Index merge
# in this merge, we pass in a list of index level names to the 'on' argument, just like we did when merging on multiple columns
# since this is an inner join, both the movie_id and cast_id must match in each table to be returned in the result
# if the index level names are different between the two tables that we want to merge, then we can use the left_on and right_on arguments of the merge method
samuel_casts = samuel.merge(casts, on =['movie_id','cast_id'])
print(samuel_casts.head())
print(samuel_casts.shape)

# Since we are merging on indexes, we need to set left_index and right_index to True
# These arguments only take True or False
# The left_index and right_index tell the merge method to use the separate indexes
movies_genres = movies.merge(movie_to_genres, left_on = 'id', left_index = True,
                              right_on = 'movie_id', right_index = True)
print(movies_genres.head())

# Merge to the movies table the ratings table on the index
movies_ratings = movies.merge(ratings, on = 'id', how = 'left')

# Print the first few rows of movies_ratings
print(movies_ratings.head())

# Merge sequels and financials on index id
sequels_fin = sequels.merge(financials, on='id', how='left')

# Self merge with suffixes as inner join with left on sequel and right on id
orig_seq = sequels_fin.merge(sequels_fin, left_on='sequel', 
                             right_on='id', right_index=True,
                             suffixes=['_org','_seq'])

# Add calculation to subtract revenue_org from revenue_seq 
orig_seq['diff'] = orig_seq['revenue_seq'] - orig_seq['revenue_org']

# Merge sequels and financials on index id
sequels_fin = sequels.merge(financials, on='id', how='left')

# Self merge with suffixes as inner join with left on sequel and right on id
orig_seq = sequels_fin.merge(sequels_fin, how='inner', left_on='sequel', 
                             right_on='id', right_index=True,
                             suffixes=('_org','_seq'))

# Add calculation to subtract revenue_org from revenue_seq 
orig_seq['diff'] = orig_seq['revenue_seq'] - orig_seq['revenue_org']

# Select the title_org, title_seq, and diff 
titles_diff = orig_seq[['title_org','title_seq','diff']]

# Print the first rows of the sorted titles_diff
print(titles_diff.sort_values(by='diff', ascending=False).head())

#### Filtering Joins ####
# Pandas does not provide direct support for filter joins
# But we will learn how to replicate them
# Mutating joins combines data from two tables based on matching observations in both tables
# Filtering joins filters observations from table based on whether or not they match an observation in another table
# A semi join filters the left table down to those observations that have a match in the right table
# similar to an inner join where the only intersection between tables is returned
# but they are unlike an inner join in that only the columns on the left table are shown, not the right
# no duplicate rows from the left table are returned, even if there is a one-to-many relationship

# Steps for a Semi Join
# Step 1 - inner join
genres_tracks = genres.merge(top_tracks, on = 'gid')
print(genres_tracks.head())

# Step 2 - isin()
# uses isin(), which compares every 'gid' in the genres table to the 'gid' in the genres_tracks table
# this will tell us if our genre appears in our merged genres_tracks table
# returns a Boolean series of True or False values
genres['gid'].isin(genres_tracks['gid'])

# Step 3 - semi join
# we use the line of code from Step 2 to subset the genres table
genres_tracks = genres.merge(top_tracks, on = 'gid')
genres['gid'].isin(genres_tracks['gid'])
print(top_genres.head())

# this is called a filtering join because we filtered the genres table by what's in the top_tracks table

#### Anti-Join ####
# returns the left table, excluding the intersection
# returns only columns from the left table and not the right
# we can use an anti-join to, say, find songs that are NOT in the top 10

# Step 1 - use left join
# with the indicator set to True, the merge method adds a column called '_merge' to the output
# this column tells the source for each row
genres_tracks = genres.merge(top_tracks, on = 'gid', how = 'left', indicator = True)
print(genres_tracks.head())

# Step 2 - use 'loc' accessor and '_merge' column
# this selects only rows that appear in the left table and return only the 'gid' column from the genres_tracks table
# this gives you a list of gids NOT in the tracks table
gid_list = genres_tracks.loc[genres_track['_merge'] == 'left_only', 'gid']
print(gid.list.head())

# Step 3 - use isin() to filter
# we use isin() to filter for the rows with gids in our gid_list
# output shows those genres NOT in the tracks table
genres_tracks = genres.merge(top_tracks, on = 'gid', how = 'left', indicator = True)
gid_list = genres_tracks.loc['_merge'] == 'left_only', 'gid']
non_top_genres = genres[genres['gid'].isin(gid_list)]
print(non_top_genres.head())

# Merge employees and top_cust
empl_cust = employees.merge(top_cust, on='srid', 
                            how='left', indicator=True)
# Select the srid column where _merge is left_only
srid_list = empl_cust.loc[empl_cust['_merge'] == 'left_only', 'srid']

# Get employees not working with top customers
print(employees[employees['srid'].isin(srid_list)])

# Merge the non_mus_tcks and top_invoices tables on tid
tracks_invoices = non_mus_tcks.merge(top_invoices, on = 'tid')

# Use .isin() to subset non_mus_tcks to rows with tid in tracks_invoices
top_tracks = non_mus_tcks[non_mus_tcks['tid'].isin(tracks_invoices['tid'])]

# Group the top_tracks by gid and count the tid rows
cnt_by_gid = top_tracks.groupby(['gid'], as_index=False).agg({'tid':'count'})

# Merge the genres table to cnt_by_gid on gid and print
print(cnt_by_gid.merge(genres, on = 'gid'))

#### Concatenate DataFrames together vertically ####
# we can use the concatenate method to stick tables together vertically or horizontally
# pandas .concat() method can concatenate both vertical and horizontal
# axis = 0, vertical
# Basic concatenation - 3 tables, same column names (think union in SQL)
# we can pass a list of table names into pd.concat to combine the tables in the order they're passed in
# to concatenate vertically, the axis argument should be set to 0, but it is 0 by default, so we don't need to write this
# when we concatenate, each table's index value is retained
pd.concat([inv_jan, inv_feb, inv_mar])

# concatenate and ignoring the index
# if the index contains no valuable info, then we can ignore it in the concat method by setting the ignore_index to True
pd.concat([inv_jan, inv_feb, inv_mar], ignore_index = True)

# Setting labels to original tables 1:36 mark
# suppose we wanted to associate specific keys with each of the pieces of our three original tables
# we can provide a list of labels to the keys argument
# we need to make sure that ignore_index argument is False, since you can't add a key and ignore the index at the same time
# the results is a multi-index table, with the label on the first level
pd.concat([inv_jan, inv_feb, inv_mar], ignore_index = False, keys = ['jan','feb','mar'])

# Concatenate tables with different column names
# the concat method by default will include all of the columns in the different tables its combining
# the sort argument, if true, will alphabetically sort the different column names in the result
# this will result in NaN values for some rows
# if we only want the matching columns between tables, we can set the join argument to 'inner'
# the order of the columns will be the same as the input tables
# this will lead to the unmatching column gone and we are only left with the columns the tables have in common
pd.concat([inv_jan, inv_feb], join = 'inner')
pd.concat([inv_jan, inv_feb], sort = True)

# Concatenate the tracks
tracks_from_albums = pd.concat([tracks_master, tracks_ride, tracks_st], sort=True)

# Concatenate the tracks so the index goes from 0 to n-1
tracks_from_albums = pd.concat([tracks_master, tracks_ride, tracks_st],
                               ignore_index = True,
                               sort=True)
                               
# Concatenate the tracks, show only columns names that are in all tables
tracks_from_albums = pd.concat([tracks_master, tracks_ride, tracks_st], 
                               join = 'inner',
                               sort=True)

# Concatenate the tables and add keys
inv_jul_thr_sep = pd.concat([inv_jul, inv_aug, inv_sep], 
                            keys=['7Jul', '8Aug', '9Sep'])

# Group the invoices by the index keys and find avg of the total column
avg_inv_by_month = inv_jul_thr_sep.groupby(level=0).agg({'total':'mean'})

# Bar plot of avg_inv_by_month
avg_inv_by_month.plot(kind = 'bar')
plt.show()

#### Verifying Integrity ####
# Both the merge and concat methods have special features that allow us to verify the structure of our data
# when merging two tables, we might expect the tables to have a one-to-one relationship
# however, one of the columns we are merging on might have a duplicated value, which will turn the relationship into one-to-many
# we might untintentionally create duplicate records if a record exists in both tables if a record exists in both tables
# the validate and verify_integrity arguments of the merge and concat methods respectively will allow us to verify the method
# if we provide the validate argument on of these key strings, it will validate the relationship between the two tables
.merge(validate = None):
# checks if merge is of specified type
# one-to-one
# one-to-many
# many-to-one
# many-to-many

# if we provide the validate argument one of these key strings, it will validate the relationship between the two table

# Merge validate: one-to-one
# in the result, a MergeError is raised
# 'MergeError: Merge keys are not unique in right dataset; not a one-to-one marge
# we need to handle these duplicates properly before merging
tracks.merge(specs, on = 'tid', validate = 'one_to_one')

# Merge validate: one-to-many
# when we set the validate argument to 'one_to_many' no error is raised is the below example
albums.merge(tracks, on = 'aid', validate = 'one_to_many')

# Verifying concatenations
# checks whether the new concatenated index contains duplicates
# default value is False
# if set to True, it will check if there are duplicate values in the index and raise an error if there are
.concat(verify_integrity=False)

# Dataset for .concat() example
# the concat method raises a ValueError stating that the indexes have overlapping values
pd.concat([inv_feb, inv_mar], verify_integrity = True)

pd.concat([inv_feb, inv_mar], verify_integrity = False)
# the concat method now returns a combined table with the invoice ID of number 9 repeated twice in this example

# real world data is often NOT clean
# if you receive a MergeError or a ValueError, you can fix the incorrect data or drop duplicate rows

# Concatenate the classic tables vertically
classic_18_19 = pd.concat([classic_18, classic_19], ignore_index=True)

# Concatenate the pop tables vertically
pop_18_19 = pd.concat([pop_18, pop_19], ignore_index=True)

# Merge classic_18_19 with pop_18_19
classic_pop = classic_18_19.merge(pop_18_19, on = 'tid', how = 'inner')

# Using .isin(), filter classic_18_19 rows where tid is in classic_pop
popular_classic = classic_18_19[classic_18_19['tid'].isin(classic_pop['tid'])]

# Print popular chart
print(popular_classic)

#### Using merge_ordered() ####
# this method can merge time-series and other ordered data
# the results are similar to the standard merge method with an outer join, but the results are sorted

# .merge() & merge_ordered() methods similarities:
# both contain arguments to allow us to merge two tables on different columns (on, left_on, right_on)
# both methods support different types of joins (how = left, right, inner, outer), but .merge() has default inner join and merge_ordered() is default outer join
# both methods support suffixes for overlapping column names

# .merge() & merge_ordered() methods differences:
# how you call on each of the methods is different
# df1.merge(df2) vs. pd.merge_ordered(df1, df2)

# Merging stock data example (using Apple and McDonald's stock prices)
# this results in a table sorted by date
import pandas as pd
pd.merge_ordered(aapl, mcd, on = 'date', suffixes = ('_aapl','_mcd'))

# if there is no value for a column it is shown as NaN
# we can fill in this missing data using a technique called forward filling
# forward filling fills the missing value with the previous value

# Forward fill example
pd.merge_ordered(aapl, mdc, on = 'date',
                 suffixes = ('_aapl','_mcd'),
                 fill_method='ffill')

# we use merge_ordered() in ordered data/time series data
# useful for handling missing data (most machine learning algorithms require no missing values)

# Use merge_ordered() to merge gdp and sp500 on year and date
gdp_sp500 = pd.merge_ordered(gdp, sp500, left_on='year', right_on='date', 
                             how='left')

# Use merge_ordered() to merge gdp and sp500, and forward fill missing values
gdp_sp500 = pd.merge_ordered(gdp, sp500, left_on='year', right_on='date', 
                             how='left',  fill_method='ffill')

# Subset the gdp and returns columns
gdp_returns = gdp_sp500[['gdp','returns']]

# Print gdp_returns correlation
print (gdp_returns.corr())

# Use merge_ordered() to merge inflation, unemployment with inner join
inflation_unemploy = pd.merge_ordered(inflation, unemployment, on = 'date', how = 'inner')

# Print inflation_unemploy 
print(inflation_unemploy)

# Plot a scatter plot of unemployment_rate vs cpi of inflation_unemploy
inflation_unemploy.plot(x='unemployment_rate', y='cpi', kind='scatter')
plt.show()

# Merge gdp and pop on date and country with fill and notice rows 2 and 3
ctry_date = pd.merge_ordered(gdp, pop, on = ['date','country'],
                             fill_method='ffill')

# Merge gdp and pop on country and date with fill
date_ctry = pd.merge_ordered(gdp, pop, on = ['country','date'], fill_method = 'ffill')

#### Using merge_asof() ####
# Another method for ordered or time-series data
# similar to an ordered left join
# similar features to merge_ordered
# but unlike an ordered left join, merge_asof() will match on the nearest value columns rather than equal values
# match on the nearest key column and not exact matches
# merged 'on' columns must be sorted

# our output is similar to a left join, so we see all of the rows from the left Visa table
# however, the values of the IBM table are based on how close the date_time values match with the Visa table
pd.merge_asof(visa, ibm, on = 'date_time', suffixes = ('_visa','_ibm'))

# in this example, we will list the direction argument as 'forward'
# this will change the behavior of the method to select the first row in the right table whose 'on' key column is greater than or equal to the left's key column
# the default value for the direction argument is 'backward'
pd.merge_asof(visa, ibm, on = ['date_time'], suffixes = ('_visa','_ibm'), direction = 'foward')

# when we look at our results, we see different values for the IBM column

# finally, you can set the direction argument to 'nearest' which returns the nearest row in the right table regardless if it is forward or backwards

# when to use merge_asof():
# data sampled from a process
# developing a training set (no data leakage)
# working on a time-series training set where you do not want any events from the future to be visible that point in time

# Use merge_asof() to merge jpm and wells
jpm_wells = pd.merge_asof(jpm, wells, on = 'date_time', 
                          suffixes = ('','_wells'), 
                          direction = 'nearest')

# Use merge_asof() to merge jpm_wells and bac
jpm_wells_bac = pd.merge_asof(jpm_wells, bac, on = 'date_time',
                              suffixes = ('_jpm','_bac'),
                              direction = 'nearest')

# Compute price diff
price_diffs = jpm_wells_bac.diff()

# Plot the price diff of the close of jpm, wells and bac only
price_diffs.plot(y=['close_jpm', 'close_wells', 'close_bac'])
plt.show()

# Merge gdp and recession on date using merge_asof()
gdp_recession = pd.merge_asof(gdp, recession, on = 'date')
print(gdp.head())
print(recession.head())
print(gdp_recession.head())

# Create a list based on the row value of gdp_recession['econ_status']
is_recession = ['r' if s=='recession' else 'g' for s in gdp_recession['econ_status']]

# Plot a bar chart of gdp_recession
gdp_recession.plot(kind='bar', y='gdp', x='date', color=is_recession, rot=90)
plt.show()

#### Selecting data with .query() ####
# Pandas provides many methods for selecting data
.query('SOME SELECTION STATEMENT')
# accepts an input string
# input string used to determine what rows are returned
# input string similar to statement after WHERE clause in SQL statement

# Querying on a single condition
# here we provide a string to the query method
# the string identifies that we want to condition which rows are returned by the value of the Nike column
# the method returns all rows in stocks where Nike is greater than or equal to 90
stocks.query('nike >=90')











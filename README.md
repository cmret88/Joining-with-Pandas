# Joining-with-Pandas

# Inner Join
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

# Importing libraries I might use
import pandas as pd
import numpy as np
import sqlite3

# Connecting to the db
connection = sqlite3.connect("interview.db")
cursor = connection.cursor()

# Function used to correct dates for Dob and eligibility dates in roster_2
def correct_date_format(dateString):
    numbers = dateString.split("/")
    # Changing to correct format yyyy-mm-dd
    correct_date = numbers[2] + '-' + numbers[0] + '-' + numbers[1]
    return correct_date

# I confirmed that so I need to do some string manipulation
# Making changes to roster_2 using pandas:
df_roster_2 = pd.read_sql_query('select * from roster_2', connection)
df_roster_2['Dob'] = df_roster_2['Dob'].apply(lambda date: correct_date_format(date))
df_roster_2['eligibility_start_date'] = df_roster_2['eligibility_start_date'].apply(lambda date: correct_date_format(date))
df_roster_2['eligibility_end_date'] = df_roster_2['eligibility_end_date'].apply(lambda date: correct_date_format(date))
df_roster_2.head()

# Write altered dataframe to SQLite database
cursor.execute("DROP TABLE IF EXISTS roster_2_fixed")
df_roster_2.to_sql("roster_2_fixed", connection, if_exists="replace", index=False)
# NOTE: roster_2 table is locked so I have to basically rename it and stored the fixed table as a new one

# Creating table called "std_member_info" (takes a second)
cursor.execute("DROP TABLE IF EXISTS std_member_info")

# Ordering columns for EACH table because UNION considers column order, NOT column name
colNames = "Person_Id, First_Name, Last_Name, Dob, Age, Gender, Street_Address, State, City, Zip, eligibility_start_date, eligibility_end_date, payer"
r1_str = "SELECT " + colNames + " FROM roster_1"
r2_str = "SELECT " + colNames + " FROM roster_2_fixed"
r3_str = "SELECT " + colNames + " FROM roster_3"
r4_str = "SELECT " + colNames + " FROM roster_4"
r5_str = "SELECT " + colNames + " FROM roster_5"

# Executing query
query = "CREATE TABLE std_member_info AS " + r1_str + " UNION " + r2_str + " UNION " + r3_str + " UNION " + r4_str + " UNION " + r5_str
result = cursor.execute(query)

# Note 1: Logically, this could be done more efficiently by filtering BEFORE joining, but I'll keep it before filtering and dropping the columns for simplicity's sake
# Note 2: If the challenge didn't specify to 'create' a table I would have looked into creating a view so that the same data isn't stored again when creating the aggregate table

# Aggregating by Person_Id
print("How many members were included more than once?")
query = "SELECT Person_Id, COUNT(Person_Id) as num_duplicates FROM std_member_info GROUP BY Person_Id HAVING num_duplicates > 1"
result = cursor.execute(query)
print(len(result.fetchall()), "members were included more than once.")

# Removing unique values
query = "DELETE FROM std_member_info WHERE rowid NOT IN (SELECT MIN(rowid) FROM std_member_info GROUP BY Person_Id)"
cursor.execute(query)

print("How many distinct members are eligible in April 2022?")
# ASSUMPTION: I'm assuming "Eligible in April 2022" means eligible at ANY point in that month, not necessarily for the whole duration
# Ex: Eligible from 2022-04-01 to 2022-04-02 is 'eligible in April 2022'
query = "SELECT COUNT(*) FROM std_member_info WHERE eligibility_start_date <= '2022-04-31' AND eligibility_end_date >= '2022-04-01'"
result = cursor.execute(query)
print("# of distinct members eligible in April 2022:", result.fetchone()[0])

# Dropping members that are not eligible in April 2022
query = "DELETE FROM std_member_info WHERE Person_Id NOT IN (SELECT Person_Id FROM std_member_info WHERE eligibility_start_date <= '2022-04-31' AND eligibility_end_date >= '2022-04-01')"
cursor.execute(query)

# Now we can drop the two eligibility columns (as instructed by challenge)
# SQLite doesn't support ALTER TABLE [] DROP, so I'm going to just drop them using pandas
df = pd.read_sql_query('select Person_Id, First_Name, Last_Name, Dob, Age, Gender, Street_Address, State, City, Zip, payer from std_member_info', connection)
query = "DROP TABLE std_member_info"
cursor.execute(query)
df.to_sql("std_member_info", connection, if_exists="replace", index=False)

print("What is the breakdown of members by payer?")
query = "SELECT payer, AVG(age) as avg_age, COUNT(*) as num_members FROM std_member_info GROUP BY payer"
result = cursor.execute(query)
print(result.fetchall())
print("Madv has 37352 members with an average age of ~50 years, and Mdcd has 62455 members, also with an average age of ~50 years.")

print("How many members live in a zip code with a food_access_score lower than 2?")
query = "SELECT COUNT(*) FROM std_member_info m JOIN model_scores_by_zip z ON m.Zip = z.zcta WHERE food_access_score < 2"
result = cursor.execute(query)
print(result.fetchall())
# There are 7,685 members in a zip code with a food_access_score lower than 2

print("What is the average social isolation score for the members?")
query = "SELECT AVG(social_isolation_score) FROM std_member_info m JOIN model_scores_by_zip z ON m.Zip = z.zcta"
result = cursor.execute(query)
print(result.fetchall())
# The average social isolation score is 3.07

print("Which members live in the zip code with the highest algorex_sdoh_composite_score?")

print("Which zip code has the highest algorex_sdoh_composite_score?")
query = "SELECT zcta FROM model_scores_by_zip ORDER BY algorex_sdoh_composite_score DESC LIMIT 1"
result = cursor.execute(query)
print("Highest zip:", result.fetchall()[0])
# Zip code 95950 has the highest score, which is 8.77

# This is the table from the query above renamed
highest_ascore_table = "(SELECT zcta FROM model_scores_by_zip ORDER BY algorex_sdoh_composite_score DESC LIMIT 1)"
query = "SELECT Person_Id FROM std_member_info m JOIN model_scores_by_zip z ON m.Zip = z.zcta WHERE zcta IN " + highest_ascore_table
result = cursor.execute(query)

# Getting it into a nice list
members_raw = result.fetchall()
members = []
for tuple in members_raw[:]:
    members.append(tuple[0])
print("List of members:", members)

# Cleaning up
cursor.execute("DROP TABLE IF EXISTS roster_2_fixed")
cursor.close()
connection.close()

# Connecting to the db
connection = sqlite3.connect("interview.db")
cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM std_member_info")
print(cursor.fetchall())
cursor.close()
connection.close()
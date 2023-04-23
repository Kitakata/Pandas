import pandas

data_dir_path = "/mnt/c/deere/data/by_project/leo foods/"
file_name_interations = "RAW_interactions.csv"
file_name_recipes = "RAW_recipes.csv"
file_name_output1 = "output1.parquet"
file_name_output2 = "output1.parquet"

# Create dataframes 
df_recipes = pandas.read_csv(data_dir_path + file_name_recipes)
df_interations = pandas.read_csv(data_dir_path + file_name_interations)
df_interations["item_id"] = [x + 1 for x in df_interations.index]

# Merge dataframes
df_merged = pandas.merge(df_interations, df_recipes, left_on="recipe_id", right_on="id" )
df_output1 = df_merged[["user_id", "item_id", "rating", "review", "tags", "description"]]

# Outputing in a parquet file
df_output1.to_parquet(data_dir_path + file_name_output1, engine = 'pyarrow', compression = 'gzip')

# Filters and tops to output2
rating_filter = (df_output1["rating"] == 1) | (df_output1["rating"] == 5)
srs_TOP1000_reviewer = df_interations["user_id"].value_counts().head(1000)
srs_TOP1000_reviewed = pandas.merge(df_merged, df_interations["recipe_id"].value_counts().head(1000), left_on="recipe_id", right_index=True)["item_id"]

# Apply filters
df_output2 = df_output1[rating_filter]
df_output2 = pandas.merge(df_output2, srs_TOP1000_reviewer, left_on="user_id", right_index=True)
df_output2 = pandas.merge(df_output2, srs_TOP1000_reviewed, left_on="item_id", right_index=True)[["user_id", "item_id", "rating", "review", "tags", "description"]]

# Outputing in a parquet file
df_output2.to_parquet(data_dir_path + file_name_output2, engine = 'pyarrow', compression = 'gzip')
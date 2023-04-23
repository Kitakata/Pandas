import pandas

data_dir_path = "/mnt/c/deere/data/by_project/leo foods/"
file_name_interations = "RAW_interactions.csv"
file_name_recipes = "RAW_recipes.csv"
file_name_output1 = "output1.parquet"

# Create dataframes 
df_recipes = pandas.read_csv(data_dir_path + file_name_recipes)
df_interations = pandas.read_csv(data_dir_path + file_name_interations)
df_interations["item_id"] = [x + 1 for x in df_interations.index]

# Merge dataframes
df_output1 = pandas.merge(df_interations, df_recipes, left_on="recipe_id", right_on="id" )[["user_id", "item_id", "rating", "review", "tags", "description"]]

# print(df_output1.info())
print(df_output1)

# Outputing in a parquet file
df_output1.to_parquet(data_dir_path + file_name_output1, engine = 'pyarrow', compression = 'gzip')



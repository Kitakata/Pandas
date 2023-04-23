import pandas

data_dir_path = "/mnt/c/deere/data/by_project/leo foods/"
file_path_interations = "RAW_interactions.csv"
file_path_recipes = "RAW_recipes.csv"

# File test
file_interations = open(data_dir_path + file_path_recipes, "r")


# Dataframes 
df_recipes = pandas.read_csv(data_dir_path + file_path_recipes, nrows=10)
#df_interations = pandas.read_csv(data_dir_path + file_path_interations)
df_recipes['item_id'] = [x + 1 for x in df_recipes.index]


print(df_recipes["steps"][0])
print(df_recipes["description"][0])


# Output in parquet files
parquet_file = 'recipes_pd.parquet'
df_recipes.to_parquet(data_dir_path + parquet_file, engine = 'pyarrow', compression = 'gzip')
df_parquet = pandas.read_parquet(data_dir_path + parquet_file)





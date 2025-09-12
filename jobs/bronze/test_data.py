import pandas as pd
p = r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_delta\Bulgaria\Bulgaria_Retail_Sales_data_240101-250831___Sheet1\part-00001-e8c1d3c5-188c-416f-863b-eab1053d0866-c000.snappy.parquet"
df = pd.read_parquet(p)
# print(df.head(10))

print(df.columns.tolist())

print(f"{len(df.columns)} columns:")
for i, c in enumerate(df.columns, 1):
    print(f"{i}. {c}")

print(*df.columns, sep="\n")




# import pandas as pd

# p = r"C:\Users\mohanreddykotha\sumitomo_datalake\sumitomo\data\bronze_delta\Bulgaria\Bulgaria_Retail_Sales_data_240101-250831___Sheet1\part-00001-e8c1d3c5-188c-416f-863b-eab1053d0866-c000.snappy.parquet"
# keep = [
#     "order_no", "model_2", "order_date", "vin", "model_year",
#     "body", "series", "engine", "transmission", "colour",
#     "trim", "option_list",
# ]

# # Read only needed columns (fast); requires pyarrow
# df = pd.read_parquet(p, engine="pyarrow", columns=keep)

# Show first 20 rows as a dataframe-like print
print(df.head(20).to_string(index=False))

import duckdb

db_path = "/app/data/general_data/analytics.duckdb"
con = duckdb.connect(db_path)

print("Successfully connected to DuckDB!")

print("\n--- Row Counts by Source & Symbol ---")
con.sql("""
    SELECT data_source, symbol, COUNT(*) as total_days 
    FROM stg_combined_stocks 
    GROUP BY data_source, symbol
    ORDER BY data_source, symbol
""").show()

print("\n--- 5 Most Recent Trading Days ---")
con.sql("""
    SELECT trade_date, symbol, close_price, volume, data_source 
    FROM stg_combined_stocks 
    ORDER BY trade_date DESC 
    LIMIT 5
""").show()

con.close()
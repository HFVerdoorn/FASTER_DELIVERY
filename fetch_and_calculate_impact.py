import pandas as pd
from sqlalchemy import create_engine


def fetch_and_calculate_impact(query: str, broad_query: str):
    # Database connection setup
    engine = create_engine('mysql+pymysql://user:password@localhost/db_name')
    with engine.connect() as connection:
        # Dynamic SQL query building
        specific_result = connection.execute(query)
        broad_result = connection.execute(broad_query)

        # Process results into DataFrame
        specific_df = pd.DataFrame(specific_result.fetchall(), columns=specific_result.keys())
        broad_df = pd.DataFrame(broad_result.fetchall(), columns=broad_result.keys())

        # CVR impact calculations
        cvr_impact = specific_df['conversion_rate'] * 100

        # Uplift mapping
        uplift_mapping = {'2DD': 0.07, '3DD': 0.03, 'BOLT': 0.01, 'STANDARD': 0.00}
        specific_df['uplift'] = specific_df['type'].map(uplift_mapping)

        return specific_df, broad_df, cvr_impact

# Usage examples
specific_query = 'SELECT * FROM sales WHERE type="2DD";'
broad_query = 'SELECT * FROM sales;'
specific_data, broad_data, impact = fetch_and_calculate_impact(specific_query, broad_query)

print(specific_data)
print(broad_data)
print(impact)
import pandas as pd
from sqlalchemy import create_engine, text


def fetch_and_calculate_impact():
    # Establish database connection
    engine = create_engine('sqlite:///mydatabase.db')
    with engine.connect() as connection:
        # Execute query to fetch data
        result = connection.execute(text('SELECT * FROM my_table'))
        data = result.fetchall()

    # Processing data
    df = pd.DataFrame(data, columns=['column1', 'column2'])
    impact = df['column1'] * df['column2']
    df['impact'] = impact

    # Uplift mapping
    uplift_mapping = {
        'low': 'No Significant Impact',
        'medium': 'Moderate Impact',
        'high': 'High Impact'
    }
    df['uplift'] = df['impact'].apply(lambda x: uplift_mapping['low'] if x < 10 else (uplift_mapping['medium'] if x < 20 else uplift_mapping['high']))

    return df

# Example Usage
if __name__ == '__main__':
    impact_df = fetch_and_calculate_impact()
    print(impact_df)

# Another Example
example_data = {'column1': [5, 10, 15], 'column2': [2, 3, 4]}
example_df = pd.DataFrame(example_data)
print(example_df)
import pandas as pd
import sqlalchemy
from sqlalchemy import text

def fetch_and_calculate_impact(
    connection_string, 
    start_date, 
    end_date, 
    shut_off_tier, 
    fallback_tier, 
    zipcodes=None, 
    lanes=None, 
    brands=None
):
    """
    Pulls granular live forecast data and calculates CVR impact.
    Allows filtering by specific zipcodes, logistics lanes, and brands.
    """
    
    # 1. Establish database connection
    engine = sqlalchemy.create_engine(connection_string)
    
    # 2. Build the dynamic SQL query
    # We always pull at the day/zipcode/lane/brand level
    query = """
        SELECT 
            forecast_date as day,
            zipcode,
            lane,
            brand,
            forecasted_sessions,
            baseline_cvr,
            fastest_eligible_tier
        FROM logistics_forecast
        WHERE forecast_date >= :start_date 
          AND forecast_date <= :end_date
          AND fastest_eligible_tier = :shut_off_tier
    """
    
    # Dictionary to hold our SQL parameters safely
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "shut_off_tier": shut_off_tier
    }
    
    # Dynamically append filters if the user provided them
    if zipcodes:
        query += " AND zipcode IN :zipcodes"
        params["zipcodes"] = tuple(zipcodes)
        
    if lanes:
        query += " AND lane IN :lanes"
        params["lanes"] = tuple(lanes)
        
    if brands:
        query += " AND brand IN :brands"
        params["brands"] = tuple(brands)
        
    # 3. Fetch the data securely
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)
        
    if df.empty:
        print("No traffic matches these specific filters for the selected dates.")
        return df

    # 4. Apply the Business Logic (Uplifts)
    uplift_map = {
        "2DD": 0.07,
        "3DD": 0.03,
        "BOLT": 0.01,
        "STANDARD": 0.00
    }
    
    original_uplift = uplift_map.get(shut_off_tier, 0)
    new_uplift = uplift_map.get(fallback_tier, 0)
    
    # Calculate effective CVRs and resulting conversions
    df['original_effective_cvr'] = df['baseline_cvr'] * (1 + original_uplift)
    df['new_effective_cvr'] = df['baseline_cvr'] * (1 + new_uplift)
    
    df['original_conversions'] = df['forecasted_sessions'] * df['original_effective_cvr']
    df['new_conversions'] = df['forecasted_sessions'] * df['new_effective_cvr']
    
    # 5. Calculate final impact
    df['projected_lost_conversions'] = (df['original_conversions'] - df['new_conversions']).astype(int)
    
    # Clean up the output to only show the relevant operational columns
    output_columns = [
        'day', 'brand', 'lane', 'zipcode', 
        'forecasted_sessions', 'projected_lost_conversions'
    ]
    
    return df[output_columns].sort_values(by=['day', 'projected_lost_conversions'], ascending=[True, False])


# --- HOW YOUR TEAM WOULD USE THIS ---

# Example A: A highly specific query. 
# "What happens if we shut off 2DD on the LAX-to-JFK lane, specifically for Brand X, over the weekend?"
""
Specific_impact = fetch_and_calculate_impact(
    connection_string="your_db_string_here",
    start_date="2026-03-21",
    end_date="2026-03-22",
    shut_off_tier="2DD",
    fallback_tier="3DD",
    lanes=["LAX-JFK", "ONT-JFK"],
    brands=["Brand_X"]
)
"""
# Example B: A broad query.
# "What happens if we shut down 3DD nationwide for the next 7 days across all brands?"
""
broad_impact = fetch_and_calculate_impact(
    connection_string="your_db_string_here",
    start_date="2026-03-17",
    end_date="2026-03-24",
    shut_off_tier="3DD",
    fallback_tier="BOLT"
    # Notice we leave zipcodes, lanes, and brands blank so it pulls everything
)
"""
import logging
from typing import Optional, List
from datetime import datetime

import pandas as pd
import sqlalchemy
from sqlalchemy import text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define valid tiers as a constant
VALID_TIERS = {"2DD", "3DD", "BOLT", "STANDARD"}

UPLIFT_MAP = {
    "2DD": 0.07,
    "3DD": 0.03,
    "BOLT": 0.01,
    "STANDARD": 0.00
}

def validate_inputs(
    start_date: str,
    end_date: str,
    shut_off_tier: str,
    fallback_tier: str,
    zipcodes: Optional[List[str]] = None,
    lanes: Optional[List[str]] = None,
    brands: Optional[List[str]] = None
) -> None:
    """
    Validates all input parameters before database query.
    
    Raises:
        ValueError: If any input is invalid
    """
    # Validate date format and logic
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD: {e}")
    
    if start > end:
        raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")
    
    # Validate tier names
    if shut_off_tier not in VALID_TIERS:
        raise ValueError(f"shut_off_tier must be one of {VALID_TIERS}, got '{shut_off_tier}'")
    
    if fallback_tier not in VALID_TIERS:
        raise ValueError(f"fallback_tier must be one of {VALID_TIERS}, got '{fallback_tier}'")
    
    # Validate filter lists aren't empty
    if zipcodes is not None and len(zipcodes) == 0:
        raise ValueError("zipcodes list cannot be empty")
    if lanes is not None and len(lanes) == 0:
        raise ValueError("lanes list cannot be empty")
    if brands is not None and len(brands) == 0:
        raise ValueError("brands list cannot be empty")

def fetch_and_calculate_impact(
    connection_string: str,
    start_date: str,
    end_date: str,
    shut_off_tier: str,
    fallback_tier: str,
    zipcodes: Optional[List[str]] = None,
    lanes: Optional[List[str]] = None,
    brands: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Pulls granular live forecast data and calculates CVR impact.
    
    Allows filtering by specific zipcodes, logistics lanes, and brands.
    Compares the impact of shutting off one delivery tier in favor of another.
    
    Args:
        connection_string: SQLAlchemy connection string for database
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        shut_off_tier: Delivery tier to simulate shutting down (2DD, 3DD, BOLT, STANDARD)
        fallback_tier: Tier customers will fall back to
        zipcodes: Optional list of zipcodes to filter
        lanes: Optional list of logistics lanes to filter
        brands: Optional list of brands to filter
        
    Returns:
        DataFrame with columns: day, brand, lane, zipcode, forecasted_sessions, projected_lost_conversions
        
    Raises:
        ValueError: If inputs are invalid
        sqlalchemy.exc.SQLAlchemyError: If database query fails
    """
    
    # Validate inputs
    logger.info(f"Validating inputs for date range {start_date} to {end_date}")
    validate_inputs(start_date, end_date, shut_off_tier, fallback_tier, zipcodes, lanes, brands)
    
    # 1. Establish database connection
    logger.info("Creating database engine...")
    try:
        engine = sqlalchemy.create_engine(connection_string, pool_recycle=3600)
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise
    
    try:
        # 2. Build the dynamic SQL query
        logger.info("Building SQL query...")
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
            logger.info(f"Added zipcode filter: {len(zipcodes)} zipcodes")
            
        if lanes:
            query += " AND lane IN :lanes"
            params["lanes"] = tuple(lanes)
            logger.info(f"Added lane filter: {len(lanes)} lanes")
            
        if brands:
            query += " AND brand IN :brands"
            params["brands"] = tuple(brands)
            logger.info(f"Added brand filter: {len(brands)} brands")
        
        # 3. Fetch the data securely
        logger.info("Executing database query...")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)
            
        logger.info(f"Query returned {len(df)} rows")
        
        if df.empty:
            logger.warning("No traffic matches these specific filters for the selected dates.")
            return df
        
        # 4. Apply the Business Logic (Uplifts)
        logger.info(f"Calculating impact: {shut_off_tier} -> {fallback_tier}")
        
        original_uplift = UPLIFT_MAP[shut_off_tier]
        new_uplift = UPLIFT_MAP[fallback_tier]
        
        logger.info(f"Uplift rates - Original: {original_uplift*100}%, New: {new_uplift*100}%")
        
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
        
        result_df = df[output_columns].sort_values(
            by=['day', 'projected_lost_conversions'], 
            ascending=[True, False]
        )
        
        total_impact = result_df['projected_lost_conversions'].sum()
        logger.info(f"Analysis complete. Total projected lost conversions: {total_impact}")
        
        return result_df
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        raise
    finally:
        engine.dispose()
        logger.info("Database connection closed")

# --- USAGE EXAMPLES ---
if __name__ == "__main__":
    # Example A: A highly specific query
    # "What happens if we shut off 2DD on the LAX-to-JFK lane, specifically for Brand X, over the weekend?"
    try:
        specific_impact = fetch_and_calculate_impact(
            connection_string="your_db_string_here",
            start_date="2026-03-21",
            end_date="2026-03-22",
            shut_off_tier="2DD",
            fallback_tier="3DD",
            lanes=["LAX-JFK", "ONT-JFK"],
            brands=["Brand_X"]
        )
        print("Specific Impact Results:")
        print(specific_impact)
    except (ValueError, Exception) as e:
        logger.error(f"Specific query failed: {e}")
    
    # Example B: A broad query
    # "What happens if we shut down 3DD nationwide for the next 7 days across all brands?"
    try:
        broad_impact = fetch_and_calculate_impact(
            connection_string="your_db_string_here",
            start_date="2026-03-17",
            end_date="2026-03-24",
            shut_off_tier="3DD",
            fallback_tier="BOLT"
        )
        print("\nBroad Impact Results:")
        print(broad_impact)
    except (ValueError, Exception) as e:
        logger.error(f"Broad query failed: {e}")

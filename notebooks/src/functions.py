import pandas as pd
from sqlalchemy import create_engine
import pymysql
from typing import Dict, Tuple

def load_and_clean_data(file_path: str) -> pd.DataFrame:
    """Load and perform initial cleaning of the HR data."""
    df = pd.read_csv(file_path)
    
    # Rename columns
    df = df.rename(columns={
        'birthdate': 'birth_date',
        'jobtitle': 'job_title',
        'termdate': 'term_date',
        'id': 'emp_id'
    })
    
    # Remove rows where all values are NaN
    df = df.dropna(how='all')
    
    # Remove rows where all columns except 'job_title' are NaN
    df = df.dropna(how='all', subset=df.columns.difference(['job_title']))
    
    # Drop location_city column
    df.drop(columns=['location_city'], inplace=True)
    
    return df

def apply_mappings(df: pd.DataFrame) -> pd.DataFrame:
    """Apply state and department mappings to the data."""
    state_mapping = {
        'Ohio': 'New York',
        'Illinois': 'New Hampshire',
        'Indiana': 'Connecticut',
        'Michigan': 'Massachusetts',
        'Kentucky': 'New Jersey',
        'Wisconsin': 'Ohio'
    }
    
    department_mapping = {
        'Engineering': 'Sales',
        'Sales': 'Engineering'
    }
    
    df['location_state'] = df['location_state'].replace(state_mapping)
    df['department'] = df['department'].replace(department_mapping)
    
    return df

def process_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Process and validate all date columns."""
    # Convert dates to datetime
    df['birth_date'] = pd.to_datetime(df['birth_date'], errors='coerce')
    df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce')
    df['term_date'] = pd.to_datetime(df['term_date']).dt.date
    df['term_date'] = pd.to_datetime(df['term_date'])
    
    # Remove future termination dates
    cutoff_date = pd.Timestamp('2020-12-31')
    df = df.drop(df[df['term_date'] > cutoff_date].index)
    
    # Shift dates forward by 3 years
    three_years = pd.DateOffset(years=3)
    df['birth_date'] = df['birth_date'] + three_years
    df['hire_date'] = df['hire_date'] + three_years
    df['term_date'] = df['term_date'] + three_years
    
    return df

def calculate_age(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate age based on birth_date and add it to the DataFrame."""
    current_year = pd.Timestamp('now').year
    age = current_year - df['birth_date'].dt.year
    df.insert(4, 'age', age)
    return df

def create_db_connection(database: str, password: str) -> create_engine:
    """Create a database connection."""
    connection_string = f'mysql+pymysql://root:{password}@localhost/{database}'
    return create_engine(connection_string)

def get_gender_breakdown(engine) -> pd.DataFrame:
    """Get gender distribution of employees."""
    query = """
    SELECT gender, COUNT(*) AS count
    FROM employees
    GROUP BY gender;
    """
    return pd.read_sql(query, engine)

def get_race_breakdown(engine) -> pd.DataFrame:
    """Get race/ethnicity distribution of employees."""
    query = """
    SELECT race, COUNT(*) AS count
    FROM employees
    GROUP BY race
    ORDER BY count DESC;
    """
    return pd.read_sql(query, engine)

def get_age_distribution(engine) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get age distribution of employees."""
    query_age_range = """
    SELECT MAX(age) AS max_age, MIN(age) AS min_age
    FROM employees;
    """
    
    query_age_distribution = """
    SELECT 
      CASE 
        WHEN age < 30 THEN '20-29'
        WHEN age < 40 THEN '30-39'
        WHEN age < 50 THEN '40-49'
        ELSE '50-59'
      END AS age_group, COUNT(*) AS count
    FROM employees
    GROUP BY age_group
    ORDER BY count DESC;
    """
    
    age_range = pd.read_sql(query_age_range, engine)
    age_distribution = pd.read_sql(query_age_distribution, engine)
    
    return age_range, age_distribution

def get_turnover_rates(engine) -> pd.DataFrame:
    """Calculate turnover rates per year."""
    query = """
    WITH year_cte AS (
        SELECT YEAR(term_date) AS year,
               COUNT(*) AS termination_count,
               (SELECT COUNT(*)
                FROM employees
                WHERE hire_date <= DATE(CONCAT(YEAR(term_date), '-12-31')) 
                  AND (term_date IS NULL OR term_date >= DATE(CONCAT(YEAR(term_date), '-01-01')))
               ) AS total_count
        FROM employees
        WHERE term_date IS NOT NULL
        GROUP BY YEAR(term_date)
    )
    SELECT year, 
           ROUND((termination_count / total_count) * 100, 1) AS turnover_rate
    FROM year_cte
    ORDER BY year ASC;
    """
    return pd.read_sql(query, engine)

def export_to_csv(df: pd.DataFrame, file_path: str) -> None:
    """Export DataFrame to CSV file."""
    df.to_csv(file_path, index=False)

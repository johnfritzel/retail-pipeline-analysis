import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
import sys
import kagglehub
from sqlalchemy.exc import OperationalError, ProgrammingError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_environment() -> dict:
    """Load and validate environment variables."""
    load_dotenv()
    
    required_vars = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_PORT": os.getenv("DB_PORT"),
        "INPUT_FILE": os.getenv("INPUT_FILE")
    }
    
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
        
    return required_vars

def create_db_engine(env_vars: dict):
    """Create SQLAlchemy database engine."""
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME', 'DB_PORT']
    missing_vars = [var for var in required_vars if var not in env_vars]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    try:
        connection_string = (
            f"mysql+mysqlconnector://{env_vars['DB_USER']}:{env_vars['DB_PASSWORD']}"
            f"@{env_vars['DB_HOST']}:{env_vars['DB_PORT']}/{env_vars['DB_NAME']}"
        )
        logger.debug(f"Attempting to create database engine with connection string: {connection_string}")

        engine = create_engine(connection_string)

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successfully established.")
        return engine

    except OperationalError:
        logger.error("Operational error during database connection. Check credentials and host.")
        raise

    except ProgrammingError:
        logger.error("Programming error during database connection. Verify the database name and query syntax.")
        raise

    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
        raise

def load_and_clean_data(file_path: str) -> pd.DataFrame:
    """Load data from CSV and perform basic cleaning operations."""
    try:
        df = pd.read_csv(file_path)
        
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        try:
            df['date'] = pd.to_datetime(df['date'], format="%d-%m-%Y")
        except ValueError:
            logger.warning("Date format does not match. Trying mixed formats.")
            df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        null_dates = df['date'].isnull().sum()
        if null_dates > 0:
            logger.warning(f"Removing {null_dates} rows with invalid dates")
            df = df.dropna(subset=['date'])
        
        return df
    except Exception as e:
        logger.error(f"Error loading or cleaning data: {str(e)}")
        raise

def validate_data(df: pd.DataFrame) -> None:
    """Perform data validation checks."""
    try:
        required_columns = ['store_number', 'date', 'weekly_sales']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            raise ValueError("Date column is not in datetime format")
        
        if not df['store_number'].dtype.kind in 'ui':
            raise ValueError("Store numbers must be integers")
        if (df['store_number'] <= 0).any():
            raise ValueError("Store numbers must be positive")
        
        negative_sales = df[df['weekly_sales'] < 0]
        if not negative_sales.empty:
            logger.warning(f"Found {len(negative_sales)} negative sales values")
            
        future_dates = df[df['date'] > datetime.now()]
        if not future_dates.empty:
            logger.warning(f"Found {len(future_dates)} future dates in the dataset")
            
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise

def save_to_database(df: pd.DataFrame, engine, table_name: str = 'best_buy_sales') -> None:
    """Save DataFrame to MySQL database with error handling."""
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS retail_db_db;"))
            conn.commit()
        
        df['loaded_at'] = datetime.now()
        df['source_file'] = os.getenv('INPUT_FILE')
        
        df.to_sql(
            name=table_name,
            schema='retail_db',
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=1000
        )
        
        inspector = inspect(engine)
        if not inspector.has_table(table_name, schema='retail_db'):
            raise Exception("Table was not created successfully")
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM retail_db.{table_name}"))
            row_count = result.scalar()
            logger.info(f"Successfully loaded {row_count} rows into {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to save data to database: {str(e)}")
        raise

def main():
    """Main function to orchestrate the data pipeline."""
    engine = None
    try:
        logger.info("Starting data pipeline")
        
        env_vars = load_environment()
        logger.info("Environment variables loaded successfully")
        
        path = kagglehub.dataset_download("staykoks/best-buy-sales")
        logger.info(f"Dataset downloaded to {path}")
        
        engine = create_db_engine(env_vars)
        logger.info("Database connection established")
        
        df = load_and_clean_data(env_vars['INPUT_FILE'])
        logger.info(f"Loaded {len(df)} rows from input file")
        
        validate_data(df)
        logger.info("Data validation passed")
        
        save_to_database(df, engine)
        logger.info("Data pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        if engine is not None:
            engine.dispose()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()

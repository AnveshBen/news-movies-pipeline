import logging
from pyspark.sql import DataFrame
from typing import Dict, List, Optional

class DataValidator:
    @staticmethod
    def validate_dataframe(
        df: DataFrame,
        name: str,
        required_columns: List[str],
        min_rows: int = 1,
        unique_columns: Optional[List[str]] = None
    ) -> bool:
        logger = logging.getLogger(__name__)
        
        try:
            # Check if DataFrame is empty
            row_count = df.count()
            if row_count < min_rows:
                raise ValueError(f"{name} has insufficient data: {row_count} rows")
            
            # Check for required columns
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"{name} is missing columns: {missing_cols}")
            
            # Check for uniqueness constraints
            if unique_columns:
                for col in unique_columns:
                    total = df.count()
                    distinct = df.select(col).distinct().count()
                    if distinct < total:
                        logger.warning(
                            f"Column {col} in {name} has duplicates: "
                            f"{total-distinct} duplicate values"
                        )
            
            # Basic statistics
            logger.info(f"\nValidation Results for {name}:")
            logger.info(f"Total rows: {row_count}")
            logger.info("Column statistics:")
            
            for col in df.columns:
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    logger.warning(f"  {col}: {null_count} null values")
            
            return True
            
        except Exception as e:
            logger.error(f"Validation failed for {name}: {str(e)}")
            raise 
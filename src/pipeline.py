from extract import extract_data
from transform import transform_data
from load import insert_countries, save_raw, save_processed
from utils.logger import get_logger

logger = get_logger()

logger.info('--- Starting Pipeline ---')
def pipeline():
    try:
        logger.info('Starting extraction')
        data = extract_data()

        logger.info('Saving raw data')
        save_raw(data)

        logger.info('Transforming data')
        df = transform_data(data)

        logger.info('Saving transformed data')
        save_processed(df)
        
        logger.info('Loading data to the database')
        insert_countries(df)

        logger.info('Pipeline completed successfully')
    except Exception as e:
        logger.error(f'Pipeline error: {e}')
        raise

if __name__ == '__main__':
    pipeline()

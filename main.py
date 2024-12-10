from flow import air_disaster_flow
import loguru
from datetime import datetime

logger = loguru.logger

def main(max_disasters: int):
    try:
        logger.info("Starting air disasters analysis pipeline")
        start_time = datetime.now()

        results = air_disaster_flow(max_disasters=max_disasters)

        execution_time = datetime.now() - start_time
        logger.info(f"Pipeline completed successfully in {execution_time}")
        logger.info("Summary of results:")
        logger.info(f"Total disasters processed: {len(results)}")

        return results

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        raise



if __name__ == "__main__":
    main(max_disasters=200)
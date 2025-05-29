import argparse
import logging
from scheduler.executor import SchedulerExecutor
from config.settings import load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Database Validation Tool')
    parser.add_argument('--config', type=str, default='config/settings.yaml',
                      help='Path to configuration file (default: config/settings.yaml)')
    parser.add_argument('--run', action='store_true',
                      help='Run the validation scheduler')
    
    args = parser.parse_args()
    
    try:
        config = load_config(args.config)
        
        if args.run:
            logger.info("Starting validation scheduler...")
            scheduler = SchedulerExecutor(config)
            scheduler.run()
        else:
            logger.info("No action specified. Use --run to start the validation scheduler.")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 
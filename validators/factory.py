from typing import Dict, Any, List
from sqlalchemy import Engine
import logging

from validators.base import BaseValidator
from validators.row_count import RowCountValidator
from validators.hash_compare import HashValidator
from validators.sample_compare import SampleValidator

logger = logging.getLogger(__name__)

class ValidatorFactory:
    @staticmethod
    def create_validators(
        source_engine: Engine,
        target_engine: Engine,
        config: Dict[str, Any]
    ) -> List[BaseValidator]:
        """
        Create validators based on configuration.
        
        Args:
            source_engine (Engine): Source database engine
            target_engine (Engine): Target database engine
            config (Dict[str, Any]): Configuration dictionary
            
        Returns:
            List[BaseValidator]: List of validators to run
        """
        validators = []
        validation_types = config['validation']['types']
        
        for vtype in validation_types:
            try:
                if vtype == 'row_count':
                    validators.append(RowCountValidator(source_engine, target_engine, config))
                elif vtype == 'hash_check':
                    validators.append(HashValidator(source_engine, target_engine, config))
                elif vtype == 'sample_comparison':
                    validators.append(SampleValidator(source_engine, target_engine, config))
                else:
                    logger.warning(f"Unknown validation type: {vtype}")
            except Exception as e:
                logger.error(f"Error creating validator for type {vtype}: {str(e)}")
        
        return validators 
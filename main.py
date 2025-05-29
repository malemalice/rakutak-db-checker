import yaml
import logging
from loguru import logger
import sys
from pathlib import Path
from typing import Dict, Any

from connectors.factory import create_db_engine
from validators.factory import ValidatorFactory

def load_config() -> Dict[str, Any]:
    """
    Load configuration from settings.yaml
    """
    config_path = Path(__file__).parent / "config" / "settings.yaml"
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        sys.exit(1)

def setup_logging(config: Dict[str, Any]) -> None:
    """
    Setup logging configuration
    """
    log_config = config['logging']
    logger.remove()  # Remove default handler
    
    # Add file handler
    logger.add(
        log_config['file'],
        rotation=log_config['max_size'],
        retention=log_config['backup_count'],
        format=log_config['format'],
        level=log_config['level']
    )
    
    # Add console handler
    logger.add(
        sys.stderr,
        format=log_config['format'],
        level=log_config['level']
    )

def run_validation(source_engine, target_engine, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the validation process using configured validators.
    
    Args:
        source_engine: Source database engine
        target_engine: Target database engine
        config (Dict[str, Any]): Configuration dictionary
        
    Returns:
        Dict[str, Any]: Validation results
    """
    try:
        # Log ignored columns configuration
        ignored_columns = config['validation'].get('ignored_columns', [])
        if ignored_columns:
            logger.info(f"Hash validation will ignore these columns: {ignored_columns}")
        else:
            logger.info("No columns configured to be ignored during hash validation")
        
        # Create validators
        validators = ValidatorFactory.create_validators(source_engine, target_engine, config)
        
        if not validators:
            return {
                "status": "error",
                "error": "No validators configured"
            }
        
        # Run all validators
        results = {}
        all_summary = {
            "total_tables": 0,
            "passed_tables": [],
            "failed_tables": [],
            "error_tables": []
        }
        
        for validator in validators:
            validator_name = validator.__class__.__name__
            logger.info(f"Running {validator_name}")
            validator_results = validator.validate_all()
            results[validator_name] = validator_results
            
            # Generate summary for this validator
            summary = _generate_validator_summary(validator_name, validator_results)
            
            # Update overall summary
            if all_summary["total_tables"] == 0:  # First validator sets the baseline
                all_summary["total_tables"] = summary["total_tables"]
            
            # Track tables by their worst status across all validators
            for table in summary["passed_tables"]:
                if table not in all_summary["failed_tables"] and table not in all_summary["error_tables"]:
                    if table not in all_summary["passed_tables"]:
                        all_summary["passed_tables"].append(table)
            
            for table in summary["failed_tables"]:
                if table not in all_summary["error_tables"]:
                    if table in all_summary["passed_tables"]:
                        all_summary["passed_tables"].remove(table)
                    if table not in all_summary["failed_tables"]:
                        all_summary["failed_tables"].append(table)
            
            for table in summary["error_tables"]:
                if table in all_summary["passed_tables"]:
                    all_summary["passed_tables"].remove(table)
                if table in all_summary["failed_tables"]:
                    all_summary["failed_tables"].remove(table)
                if table not in all_summary["error_tables"]:
                    all_summary["error_tables"].append(table)
        
        # Print overall summary
        _print_overall_summary(all_summary)
        
        return {
            "status": "success",
            "results": results,
            "summary": all_summary
        }
        
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

def _generate_validator_summary(validator_name: str, validator_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate summary statistics for a single validator.
    
    Args:
        validator_name (str): Name of the validator
        validator_results (Dict[str, Any]): Validation results for all tables
        
    Returns:
        Dict[str, Any]: Summary statistics
    """
    passed_tables = []
    failed_tables = []
    error_tables = []
    
    for table_name, result in validator_results.items():
        if result.get("status") == "success":
            passed_tables.append(table_name)
        elif result.get("status") == "mismatch":
            failed_tables.append(table_name)
        elif result.get("status") == "error":
            error_tables.append(table_name)
    
    total_tables = len(validator_results)
    
    # Print validator-specific summary with clean formatting
    print(f"\n{'='*60}")
    print(f"VALIDATION SUMMARY - {validator_name}")
    print(f"{'='*60}")
    print(f"Total tables validated: {total_tables}")
    print(f"✅ Passed: {len(passed_tables)} tables")
    print(f"❌ Failed: {len(failed_tables)} tables")
    print(f"⚠️  Errors: {len(error_tables)} tables")
    
    if passed_tables:
        print(f"\n✅ PASSED TABLES:")
        for table in sorted(passed_tables):
            print(f"   • {table}")
    
    if failed_tables:
        print(f"\n❌ FAILED TABLES:")
        for table in sorted(failed_tables):
            result = validator_results[table]
            if validator_name == "SampleValidator":
                source_count = result.get("source_count", 0)
                target_count = result.get("target_count", 0)
                print(f"   • {table} (source: {source_count}, target: {target_count})")
            elif validator_name == "RowCountValidator":
                source_count = result.get("source_count", 0)
                target_count = result.get("target_count", 0)
                diff = result.get("difference", 0)
                print(f"   • {table} (source: {source_count}, target: {target_count}, diff: {diff})")
            elif validator_name == "HashValidator":
                if "mismatches" in result:
                    mismatch_count = len(result["mismatches"])
                    ignored_cols = result.get("ignored_columns", [])
                    if ignored_cols:
                        print(f"   • {table} ({mismatch_count} hash mismatches, ignored: {ignored_cols})")
                    else:
                        print(f"   • {table} ({mismatch_count} hash mismatches)")
                else:
                    print(f"   • {table}")
            else:
                print(f"   • {table}")
    
    if error_tables:
        print(f"\n⚠️  ERROR TABLES:")
        for table in sorted(error_tables):
            error_msg = validator_results[table].get("error", "Unknown error")
            print(f"   • {table}: {error_msg}")
    
    print(f"{'='*60}")
    
    return {
        "total_tables": total_tables,
        "passed_tables": passed_tables,
        "failed_tables": failed_tables,
        "error_tables": error_tables
    }

def _print_overall_summary(summary: Dict[str, Any]) -> None:
    """
    Print overall validation summary across all validators.
    
    Args:
        summary (Dict[str, Any]): Overall summary statistics
    """
    total = summary["total_tables"]
    passed = len(summary["passed_tables"])
    failed = len(summary["failed_tables"])
    errors = len(summary["error_tables"])
    
    print(f"\n{'='*60}")
    print(f"OVERALL VALIDATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total tables: {total}")
    print(f"✅ Fully matched: {passed} tables ({passed/total*100:.1f}%)")
    print(f"❌ Mismatched: {failed} tables ({failed/total*100:.1f}%)")
    print(f"⚠️  Errors: {errors} tables ({errors/total*100:.1f}%)")
    
    if summary["passed_tables"]:
        print(f"\n✅ FULLY MATCHED TABLES:")
        for table in sorted(summary["passed_tables"]):
            print(f"   • {table}")
    
    if summary["failed_tables"]:
        print(f"\n❌ TABLES WITH MISMATCHES:")
        for table in sorted(summary["failed_tables"]):
            print(f"   • {table}")
    
    if summary["error_tables"]:
        print(f"\n⚠️  TABLES WITH ERRORS:")
        for table in sorted(summary["error_tables"]):
            print(f"   • {table}")
    
    print(f"{'='*60}")

def main():
    # Load configuration
    config = load_config()
    
    # Setup logging
    setup_logging(config)
    
    try:
        # Create database engines
        source_engine = create_db_engine(config['source_db'])
        target_engine = create_db_engine(config['target_db'])
        
        # Run validation
        results = run_validation(source_engine, target_engine, config)
        logger.info(f"Validation completed with status: {results['status']}")
        
    except Exception as e:
        logger.error(f"Application failed to start: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
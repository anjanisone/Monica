import importlib
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from analyticProcUtil import FDPAnalyzer
import logging

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s| %(message)s')
logger = logging.getLogger(__name__)

# Import utils
cc_utils = "utils.cct_common_utils"
tags_utils = importlib.import_module(cc_utils)

class SchemaEvolveDriver(FDPAnalyzer):

    @tags_utils.logger_error_handler
    def processData(self):
        spark = SparkSession.builder.getOrCreate()

        LOOKBACK_DAYS = 10
        today = datetime.now()
        cutoff = (today - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')

        RULES_VIEW = "anbc-pss-dev.cct_enrv_pss_dev.V_RULES_OPERATIONS_ALLTAGS"
        RULES_TABLE = "anbc-pss-dev.cct_enrv_pss_dev.T_RULES"
        TAGGING_HIST = "T_TAG_CLM_ALLTAGS_HIST"
        TAGGING_DLY = "T_TAG_CLM_ALLTAGS_DLY"

        # Step 1: Find new rules
        rule_query = f"""
            SELECT DISTINCT v.RULE_NM
            FROM `{RULES_VIEW}` v
            JOIN `{RULES_TABLE}` t ON v.rule_id = t.rule_id
            WHERE PARSE_DATE('%Y-%m-%d', t.enabled_on) >= DATE_SUB(CURRENT_DATE(), INTERVAL {LOOKBACK_DAYS} DAY)
        """
        rules_df = spark.sql(rule_query)
        new_rules = [row['RULE_NM'] for row in rules_df.collect()]
        logger.info(f"Found {len(new_rules)} new rule(s): {new_rules}")

        if not new_rules:
            logger.info("No new rules found. Schema update not required.")
            return "no_rules"

        # Step 2: Get existing columns from both tables
        existing_hist_cols = self.get_table_columns(spark, TAGGING_HIST)
        existing_dly_cols = self.get_table_columns(spark, TAGGING_DLY)

        missing_cols = [
            rule for rule in new_rules
            if rule not in existing_hist_cols or rule not in existing_dly_cols
        ]

        if not missing_cols:
            logger.info("No schema evolution needed.")
            return "no_schema_change"

        # Step 3: Alter both tables
        for col in missing_cols:
            safe_col = col.replace("`", "")
            alter_hist = f"ALTER TABLE {TAGGING_HIST} ADD COLUMN `{safe_col}` STRING;"
            alter_dly = f"ALTER TABLE {TAGGING_DLY} ADD COLUMN `{safe_col}` STRING;"
            logger.info(f"Adding column: {safe_col}")
            tags_utils.execute_bq_query(alter_hist)
            tags_utils.execute_bq_query(alter_dly)

        logger.info(f"Schema updated with {len(missing_cols)} new rule columns.")
        return "success"

    def get_table_columns(self, spark, table_name):
        query = f"""
            SELECT column_name
            FROM `anbc-pss-dev.cct_enrv_pss_dev.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """
        df = spark.sql(query)
        return [row['column_name'] for row in df.collect()]

def processing():
    myProcessor = SchemaEvolveDriver(
        sysConfName='cct_env',
        projConfName='cctTags.projConf_rhcct_alltags',
        tenantName='cct',
        env='dev',
        compute_mtdt_identifier='fdl-rh-ae-pss'
    )
    return myProcessor.processData()
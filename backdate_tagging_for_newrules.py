import importlib
from datetime import datetime
from pyspark.sql import SparkSession
from fdpPySparkUtil import fdpPySparkConfig, fdpPySparkOps
from analyticProcUtil import FDPAnalyzer
import logging

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s| %(message)s')
logger = logging.getLogger(__name__)

# Import dynamic utils
cc_utils = "utils.cct_common_utils"
tags_utils = importlib.import_module(cc_utils)

class BackfillTaggingDriver(FDPAnalyzer):

    @tags_utils.logger_error_handler
    def processData(self):
        spark = SparkSession.builder.getOrCreate()

        RULES_VIEW = "anbc-pss-dev.cct_enrv_pss_dev.V_RULES_OPERATIONS_ALLTAGS"
        RULES_TABLE = "anbc-pss-dev.cct_enrv_pss_dev.T_RULES"
        CLAIMS_TABLE = "T_PHMCY_CLM_DLY_ALLTAGS"
        TARGET_TABLE = "T_TAG_CLM_ALLTAGS"
        STAGING_TABLE = "T_TAG_CLM_ALLTAGS_BACKFILL"
        RUNSTATS_TABLE = "runstats_backfill"

        today = datetime.now().strftime('%Y-%m-%d')

        rule_query = f"""
            SELECT DISTINCT v.RULE_ID, t.effective_dt
            FROM `{RULES_VIEW}` v
            JOIN `{RULES_TABLE}` t ON v.rule_id = t.rule_id
            WHERE PARSE_DATE('%Y-%m-%d', t.effective_dt) < DATE('{today}')
        """
        rules_df = spark.sql(rule_query)
        rules = rules_df.collect()

        if not rules:
            logger.info("[INFO] No backdate rules found.")
            return "no_rules"

        logger.info(f"[INFO] Found {len(rules)} backdated rule(s). Running tagging...")

        my_dict = {
            "RULES": "v_rules_operations_alltags",
            "JOINS": "joins_alltags",
            "CLMS": "T_PHMCY_CLM_DLY_ALLTAGS",
            "ALLTAGS": STAGING_TABLE,
            "RUNSTATS": RUNSTATS_TABLE
        }

        logger.info("Calling tagging process...")
        self.tagging_process(my_dict)

        merge_sql = f"""
            MERGE INTO `{TARGET_TABLE}` AS target
            USING `{STAGING_TABLE}` AS source
            ON target.CLAIM_ID = source.CLAIM_ID
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT ROW
        """
        tags_utils.execute_bq_query(merge_sql)
        logger.info(f"[SUCCESS] All backfilled tags merged into {TARGET_TABLE}")

        return "success"

    def tagging_process(self, my_dict):
        logger.info("Calling tagging driver...")
        from all_tags import TaggingDriver
        myProcessor = TaggingDriver(
            sysConfName='cct_env',
            projConfName='cctTags.projConf_rhcct_alltags',
            tenantName='cct',
            env='dev',
            compute_mtdt_identifier='fdl-rh-ae-pss'
        )
        myProcessor.processData(my_dict)

def processing():
    myProcessor = BackfillTaggingDriver(
        sysConfName='cct_env',
        projConfName='cctTags.projConf_rhcct_alltags',
        tenantName='cct',
        env='dev',
        compute_mtdt_identifier='fdl-rh-ae-pss'
    )
    return myProcessor.processData()
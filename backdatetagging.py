import importlib
import re
from datetime import datetime
from pyspark.sql import SparkSession
from analyticProcUtil import FDPAnalyzer
import logging
from functools import reduce
from pyspark.sql.functions import col

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s| %(message)s')
logger = logging.getLogger(__name__)

cc_utils = "utils.cct_common_utils"
tags_utils = importlib.import_module(cc_utils)

class BackfillTaggingDriver(FDPAnalyzer):

    @tags_utils.logger_error_handler
    def processData(self, my_dict):
        spark = SparkSession.builder.getOrCreate()

        today = datetime.now().strftime('%Y-%m-%d')

        rule_query = f"""
            SELECT DISTINCT RULE_ID, RULE_NM, RULE_EXPR, effective_dt
            FROM {my_dict["RULES"]}
            WHERE PARSE_DATE('%Y-%m-%d', effective_dt) < DATE('{today}')
        """
        rules_df = self.bq2df(rule_query, self.sysConf.sourceDB["sourceBigQueryEnrichSchema"])
        rules = rules_df.collect()

        if not rules:
            logger.info("[INFO] No backdate rules found.")
            return "no_rules"

        logger.info(f"[INFO] Found {len(rules)} backdated rule(s)")

        joins_df = self.bq2df(f"""
            SELECT * FROM {my_dict["JOINS"]}
            WHERE ACTIVE_IND = 'Y' AND FEATURE_NAME = 'PRE'
        """, self.sysConf.sourceDB["sourceBigQueryEnrichSchema"])

        for rule in rules:
            rule_id = str(rule["RULE_ID"])
            rule_name = rule["RULE_NM"]
            rule_expr = rule["RULE_EXPR"]
            effective_dt = rule["effective_dt"]
            logger.info(f"Processing rule: {rule_name} (ID: {rule_id})")

            aliases = self.extract_aliases_from_expr(rule_expr)
            logger.info(f"Aliases used in rule: {aliases}")

            filtered_joins = self.filter_joins(joins_df, rule_id, aliases)

            if filtered_joins.count() == 0:
                logger.warning(f"No applicable joins found for rule: {rule_name}")
                continue

            # Filter relevant claims
            claim_query = f"""
                SELECT DISTINCT CLAIM_ID
                FROM {my_dict["CLMS"]}
                WHERE FILL_DATE >= DATE('{effective_dt}')
            """
            claims_df = self.bq2df(claim_query, self.sysConf.sourceDB["sourceBigQueryEnrichSchema"])
            claim_ids = [row["CLAIM_ID"] for row in claims_df.collect()]

            if not claim_ids:
                logger.info(f"No claims found for rule: {rule_name}")
                continue

            logger.info(f"Found {len(claim_ids)} claims for rule: {rule_name}")

            # Replace RULES and JOINS with filtered subsets
            custom_dict = my_dict.copy()
            custom_dict["RULES"] = f"(SELECT * FROM {my_dict['RULES']} WHERE RULE_ID = '{rule_id}')"
            custom_dict["JOINS"] = f"({self.df_to_temp_table(filtered_joins)})"

            self.tagging_process(custom_dict, claim_ids)

        # Merge staging to main
        merge_sql = f"""
            MERGE INTO {my_dict["ALLTAGS_MERGE_TARGET"]} AS target
            USING {my_dict["ALLTAGS"]} AS source
            ON target.CLAIM_ID = source.CLAIM_ID
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT ROW
        """
        tags_utils.execute_bq_query(merge_sql)
        logger.info(f"[SUCCESS] All backfilled tags merged into {my_dict['ALLTAGS_MERGE_TARGET']}")

        return "success"

    def extract_aliases_from_expr(self, rule_expr):
        matches = re.findall(r'\b([A-Z_]+)\.', rule_expr)
        return list(set(matches))

    def filter_joins(self, joins_df, rule_id, aliases):
        contains_rule = joins_df["RULES"].contains(rule_id)
        alias_filters = [joins_df["JOIN_STATEMENT"].contains(alias) for alias in aliases]
        combined_alias_filter = reduce(lambda a, b: a | b, alias_filters) if alias_filters else None
        if combined_alias_filter:
            return joins_df.filter(contains_rule & combined_alias_filter)
        else:
            return joins_df.filter(contains_rule)

    def df_to_temp_table(self, df):
        temp_name = f"temp_joins_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        df.createOrReplaceTempView(temp_name)
        return temp_name

    def tagging_process(self, my_dict, claim_ids):
        from all_tags import TaggingDriver
        processor = TaggingDriver(
            sysConfName=self.sysConfName,
            projConfName=self.projConfName,
            tenantName=self.tenantName,
            env=self.env,
            compute_mtdt_identifier=self.compute_mtdt_identifier
        )
        processor.processData(my_dict)

def processing(sysConfName='cct_env', projConfName='cctTags.projConf_rhcct_alltags', tenantName='cct', env='dev', compute_mtdt_identifier='fdl-rh-ae-pss'):
    myProcessor = BackfillTaggingDriver(
        sysConfName=sysConfName,
        projConfName=projConfName,
        tenantName=tenantName,
        env=env,
        compute_mtdt_identifier=compute_mtdt_identifier
    )
    myProcessor.processing()

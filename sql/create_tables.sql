CREATE OR REPLACE TABLE fda_recalls_2023 (
    recall_number STRING,
    reason_for_recall STRING,
    status STRING,
    distribution_pattern STRING,
    product_type STRING,
    state STRING,
    product_description STRING,
    report_date DATE,
    recall_initiation_date DATE,
    classification STRING
);
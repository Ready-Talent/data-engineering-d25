import great_expectations as ge
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

credentials = service_account.Credentials.from_service_account_file(
    "C:/Users/Omar/Downloads/ready-de-25-0b7d75dd1f68.json"
)


def run_ge_check(validation_query: str):
    client = bigquery.Client(credentials=credentials)
    query_job = client.query(validation_query)
    results = query_job.result()

    df = results.to_dataframe()
    ge_df = ge.from_pandas(df)

    expectations_config = {
        "expectation_suite_name": "name",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 10000,
                    "max_value": 1000000,
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "order_id",
                },
            },
        ],
    }

    results = ge_df.validate(expectations_config)

    print(results["success"])

    if not results["success"]:
        failed_expectations = []
        for result in results["results"]:
            if not result["success"]:
                failed_expectations.append(
                    {
                        "expectation_type": result["expectation_config"][
                            "expectation_type"
                        ],
                        "kwargs": result["expectation_config"]["kwargs"],
                        "result": result["result"],
                    }
                )

        # Log the failed expectations
        for failure in failed_expectations:
            logging.error(
                "Validation failed for expectation: %s with parameters: %s. Result: %s",
                failure["expectation_type"],
                failure["kwargs"],
                failure["result"],
            )


sql = """SELECT * FROM `ready-de-25.ecommerce.orders`"""

run_ge_check(sql)

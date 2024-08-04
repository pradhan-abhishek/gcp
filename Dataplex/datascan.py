from google.cloud import dataplex_v1

# TODO:
# PROJECT_ID = "<PROJECT_ID"
# LOCATION = "<LOCATION>"
# PARENT_VALUE = f"projects/{PROJECT_ID}/locations/{LOCATION}"

# Dataplex Client object
dataplex_client = dataplex_v1.DataScanServiceClient()


def list_data_scans():
    """
    This function fetches all the list of Auto Data Quality jobs present in the current Project
    :return: Returns all the details regarding the Auto Data Quality Jobs
    """
    list_request_object = dataplex_v1.ListDataScansRequest(
        # parent = PARENT_VALUE
    )
    page_result = dataplex_client.list_data_scans(request=list_request_object)

    response_list = []
    for response in page_result:
        response_list.append(response)

    return response_list


def create_data_scan():
    """
    This function is responsible to create new Auto Data Quality Scan jobs
    """
    datascan_obj = dataplex_v1.DataScan()

    datascan_obj.data_quality_spec.post_scan_actions.bigquery_export = {
        # TODO:
        # PROJECT_ID = Project Id in which You want to store the scan results
        # DATASET_NAME = BigQuery dataset in which You want to store the scan results
        # SCAN_RESULT_TABLE_NAME = BigQuery table in which You want to store the scan results

        # "results_table" : f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{DATASET_NAME}/tables/{SCAN_RESULT_TABLE_NAME}"
    }

    # TODO:
    # PROJECT_NUMBER = Project Number for which You want to perform the scan
    # DATASET_NAME = BigQuery dataset in which You want to perform the scan
    # SCAN_RESULT_TABLE_NAME = BigQuery table for which You want to perform the scan
    datascan_obj.data.resource = "//bigquery.googleapis.com/projects/{PROJECT_NUMBER}/datasets/{DATASET_NAME}/tables/{SCAN_RESULT_TABLE_NAME}"

    non_null_rule = {
        "column": "<COLUMN_NAME>",
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "non_null_exceptation": {}
    }

    sql_row_condition_rule = {
        "column": "<COLUMN_NAME>",
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "threshold": "<RANGES FROM 0 - 1>",
        "row_condition_exceptation": {
            "sql_expression": "<SQL Expression as per data scan rule>"
        }
    }

    uniquness_rule = {
        "column": "<COLUMN_NAME>",
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "uniqueness_expectation": {}
    }

    regex_rule = {
        "column": "<COLUMN_NAME>",
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "threshold": "<RANGES FROM 0 - 1>",
        "ignoreNull": "true",
        "regex_expectation": {
            "regex": '^[0-9]{8}[a-zA-Z]{16}$'
        }
    }

    set_rule = {
        "column": "<COLUMN_NAME>",
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "threshold": "<RANGES FROM 0 - 1>",
        "ignoreNull": "true",
        "set_expectation ": {
            "values": {"value1", "value2"}
        }
    }

    range_rule = {
        "column": "<COLUMN_NAME>",
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "range_expectation": {
            "min_value": "0",
            "max_value": "100"
        }
    }

    table_condition_rule = {
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "table_condition_expectation": {
            "sql_expression": "SQL Expression on table level in respect to Data Scan"
        }
    }

    sql_assertion_rule = {
        "dimension": "<DIMENSION>",
        "name": "<RULE_NAME>",
        "description": "<RULE_DESCRIPTION>",
        "sql_assertion": {
            "sql_statement ": "Standard SQL statement"
        }
    }

    datascan_obj.data_quality_spec.rules.append(non_null_rule)
    datascan_obj.data_quality_spec.rules.append(sql_row_condition_rule)
    datascan_obj.data_quality_spec.rules.append(uniquness_rule)
    datascan_obj.data_quality_spec.rules.append(regex_rule)
    datascan_obj.data_quality_spec.rules.append(set_rule)
    datascan_obj.data_quality_spec.rules.append(range_rule)
    datascan_obj.data_quality_spec.rules.append(table_condition_rule)
    datascan_obj.data_quality_spec.rules.append(sql_assertion_rule)

    request_obj = dataplex_v1.CreateDataScanRequest(
        # parent = PARENT_VALUE,
        data_scan=datascan_obj,
        data_scan_id="<NAME OF THE DATASACAN JOB>"
    )

    operation = dataplex_client.create_data_scan(request=request_obj)
    # Handle the response
    print("Waiting for the operation to complete...")
    response = operation.result()
    print(response)


def trigger_data_scan_job():
    """
    This function is used to trigger the Data scan jobs
    """
    request_obj = dataplex_v1.RunDataScanRequest(
        # TODO:
        # PROJECT_ID = Project ID where the Datascan job exists
        # LOCATION = Location where the Datascan job exists
        # DATASCAN_NAME = Name of the Datascan job
        # name=f"projects/{PROJECT_ID}/locations/{LOCATION}/dataScans/{DATASCAN_NAME}"
    )

    response = dataplex_client.run_data_scan(request=request_obj)


def check_datascan_job_status():
    # TODO:
    # PROJECT_NUMBER = Project Number
    # LOCATION = Location
    # DATASCAN_NAME = Datascan job name
    # TRIGGERED_JOB_ID = Triggered job Id
    datascan_job_name = "projects/{PROJECT_NUMBER}/locations/{LOCATION}/dataScans/{DATASCAN_NAME}/jobs/{TRIGGERED_JOB_ID}"

    request_obj = dataplex_v1.GetDataScanJobRequest(
        name=datascan_job_name
    )

    response = dataplex_client.get_data_scan_job(request=request_obj)
    # Handle the response
    print(response)


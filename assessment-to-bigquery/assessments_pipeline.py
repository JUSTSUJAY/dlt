# flake8: noqa
import humanize
from typing import Any
import os

import dlt
from dlt.common import pendulum
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database, sql_table, Table

from sqlalchemy.sql.sqltypes import TypeEngine
import sqlalchemy as sa


def load_select_tables_from_database() -> None:
    """Use the sql_database source to reflect an entire database schema and load select tables from it.

    """
    # Create a pipeline
    pipeline = dlt.pipeline(pipeline_name="assessments", destination='bigquery', dataset_name="raw_assessments_dataset")

    # Configure the source to load a few select tables incrementally
    source_1 = sql_database().with_resources("submission_report", "submission","assignment", "assessment", "attempt", "attempt_report")

    # Add incremental config to the resources. "updated_at" is a timestamp column in these tables that gets used as a cursor
    source_1.submission_report.apply_hints(incremental=dlt.sources.incremental("latest_updated_at"))
    source_1.submission.apply_hints(incremental=dlt.sources.incremental("latest_updated_at"))
    source_1.assignment.apply_hints(incremental=dlt.sources.incremental("latest_updated_at"))
    source_1.assessment.apply_hints(incremental=dlt.sources.incremental("latest_updated_at"))
    source_1.attempt.apply_hints(incremental=dlt.sources.incremental("latest_updated_at"))
    source_1.attempt_report.apply_hints(incremental=dlt.sources.incremental("latest_updated_at"))

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(source_1, write_disposition="merge")
    print(info)


if __name__ == "__main__":
    # Load selected tables with different settings
    load_select_tables_from_database()
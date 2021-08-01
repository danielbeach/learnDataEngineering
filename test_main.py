import pytest
import main
from pyspark.sql import Row


@pytest.mark.usefixtures("spark_session")
def test_read_college_date(spark_session):
    test_df = main.read_college_date(spark_session)
    assert test_df


@pytest.mark.usefixtures("spark_session")
def test_add_source_file(spark_session):
    test_df = main.read_college_date(spark_session)
    test_df = main.add_source_file(test_df)
    assert 'source_file' in test_df.columns


@pytest.mark.usefixtures("spark_session")
def test_select_and_filter(spark_session):
    test_df = spark_session.createDataFrame(
        [('10000', 'IA', 'test.file'), ('NULL', 'IA', 'test.file')],
        ['NPT4_PUB', 'STABBR', 'source_file']
    )
    test_df = main.select_and_filter(test_df)
    assert test_df.count() == 1


@pytest.mark.usefixtures("spark_session")
def test_pull_year_from_file_name(spark_session):
    test_df = spark_session.createDataFrame(
        [('10000', 'IA', 'MERGED2019_')],
        ['NPT4_PUB', 'STABBR', 'source_file']
    )

    test_df = main.pull_year_from_file_name(test_df)
    assert test_df.select('year').collect() == [Row(year='2019')]





import io
import pandas as pd
import airbyte as ab
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_salesloft(*args, **kwargs):
    """
    Fetches data from the SalesLoft API using the PyAirbyte connector in Mage.ai.
    Returns a dictionary of DataFrames for each available stream.
    """

    # Step 1: Create and install the SalesLoft source
    source = ab.get_source("source-salesloft", install_if_missing=True)

    # Step 2: Configure the SalesLoft source
    source.set_config(
        config={
            "api_key": "v2_ak_7266_04300949564714b24fe9b5fecc5a5ab37c47666d231767539f96b2f4c67721b7",
            "start_date": "2025-02-09T00:00:00Z"  # Format: YYYY-MM-DDT00:00:00Z
        }
    )

    # Step 3: Verify the configuration and credentials
    source.check()
    print("✅ SalesLoft connection check passed!")

    # Step 4: Select all available streams from SalesLoft
    source.select_all_streams()

    # Step 5: Read data from SalesLoft
    read_result = source.read()

    # Step 6: Convert the data into Pandas DataFrames
    dataframes = {stream: read_result[stream].to_pandas() for stream in read_result.keys()}

    print(f"🔍 Available streams: {list(dataframes.keys())}")

    return dataframes  # Returns a dictionary of DataFrames


@test
def test_output(output, *args) -> None:
    """
    Basic test to check if output contains data.
    """
    assert output is not None, "The output is undefined"
    assert isinstance(output, dict), "Output should be a dictionary of DataFrames"
    assert len(output) > 0, "No data streams retrieved from SalesLoft"
    for key, df in output.items():
        assert isinstance(df, pd.DataFrame), f"Stream {key} is not a DataFrame"
        assert not df.empty, f"DataFrame {key} is empty"
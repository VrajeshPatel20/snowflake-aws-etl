from src.snowflake_azure_etl_tools.utils.base.base_operator import BaseOperator


class PolygonOperator(BaseOperator):
    """
    Operator for interacting with Polygon API.
    This class provides functionality to execute operations related to Polygon.
    """

    def __init__(self, **kwargs):
        """
        Initialize the PolygonOperator with any necessary parameters.
        """
        self.api_key = kwargs
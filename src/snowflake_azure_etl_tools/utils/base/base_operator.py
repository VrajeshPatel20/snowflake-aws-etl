from abc import ABC, abstractmethod
from src.snowflake_azure_etl_tools.utils.base.base import Base

class BaseOperator(Base, ABC):
    """
    Base class for all operators in the ETL pipeline.
    This class should be inherited by all operator classes.
    """

    def __init__(self, **kwargs):
        """
        Initialize the operator with any necessary parameters.
        """
        super().__init__(**kwargs)
        self.params = kwargs

    @abstractmethod
    def execute(self):
        """
        Execute the operator's logic.
        This method should be overridden by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")
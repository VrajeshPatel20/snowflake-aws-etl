from src.snowflake_azure_etl_tools.utils.common.base import Base
from abc import ABC, abstractmethod
class BaseHook(Base, ABC):
    def __init__(self, **kwargs):
        """
        Base class for all hooks.
        This class initializes the hook with the provided keyword arguments.

        Args:
            **kwargs: Arbitrary keyword arguments to set as attributes of the hook.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
    @abstractmethod
    def execute(self):
        """
        Execute the hook's logic.
        This method should be overridden by subclasses to implement specific hook behavior.

        Raises:
            NotImplementedError: If the method is not overridden in a subclass.
        """
        raise NotImplementedError("Subclasses must implement this method.")
from etl_factory.config.config_loader import config
from abc import ABC, abstractmethod

class Base:
    def __init__(self, **kwargs):
        # super().__init__()
        for key, value in kwargs.items():
            setattr(self, key, value)

    @staticmethod
    def get_config(key, section, fallback=None):
        return config.get_value(section=section, key=key, fallback=fallback)


class BaseHook(Base, ABC):
    def __init__(self, **kwargs):
        """
        Base class for all hooks.
        This class initializes the hook with the provided keyword arguments.

        Args:
            **kwargs: Arbitrary keyword arguments to set as attributes of the hook.
        """
        super().__init__(**kwargs)
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
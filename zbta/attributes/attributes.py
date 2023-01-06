from abc import abstractclassmethod, ABC
from zbta.btanalyzer.btanalyzer import BTAnalyzer
import logging

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


class TriggerAbstract(ABC):

    def __init__(
        self,
        attribute_id: str,
        description: str
    ) -> None:
        self._attribute_id = attribute_id
        self._description = description

    @property
    def description(self) -> str:
        """the description of the trigger

        Returns:
            str: the description of the trigger
        """
        return self._description

    @property
    def attribute_id(self) -> int:
        """Returns the attribute id of the trigger

        Raises:
            NotImplementedError: in case this method is called directly from the abstract class

        Returns:
            int: the attribute_id
        """
        return self._attribute_id

    @abstractclassmethod
    def trigger_trigger(self, btanalyzer: BTAnalyzer) -> bool:
        """Must implement the logic to trigger the trigger i.e. 
        by convention returns True if the trigger is triggered.

        Returns:
            bool: wether or not the trigger is triggered.
        """
        raise NotImplementedError("This is an abstract method.")


class NbMinInflows(TriggerAbstract):
    """The Nb of Inflows is smaller than N
    """

    def __init__(self,
                 attribute_id: str="zbta_counters_001",
                 description: str="Triggers if the number of Inflow transactions is smaller than N",
                 thresh: int=60
                 ) -> None:
        super().__init__(attribute_id, description)
        self._thresh = thresh
    
    def trigger_trigger(self, btanalyzer: BTAnalyzer) -> bool:
        return 1.0 > 0.0


class Triggers:

    def __init__(
        self,
        triggers: list = [TriggerAbstract]
    ) -> None:
        self._triggers = triggers

    @property
    def nb_triggers(self) -> int:
        """the number of triggers

        Returns:
            int: the number of triggers.
        """
        return len(self._triggers)

    def add_trigger(self, trigger: TriggerAbstract) -> None:
        """Add a trigger to the list of triggers

        Args:
            trigger (TriggerAbstract): the trigger to add.
        """
        if not isinstance(trigger, TriggerAbstract):
            msg = f"expecting a trigger got {type(trigger)} instead."
            logger.error(msg)
            raise TypeError(msg)
        self._triggers.append(trigger)


if __name__ == "__main__":

    triggers = Triggers()
    triggers.add_trigger(
        NbMinInflows(
            thresh=65
        )
    )
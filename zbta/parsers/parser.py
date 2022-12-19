from typing import Dict
from zbta.parsers import __VALID_PARSERS__, __MAPPING_VALID_PARSERS__
from zbta.parsers.fiserv import ReportFiserv
import logging
from zbta.core.common import APIError
import importlib

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)

class Parser:
    """Provides a single interface to parser any report.
    """
    
    def __init__(self, payload: Dict) -> None:
        self._payload = payload
        self._acct_ins = None
        self._rep_ins = None
        self._report = None
        self._data_provider = self._payload["meta"].get("data_provider", "fiserv_alldata").lower()

    def _get_classes_instances(self) -> None:
        """Constructs the instance classes and report classes.

        Raises
        ------
        APIError
            if `data_provider` is invalid or not recognized.
        """
        if self._data_provider not in __VALID_PARSERS__:
            msg = f"Unknown parser `{self._data_provider}`"
            logger.error(msg)
            raise APIError(msg)
        mod, acct_cls, rep_cls = __MAPPING_VALID_PARSERS__[self._data_provider]
        # 1.1 import the module and account class
        mod = importlib.import_module(f"zbta.parsers.{mod}")
        self._rep_ins = getattr(mod, rep_cls)
        self._acct_ins = getattr(mod, acct_cls)

    def parse(self) -> None:
        """Parses the report provided.
        """
        self._get_classes_instances()
        self._report = self._rep_ins(self._payload, acct_ins=self._acct_ins)

    @property
    def report(self) -> ReportFiserv:
        """Returns the report instance.

        Returns
        -------
        ReportFiserv
            the report instance.
        """
        return self._report

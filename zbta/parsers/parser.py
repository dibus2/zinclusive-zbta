from typing import Dict
from zbta.parsers import __VALID_PARSERS__, __MAPPING_VALID_PARSERS__
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
        self._data_provider = self._payload["request"]["meta"].get("data_provider", "fiserv_alldata").lower()
        self._get_classes_instances()

    def _get_classes_instances(self) -> None
        if self._data_provider not in __VALID_PARSERS__:
            msg = f"Unknown parser `{self._data_provider}`"
            logger.error(msg)
            raise APIError(msg)
        mod, acct_cls, bk_cls = __MAPPING_VALID_PARSERS__[self._data_provider]
        # 1.1 import the module and account class
        acct_ins = importlib.import_module(f"zbta.parsers.{mod}", acct_cls)
        bk_ins = importlib.import_module(f"zbta.parsers.{mod}", bk_cls)
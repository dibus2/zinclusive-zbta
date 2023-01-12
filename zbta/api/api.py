import json
import time
from zbta.attributes.attributes import ZBTAGeneral
import jsonschema
from zbta.core.status import Statuses
from typing import Dict
from zbta.core.schemas import __API_SCHEMA__
from zbta.core.common import validate_schema, APIError
from zbta.parsers.parser import Parser
from zbta.btanalyzer.btanalyzer import BTAnalyzer
from zbta.btanalyzer.assets.categories_general_contained import __DICT_CATEGORIES_GENERAL_CONTAINED__
from zbta.btanalyzer.assets.categories_general_match import __DICT_CATEGORIES_GENERAL_MATCH__
import logging


logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)


class Response:

    def __init__(self, payload, error_code=None, error_message=None) -> None:
        self._payload = payload
        self._error_code = None
        self._error_message = None

    def as_payload(self):
        res = {"response": self._payload}
        if self._error_code is not None:
            res["error_code"] = self._error_code
        if self._error_message is not None:
            res["error_message"] = self._error_message
        return res


class APIConnector:
    """Highest level class. Defines the entry point into the python code.
    1. Defines the jsonschema that the input payload must follow as well.
    2. Constructs the ZEngine object as well and handle the routing of 
    the calculation.
    """

    def __init__(self, payload: Dict) -> None:
        self._payload = payload
        self._response = None
        self._parser = None
        self._btanalyzer = None
        self._engine = None
        logger.info("APIConnector instance created")
        self._validate_payload()

    @property
    def parser(self) -> Parser:
        """Returns the parser object

        Returns
        -------
        Parser
            the parser object
        """
        return self._parser

    @property
    def btanalyzer(self) -> BTAnalyzer:
        """Returns the Analyzer object

        Returns
        -------
        BTAnalyzer
            the analyzer object
        """
        return self._btanalyzer

    @property
    def engine(self) -> ZBTAGeneral:
        """Returns the Engine object

        Returns
        -------
        ZBTAGeneral
            the engine object
        """
        return self._engine

    def _validate_payload(self) -> None:
        logger.debug("validating payload...")
        try:
            self._payload = json.loads(self._payload)
        except Exception as err:
            response = f"Error reading the payload, invalid json: `{err}`"
            logger.error(response)
            self._response = Response(
                {}, error_message=response, error_code=Statuses.HTTP_400_BAD_REQUEST).as_payload()
        is_valid, error_msg = validate_schema(
            self._payload, __API_SCHEMA__, "api_schema"
        )
        if not is_valid:
            logger.error(error_msg)
            self._response = Response(
                {}, error_code=Statuses.HTTP_400_BAD_REQUEST, error_message=error_msg).as_payload()

    def process_payload(self) -> Response:
        """Processed the payload json received.

        Returns
        -------
        Response: the response object
            returns the response object.
        """
        logger.info("processing payload...")
        if self._response is not None:
            return self._response
        # 1. create the parser object
        init = time.time()
        st = time.time()
        self._parser = Parser(self._payload["request"])
        # 2. parse
        self._parser.parse()
        logger.debug("time to parse the report %s", time.time() -st )
        # 3. BT Analyser
        st = time.time()
        self._btanalyzer = BTAnalyzer(
            report=self._parser.report,
            dict_kw_id_match=__DICT_CATEGORIES_GENERAL_MATCH__,
            dict_kw_id_contained=__DICT_CATEGORIES_GENERAL_CONTAINED__,
            limit_kw_id_match=[
                "is_salary",
                "is_benefit",
                "is_fee",
                "is_cash",
            ],
            limit_kw_id_contained=[
                "is_obligation",
                "is_benefit",
                "is_salary",
                "is_fee",
                "is_cash",
                "is_payday",
                "is_consumer_loan"
            ],
            do_nweek_nmonth_id=True,
            do_weekend_id=True,
            do_enforce_priorities=False, 
            do_salary_like=True,
            do_internal_transfers=True
        )
        logger.debug("time to analyzer transactions: %s", time.time() - st)
        # 3. generate triggers
        self._engine = ZBTAGeneral(
            btanalyzer=self._btanalyzer
        )
        st = time.time()
        self._engine.calculate_attributes()
        logger.debug("time to calculate the attributes: %s", time.time() - st)
        logger.debug("time total %s", time.time() - init)
        # 4. generate attributes


if __name__ == "__main__":
    #import cProfile
    import pstats
    import io

    with open("zbta/data/fiserv/415060515_AllData_single_file_nopii.json", "r", encoding="utf8") as hh:
        payl = hh.read()

    #with cProfile.Profile() as pr:
        con = APIConnector(payload=payl)
        con.process_payload()
        con.engine.create_list_attributes_csv("attribute_list.csv")
     #   sortby = pstats.SortKey.CUMULATIVE
      #  s = io.StringIO()
       # ps = pstats.Stats(pr).strip_dirs().sort_stats(sortby).sort_stats(-1)
        #ps.print_stats()

        print("profiling done.")

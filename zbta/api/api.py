import json
import jsonschema
from zbta.core.status import Statuses
from typing import Dict
from zbta.core.schemas import __API_SCHEMA__
from zbta.core.common import validate_schema, APIError
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
        logger.info("APIConnector instance created")
        self._validate_payload()

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
        else:
            # 1. create the parser object
            parser = Parser(self._payload["request"])
            
            # 2. parse 
            # 3. generate triggers
            # 4. generate attributes


if __name__ == "__main__":
    with open("zbta/data/fiserv/415060515_AllData_single_file_nopii.json", "r", encoding="utf8") as hh:
        payl = hh.read()
    con = APIConnector(payload=payl)
    con.process_payload()

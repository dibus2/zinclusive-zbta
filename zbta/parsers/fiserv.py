import logging
from typing import Dict
import jsonschema
from zbta.core.common import validate_schema, APIError
from zbta.core.schemas import __ACCOUNT_SCHEMA__

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


class AccountFiserv:
    """Represents an account found in a FISERV report.
    Note that multiple accounts form a Report object.
    """

    def __init__(self, payload: Dict) -> None:
        self._payload = payload
        if not isinstance(self._payload, dict):
            logger.error("Expecting a dictionary")
            raise TypeError("Expecting a dictionary")
        self._owners_names = []
        self._bank_name = None
        self._creation_date = None
        self._account_number = None
        self._account_number_orig = None
        self._account_name = None
        self._account_type = None
        self._account_subtype = None
        self._routing_number = None
        self._nb_transactions = None
        self._email_addresses = []
        self._phone_numbers = []
        self._addresses = []
        self._cities = []
        self._states = []
        self._streets = []
        self._zipcodes = []
        self._most_recent_transaction_date = None
        self._oldest_transaction_date = None
        self._most_recent_balance_date = None
        self._oldest_balance_date = None
        self._nb_inc_transactions = None
        self._nb_out_transactions = None
        self._nb_overdrafts = None
        self._current_balance = None
        self._transactions = None
        self._transactions_columns_reqs = [
            'date', 'amount', 'balance', 'description', 'status']
        self._days_span = None
        self._validate_schema()
        self._extract_curr_balance()

    def _validate_schema(self) -> None:
        is_valid, error_message = validate_schema(
            self._payload, __ACCOUNT_SCHEMA__, "account_schema"
        )
        if not is_valid:
            raise APIError(f"invalid account structure: `{error_message}`")

    def _extract_curr_balance(self) -> None:
        """Extracts the current balance from the payload.
        """
        self._current_balance = [el for el in self._payload["AcctBal"]
                                 if el["BalType"] == "Current"][0]['CurAmt']["Amt"]

class ReportFiserv:

    def __init__(self) -> None:
        pass
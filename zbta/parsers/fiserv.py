import logging
from typing import Dict, List
from zbta.core.common import validate_schema, APIError
from zbta.core.schemas import __ACCOUNT_SCHEMA__
import pandas as pd

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


class AccountAbstract:
    """Represents the abstract class for the Account Classes.
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
        self._days_span = None

    @property
    def owners_names(self):
        return self._owners_names

    @property
    def bank_name(self):
        return self._bank_name

    @property
    def creation_date(self):
        return self._creation_date

    @property
    def account_number(self):
        return self._account_number

    @property
    def account_number_orig(self):
        return self._account_number_orig

    @property
    def account_name(self):
        return self._account_name

    @property
    def account_type(self):
        return self._account_type

    @property
    def account_subtype(self):
        return self._account_subtype

    @property
    def routing_number(self):
        return self._routing_number

    @property
    def nb_transactions(self):
        return self._nb_transactions

    @property
    def email_addresses(self):
        return self._email_addresses

    @property
    def phone_numbers(self):
        return self._phone_numbers

    @property
    def addresses(self):
        return self._addresses

    @property
    def cities(self):
        return self._cities

    @property
    def states(self):
        return self._states

    @property
    def streets(self):
        return self._streets

    @property
    def zipcodes(self):
        return self._zipcodes

    @property
    def most_recent_transaction_date(self):
        return self._most_recent_transaction_date

    @property
    def oldest_transaction_date(self):
        return self._oldest_transaction_date

    @property
    def most_recent_balance_date(self):
        return self._most_recent_balance_date

    @property
    def oldest_balance_date(self):
        return self._oldest_balance_date

    @property
    def nb_inc_transactions(self):
        return self._nb_inc_transactions

    @property
    def nb_out_transactions(self):
        return self._nb_out_transactions

    @property
    def nb_overdrafts(self):
        return self._nb_overdrafts

    @property
    def current_balance(self):
        return self._current_balance

    @property
    def transactions(self):
        return self._transactions

    @property
    def days_span(self):
        return self._days_span


class AccountFiserv(AccountAbstract):
    """Represents an account found in a FISERV report.
    Note that multiple accounts form a Report object.
    """

    def __init__(self, payload: Dict) -> None:
        super().__init__(payload)
        self._transactions_columns_reqs = [
            'date', 'amount', 'balance', 'description', 'status']
        self._validate_schema()
        self._extract_curr_balance()
        self._owners_info()
        self._extract_transactions()

    def _validate_schema(self) -> None:
        is_valid, error_message = validate_schema(
            self._payload, __ACCOUNT_SCHEMA__, "account_schema"
        )
        if not is_valid:
            raise APIError(f"invalid account structure: `{error_message}`")

    def _extract_curr_balance(self) -> None:
        """Extracts the current balance from the payload.
        """
        self._current_balance = [el for el in self._payload['accountinfo']["AcctBal"]
                                 if el["BalType"] == "Current"][0]['CurAmt']["Amt"]

    def _owners_info(self):
        """
        Parse the PII information from the account.
        """
        if "AcctOwnerName" in (self._payload["accountinfo"][
                "FIAcctInfo"].keys()):
            self._owners_names.append(self._payload["accountinfo"][
                "FIAcctInfo"]["AcctOwnerName"].lower())
        else:
            self._owners_names.append("")
        self._account_number_orig = self._payload['accountinfo'][
            'FIAcctInfo']['FIAcctId']['AcctId']

    def _extract_transactions(self) -> None:
        """Extracts the transactions from the raw report.
        """
        self._transactions = pd.DataFrame(self._payload["banktrans"][
            "result"]["DepAcctTrnInqRs"]["DepAcctTrns"]["BankAcctTrnRec"])
        df_amounts = self._transactions["CurAmt"].apply(pd.Series)
        self._transactions.loc[:, "amount"] = df_amounts["Amt"]
        self._transactions = self._transactions.rename(
            columns={"PostedDt": "date",
                     "Memo": "description",
                     "TrnID": "id"})
        self._transactions.loc[:, "status"] = "posted"

        # make sure signs are ok
        self._transactions.loc[
            self._transactions.TrnType.str.lower() == "debit", "amount"] = (
            self._transactions.loc[
                self._transactions.TrnType.str.lower() == "debit",
                "amount"].abs()
        )
        self._transactions.loc[
            self._transactions.TrnType.str.lower() == "credit", "amount"] = -(
            self._transactions.loc[
                self._transactions.TrnType.str.lower() == "credit",
                "amount"].abs()
        )
        self._transactions = self._transactions.rename(
            columns={"Category": "category"})

        self._nb_transactions = self._transactions.shape[0]


class ReportFiserv:
    """Represents a full report.
    """
    __INVALID_ACCT_TYPE__ = ["CCA"]

    def __init__(self, payload: Dict, acct_ins: AccountAbstract) -> None:
        self._acct_ins = acct_ins  # the account class
        self._accts = []  # the accounts objects
        self._payload = payload  # the json payload
        self._nb_accounts = 0  # the number of accounts in the report
        self._nb_accounts_rep = 0 # the number of nodes "account" in the report some might be invalid
        self._nb_transactions = {}
        self._create_accts()

    def _create_accts(self) -> None:
        """Creates account object based on the payload.
        """
        self._nb_accounts_rep = len(self._payload['bt_data']['data'])
        for icc, acc in enumerate(self._payload['bt_data']['data']):
            if acc['accountinfo']['FIAcctInfo']['FIAcctId']['AcctType'] not in self.__INVALID_ACCT_TYPE__:
                self._accts.append(self._acct_ins(acc))
                self._nb_transactions[icc] = self._accts[-1].nb_transactions
        if len(self._accts) != self._nb_accounts:
            logging.debug("some accounts were ignored because invalid type.")
        self._nb_accounts = len(self._accts)
        self._nb_transactions_tot = sum(
            [el for el in list(self._nb_transactions.values()) if el is not None
             ]
        )


    @property
    def accounts(self) -> List[AccountFiserv]:
        """Returns the account objects.

        Returns
        -------
        AccountFiserv
            the accounts object
        """
        return self._accts

    @property
    def nb_accounts(self) -> int:
        """returns the number of accounts in the report

        Returns
        -------
        int
            the number of accounts in the report.
        """
        return self._nb_accounts

    @property
    def nb_transactions(self) -> Dict:
        """returns the number of transactions perf account reference 
        by their position in the report.

        Returns
        -------
        Dict
            the mapping of report_id to number of transactions
        """
        return self._nb_transactions

    @property
    def nb_transactions_tot(self) -> int:
        """returns the total number of transactions accross
        all accounts.

        Returns
        -------
        int
            the total number of transactions accross all accounts.
        """
        return self._nb_transactions_tot

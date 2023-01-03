import logging
from datetime import timedelta, datetime
import warnings
import numpy as np
from typing import Dict, List
from zbta.core.common import validate_schema, APIError, NoTransactionError, NoValidAccountError
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

    def _standardize_transactions(self) -> None:
        if any([el not in self._transactions.columns
                for el in self._transactions_columns_reqs]):
            logger.error(
                "Missing required column in the transaction data frame: `{}`"
                .format(self._transactions_columns_reqs))
            raise ValueError(
                "Missing required column in the transaction data frame: `{}`"
                .format(self._transactions_columns_reqs))
        # reset the index
        self._transactions.index = pd.RangeIndex(self._nb_transactions)
        # turn dates into pandas datetime objects
        self._transactions.date = pd.to_datetime(self._transactions.date)
        # convert amount and balance to float
        self._transactions.loc[
            :, ['amount', 'balance']] = self._transactions.loc[
            :, ['amount', 'balance']].astype(float)
        # convert description and status to str
        self._transactions.loc[
            :, ['description', 'status']] = self._transactions.loc[
            :, ['description', 'status']].astype(str)

        # Get common data from transaction table
        if self._transactions.shape[0] > 0:
            self._oldest_transaction_date = self._transactions["date"].min()
            self._most_recent_transaction_date = self._transactions["date"].max(
            )
            self._nb_inc_transactions = (
                self._transactions["amount"] > 0).sum()
            self._nb_out_transactions = (
                self._transactions["amount"] < 0).sum()
            self._nb_overdrafts = (
                self._transactions["balance"] < 0).sum()
            self._days_span = (self._transactions["date"].max()
                            - self._transactions["date"].min()).days

    def _standardize_pii(self):
        for idx, city in enumerate(self._cities):
            self._cities[idx] = city.lower()

        for idx, name in enumerate(self._owners_names):
            self._owners_names[idx] = name.lower()

        for idx, email in enumerate(self._email_addresses):
            self._email_addresses[idx] = email.lower()

        for idx, street in enumerate(self._streets):
            self._streets[idx] = street.lower()


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
        self._standardize_transactions()  # from abstract class
        self._standardize_pii()  # from abstract class

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

    def _get_trans(self) -> None:
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
            self._transactions.TrnType.str.lower() == "debit", "amount"] = -1*(
            self._transactions.loc[
                self._transactions.TrnType.str.lower() == "debit",
                "amount"].abs()
        )
        self._transactions.loc[
            self._transactions.TrnType.str.lower() == "credit", "amount"] = (
            self._transactions.loc[
                self._transactions.TrnType.str.lower() == "credit",
                "amount"].abs()
        )
        self._transactions = self._transactions.rename(
            columns={"Category": "category"})

        self._transactions.drop('CurAmt', axis=1, inplace=True)

        self._nb_transactions = self._transactions.shape[0]

    def _compute_oldest_newest_dates(self):
        """Obtain oldest / newest balance date
        """
        self._oldest_balance_date = pd.to_datetime(self._payload[
            "banktrans"]["result"]["DepAcctTrnInqRs"]["DepAcctTrns"][
            "SelectionCriterion"]["SelRangeDt"]["StartDt"])
        self._most_recent_balance_date = pd.to_datetime(self._payload[
            "banktrans"]["result"]["DepAcctTrnInqRs"]["DepAcctTrns"][
            "SelectionCriterion"]["SelRangeDt"]["EndDt"])

        if self._oldest_balance_date > self._most_recent_balance_date:
            intermediate = self._oldest_balance_date
            self._oldest_balance_date = self._most_recent_balance_date
            self._most_recent_balance_date = intermediate

    def _extract_transactions(self) -> None:

        # Get transactions
        self._get_trans()
        self._compute_oldest_newest_dates()

        self._nb_transactions = self._transactions.shape[0]

        # extract the data frame of transactions
        if self._nb_transactions == 0:
            self._transactions = pd.DataFrame(columns=[
                'date', 'amount', 'status', 'description', 'balance'])

        else:
            self._transactions['status'] = (
                self._transactions['status'].str.lower().apply(
                    lambda x: 'pending' if "pending" in x else 'posted'))

            # removing pending trans
            self._transactions = self._transactions.loc[
                self._transactions.status != "pending"]

            self._nb_transactions = self._transactions.shape[0]
            with warnings.catch_warnings():
                # Not sure why the other copy in place do not raise the
                # Setting values in-place is fine, ignore the warning in Pandas >= 1.5.0
                # This can be removed, if Pandas 1.5.0 does not need to be supported any longer.
                # See also: https://stackoverflow.com/q/74057367/859591
                warnings.filterwarnings(
                    "ignore",
                    category=FutureWarning,
                    message=(
                        ".*will attempt to set the values inplace instead of always setting a new array. "
                        "To retain the old behavior, use either.*"
                    ),
                )
                self._transactions.loc[:, "date"] = pd.to_datetime(
                    self._transactions["date"], format="%Y-%m-%d")
            self._transactions = self._transactions.sort_values(
                by=["date", "id"], ascending=[True, True])
            self._transactions.loc[:,
                                   "amount"] = self._transactions.loc[:, "amount"]

            self._transactions.loc[:, "amount_collected"] = 0
            self._transactions.loc[:, "amount_collected"] = (
                self._transactions["amount"].cumsum())
            starting_amount = self._current_balance - self._transactions[
                "amount"].sum()
            self._transactions.loc[:, "balance"] = (
                self._transactions["amount_collected"] + starting_amount)


class ReportFiserv:
    """Represents a full report.
    """
    __INVALID_ACCT_TYPE__ = ["CCA"]

    def __init__(self, payload: Dict, acct_ins: AccountAbstract) -> None:
        self._acct_ins = acct_ins  # the account class
        self._accts = []  # the accounts objects
        self._payload = payload  # the json payload
        self._nb_accounts = 0  # the number of accounts in the report
        # the number of nodes "account" in the report some might be invalid
        self._nb_accounts_rep = 0
        self._nb_transactions = {}
        self._min_date = None  # the minimum date accross accounts
        self._max_date = None  # the maximum date accross accounts
        self._dfs = None  # the merged transactions
        self._create_accts()
        self._merge_accts()  # creates a single view of the accounts

    def _create_accts(self) -> None:
        """Creates account object based on the payload.
        """
        self._nb_accounts_rep = len(self._payload['bt_data']['data'])
        for icc, acc in enumerate(self._payload['bt_data']['data']):
            if acc['accountinfo']['FIAcctInfo']['FIAcctId']['AcctType'] not in self.__INVALID_ACCT_TYPE__:
                _acc = self._acct_ins(acc)
                if _acc.nb_transactions != 0:
                    self._accts.append(_acc)
                    self._nb_transactions[icc] = self._accts[-1].nb_transactions
        if len(self._accts) != self._nb_accounts:
            logging.debug(
                "some accounts were ignored because invalid type or no transactions found.")
        self._nb_accounts = len(self._accts)
        self._nb_transactions_tot = sum(
            [el for el in list(self._nb_transactions.values()) if el is not None
             ]
        )
        if self._nb_accounts == 0:
            raise NoValidAccountError(
                "No valid account found to calculate attributes")

        if self._nb_transactions_tot == 0:
            raise NoTransactionError("No transaction Found.")

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

    @property
    def dfs(self) -> pd.DataFrame:
        return self._dfs

    @property
    def dfs_daily(self) -> pd.DataFrame:
        return self._df_daily

    def _merge_accts(self) -> None:
        """Merges the different tables into a single view.
        """
        self._dfs = [
            self._get_trans_table(x) for x in self._accts]
        self._min_date = np.min(
            [x.oldest_balance_date for x in self._accts
                if x.oldest_balance_date is not None])
        self._max_date = np.max(
            [x.most_recent_balance_date for x in self._accts
                if x.most_recent_balance_date is not None])

        self._dfs = pd.concat(self._dfs)
        self._dfs = self._dfs.sort_values(
            by=["date", "account_number"]).reset_index(drop=True).rename(
                columns={"balance": "balance_acct"})

        self._dfs.loc[:, "out"] = (
            self._dfs["amount"] < 0).astype("int64")
        # self._categorise_transactions()
        self._daily_bals = [
            self._get_end_of_day(acc) for acc in self._accts]
        self._df_daily = pd.concat(self._daily_bals).groupby(by="date")[
            "balance"].sum().to_frame().reset_index()

    def _get_end_of_day(self, account):
        """
        Get balance at the end of the day

        Parameters
        -------------

        account: AccountAbstract
            AccountAbstract object
        """

        df = account.transactions

        dateloopvar = account.oldest_balance_date
        ndays = int((account.most_recent_balance_date
                     - account.oldest_balance_date).days)+1

        #  Make sure all dates are included between min and max date
        if df.shape[0] == 0:
            list_balances = [account.current_balance]*ndays
            list_dates = [dateloopvar + timedelta(days=idays)
                          for idays in range(0, ndays)]
            return pd.DataFrame(
                {"date": list_dates, "balance": list_balances})
            # return pd.DataFrame(columns=["date", "balance"])

        list_dates = [0.]*ndays
        list_balances = [0.]*ndays
        Ncases = 0

        df_endofday = df.loc[
            :, ["date", "balance"]].groupby(by="date").last().reset_index(
        ).sort_values(by="date")

        df = df.sort_values(by="date", ascending=True)

        day_bal = df["balance"].iloc[0] - df["amount"].iloc[0]
        counter_orig = 0
        Ncases = 0
        while (dateloopvar <= account.most_recent_balance_date):
            condition = dateloopvar == df_endofday["date"].values[counter_orig]

            if condition:
                day_bal = df_endofday["balance"].values[counter_orig]
                if counter_orig < df_endofday.shape[0]-1:
                    counter_orig += 1
            elif day_bal is not None:
                list_dates[Ncases] = dateloopvar
                list_balances[Ncases] = day_bal
                Ncases += 1

            dateloopvar += timedelta(days=1)

        df_endofday = pd.concat(
            [
                df_endofday,
                pd.DataFrame(
                    {"date": list_dates[:Ncases],
                     "balance": list_balances[:Ncases]}
                )
            ],
            ignore_index=True
        ).sort_values(by="date")

        return df_endofday

    def _get_trans_table(self, account):
        """
        Return transaction table from an account and add account_number.

        Parameters
        ------------

        account: AccontAbstrct object
            account object
        """
        transtable = account.transactions
        if transtable.shape[0] > 0:
            transtable.loc[:, "account_number"] = account.account_number
        else:
            transtable["account_number"] = None

        return transtable

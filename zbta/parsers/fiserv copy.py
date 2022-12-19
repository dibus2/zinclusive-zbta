__date__ = "5/26/2020"

import logging
# import json
import pandas as pd
from gdsbbta.parsers.common import AccountAbstract, BTReportAbstract

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


# Associated to one node
class AcctFiservProd(AccountAbstract):
    """
    Read in a json node from Yodlee format and generate the proper 
    AccountAbstract structure.

    Parameters
    ----------
    json_node : dict
        node with the account to be processed

    Raises
    ------
    NotImplementedError
        Multiple owners for a single account are not implemented yet
    """

    __MANDATORY_FIELDS__ = [  # 'official_name',
        'accountinfo',
        'banktrans']

    # These fields are automatically extracted and populated in the class as
    # attributes via __setattr__ method and mapped to the internal common
    # schema name.
    __FIELDS_TO_EXTRACT_AUTO__ = {
    }

    def __init__(self, json_node):
        super().__init__(json_node)
        self._compute_stats()
        # must be called after compute stats.
        self._standardize_transaction_table()
        self._standardize_pii()

    def _owners_info(self):
        """
        Parse the PII information from the account.
        """
        if "AcctOwnerName" in (self._json_node["accountinfo"][
                "FIAcctInfo"].keys()):
            self._owners_names.append(self._json_node["accountinfo"][
                "FIAcctInfo"]["AcctOwnerName"].lower())
        else:
            self._owners_names.append("")
        self._account_number_orig = self._json_node['accountinfo'][
            'FIAcctInfo']['FIAcctId']['AcctId']

    def _compute_oldest_newest_dates(self):
        """Obtain oldest / newest balance date
        """
        self._oldest_balance_date = pd.to_datetime(self._json_node[
            "banktrans"]["result"]["DepAcctTrnInqRs"]["DepAcctTrns"][
            "SelectionCriterion"]["SelRangeDt"]["StartDt"])
        self._most_recent_balance_date = pd.to_datetime(self._json_node[
            "banktrans"]["result"]["DepAcctTrnInqRs"]["DepAcctTrns"][
            "SelectionCriterion"]["SelRangeDt"]["EndDt"])

        if self._oldest_balance_date > self._most_recent_balance_date:
            intermediate = self._oldest_balance_date
            self._oldest_balance_date = self._most_recent_balance_date
            self._most_recent_balance_date = intermediate
    
    def get_transactions(self):
        """
        Get transactions
        """
        self._transactions = pd.DataFrame(self._json_node["banktrans"][
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
        self._transactions = self._transactions.rename(columns={"Category": "category"})

    def _compute_stats(self):
        """
        Process the historical balance and transactions in the account.
        """
        self._owners_info()

        # Balance info
        dict_account = self._json_node["accountinfo"]
        if "AcctBal" in dict_account:
            self._current_balance = dict_account["AcctBal"][0]["CurAmt"]["Amt"]
        else:
            logger.error("No balance found, I can't continue")
            raise Exception("No balance found, I can't continue")

        # Get transactions
        self.get_transactions()
        self._compute_oldest_newest_dates()

        # Get current balance
        self._nb_transactions = self._transactions.shape[0]

        # extract the data frame of transactions
        if self._nb_transactions == 0:
            self._transactions = pd.DataFrame(columns=[
                'date', 'amount', 'status', 'description', 'balance'])

        else:
            self._transactions['status'] = (
                self._transactions['status'].str.lower().apply(
                    lambda x: 'pending' if "pending" in x else 'posted'))

            # ************ N O T E ***************
            # We are including ONLY non-pending Transaction
            self._transactions = self._transactions.loc[
                self._transactions.status != "pending"]

            self._nb_transactions = self._transactions.shape[0]
            self._transactions.loc[:, "date"] = pd.to_datetime(
                self._transactions["date"], format="%Y-%m-%d")
            self._transactions = self._transactions.sort_values(
                by=["date", "id"], ascending=[True, True])

            self._transactions.loc[:, "amount_collected"] = 0
            self._transactions.loc[:, "amount_collected"] = (
                    self._transactions["amount"].cumsum())
            starting_amount = self._current_balance + self._transactions[
                "amount"].sum()
            self._transactions.loc[:, "balance"] = (
                -self._transactions["amount_collected"] + starting_amount)


class BTReportFiservProd(BTReportAbstract):
    """
    Parse a Plaid report with multiple accounts to generate a BT report.

    Parameters
    ----------
    json_report : dict
        bt report generated with the Plaid API
    account_cls : AccountAbstract
        class used to parse the individual accounts in the report
    """

    def __init__(self, json_report, account_cls):
        super().__init__(json_report)
        self._account_cls = account_cls
        self._create_accounts()
        self._init_values()  # must be run after the create accounts otherwise
        # will throw an error

    def _create_accounts(self):
        """
        Iterate through the accounts parsing them and adding them to the
        report. Credit, loans, brokerage and other accounts are excluded.
        """
        __INVALID_ACCOUNTS__ = ["CREDIT", "LOAN", "OTHER", "BROKERAGE"]
        address = None
        email = None
        root_rep = self._json_report
        if "data" not in root_rep.keys():
            root_rep = self._json_report['lender_request'][
                'bank_transaction_data']['data']
            # if "application_information" in self._json_report[
            #         "lender_request"]:
            #     address = self._json_report["lender_request"][
            #         "application_information"]["signer_address"]
            #     email = self._json_report["lender_request"][
            #         "application_information"]["signer_email"]

        counter_acc = 0

        for this_dictionary in root_rep:

            if (("accountinfo" not in this_dictionary.keys()) 
                or ("banktrans" not in this_dictionary.keys())):
                continue

            account_type = ""
            for names in this_dictionary["accountinfo"]["FIAcctInfo"][
                    "FIAcctName"]:
                account_type += "{} ".format(names["ParamVal"])
            
            # No credit accounts
            invalid_account = False
            for invalid in __INVALID_ACCOUNTS__:
                if invalid in account_type:
                    invalid_account = True
                    break

            if invalid_account:
                continue

            if self._accounts is None:
                self._accounts = [self._account_cls(this_dictionary)]
            else:
                self._accounts.append(self._account_cls(this_dictionary))
            self._accounts[counter_acc]._account_number = counter_acc
            counter_acc += 1

        #if self._accounts is None:
        #    logger.error("No valid accounts were found")
        #    raise RuntimeError("No valid accounts were found")
import pandas as pd
from datetime import timedelta
from typing import List, Dict
import numpy as np
import logging
from zbta.btanalyzer.assets.categories_salary_like_fp import __DICT_EXCLUSIONS_SALARY_LIKE_FP__

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


class SalaryLikeTagger:
    """Income Estimator works by identifying `salary like` transactions.
    This means that a transaction must be repeated in description and amount
    several times in the last X days. 
    """

    def __init__(self,
                 dfs: pd.DataFrame,
                 report_max_date: pd.Timestamp,
                 dict_salary_like_fp: Dict = __DICT_EXCLUSIONS_SALARY_LIKE_FP__
                 ) -> None:
        self._dfs = dfs
        self._report_max_date = report_max_date
        self._dict_salary_like_fp = dict_salary_like_fp

    @property
    def dfs(self) -> pd.DataFrame:
        return self._dfs

    def tag_income_transactions(
        self,
        ndays=180,  # number of days over which to look for tagging
        amt_thr=100,  # the minimum amount of the transaction to be considered potential income
        nwords=6,  # fir N number of words to use for the comparison of description
        nminfreq=5,  # the minimum number of time a transaction must repeat
        # The number of different months over which the transaction must be found.
        diffmonths=4,
        diffdays_min=60  # minimum history in the account to run the algo
    ) -> None:
        """
            tag salary-like transactions
        """

        if self._dfs.shape[0] == 0:
            # set up default
            self._dfs["is_salary_like"] = ""
            return

        self._dfs.loc[:, "is_salary_like"] = False

        # Check if enough number of days
        last_date = self._get_last_date("last_date")
        mindate = self._dfs["date"].min()
        diffdays = (last_date-mindate).days
        if ndays > diffdays:
            ndays = diffdays
            nminfreq = int(diffdays/30.)-1
            diffmonths = nminfreq - 1
            if nminfreq == 1:
                nminfreq = 2
            if diffmonths < 2:
                diffmonths = 2

        if diffdays < diffdays_min:  # not enough data to judge!!
            return

        transaction_mask = self._limit_transaction_dataset(
            last_date="last_date",
            ndays=ndays,
            amt_thr=amt_thr,
            is_inc=True,
            remove_internal=True
        )

        # Remove transactions with keywords that do not correspond to salaries
        transaction_mask = self._sal_like_remove_non_salary_transactions(
            transaction_mask)

        # remove keywords as defined in the dictionary
        self._dfs = self._clean_description_salarylike(
            self._dfs)
        transactions = self._dfs[transaction_mask]
        if 'is_investment' in transactions:
            transactions = transactions[~transactions.is_investment]
        if 'is_taxes' in transactions:
            transactions = transactions[~transactions.is_taxes].copy()

        # keywords
        description = transactions.clean_description_salarylike.str.split().str[
            :nwords]
        transactions["description_split"] = description.astype(str).str.lower()
        ntrans = transactions.shape[0]

        for idx in range(ntrans-1):
            splitdesc = transactions["description_split"].iloc[idx]
            list_repeated = [idx]
            for idx_second in range(idx+1, ntrans):
                splitdesc_two = transactions["description_split"].iloc[
                    idx_second]
                stringbase = transactions["clean_description_salarylike"].iloc[
                    idx_second]
                if len(stringbase) == 0:
                    continue

                ncases = np.isin(splitdesc.split(),
                                 splitdesc_two.split()).sum()
                nlensplit = len(splitdesc.split())
                nlensplit2 = len(splitdesc_two.split())
                # if less or equal to 3 words require all match else if 4 only 3 out of 4
                if nlensplit <= 3 and ncases == nlensplit and nlensplit2 > 0 and ncases > 0:
                    list_repeated.append(idx_second)
                elif nlensplit == 4 and ncases >= 3:
                    list_repeated.append(idx_second)
                elif ncases >= 4:
                    list_repeated.append(idx_second)

            if len(list_repeated) < nminfreq:
                continue

            # Transactions in different months
            if diffmonths > 1:
                Nmonths = len(transactions.iloc[list_repeated].loc[
                    :, "date"].dt.month.unique())
                if Nmonths < diffmonths:
                    continue

            indices = transactions.iloc[list_repeated].index
            self._dfs.loc[indices, "is_salary_like"] = True
            self._dfs.loc[
                self._dfs.amount < 0, "is_salary_like"] = False

    def _limit_transaction_dataset(
        self,
        last_date: pd.Timestamp,
        ndays: int,
        amt_thr: float,
        is_inc: bool = False,
        is_out: bool = False,
        remove_internal: bool = False,
        categories: List[str] = []
    ) -> pd.Series:
        """
        Trim the dataset to the relevant transaction window.

        Restricts the dataset to the last N days with transactions higher
        than the selected threshold, that can be Incoming, Outgoing or both,
        and withing given categories.

        Parameters
        ----------
        last_date : pd.Timestamp
            date cutoff for the transactions dataset
        ndays : int
            days previous to the last date cutoff to consider
        amt_thr : float
            minimum amount (in absolute terms) to consider a transaction
        is_inc : bool, optional
            consider only incoming transactions, by default False
        is_out : bool, optional
            consider only outgoing transactions, by default False
        remove_internal : bool, optional
            consider only external transactions, by default False
        categories : List[str], optional
            list of categories to be considered, by default ""

        Returns
        -------
        pd.Series
            a pd series containing booleans that mask the non-relevant
            transactions according to the conditions imposed
        """
        dataset = self.dfs
        last_date = self._get_last_date(last_date)
        first_date = last_date - timedelta(days=ndays)

        temporal_mask = (dataset.date >= first_date) & (
            dataset.date <= last_date)
        amount_mask = dataset.amount.abs() > amt_thr

        cashflow_mask = pd.Series(
            np.ones(len(dataset)),
            index=dataset.index,
            dtype=bool
        )

        if is_inc:
            cashflow_mask = cashflow_mask & (dataset.amount > 0)
        if is_out:
            cashflow_mask = cashflow_mask & (dataset.amount < 0)

        if len(categories) >= 1:
            category_mask = pd.Series(
                np.zeros(len(dataset)),
                index=dataset.index,
                dtype=bool
            )
            for category in categories:
                category_mask = category_mask | dataset[category]

        else:
            category_mask = pd.Series(
                np.ones(len(dataset)),
                index=dataset.index,
                dtype=bool
            )

        external_mask = pd.Series(
            np.ones(len(dataset)),
            index=dataset.index,
            dtype=bool
        )
        if remove_internal:
            external_mask = external_mask & ~dataset["is_internal"]

        return temporal_mask & amount_mask & cashflow_mask & category_mask \
            & external_mask

    def _get_last_date(
        self,
        last_date: str
    ) -> pd.Timestamp:
        """
        Get the timestamp of the last balance/transaction to be considered.

        Parameters
        ----------
        last_date : str
            data cutoff for the transactions dataset, if "last date", the 
            date from the last balance or transaction in the history is used

        Returns
        -------
        pd.Timestamp
            timestamp of the last transaction to be considered
        """
        if last_date == "last_date":
            logger.debug("Warning: taking last date available")
            return self._report_max_date
        elif type(last_date) is pd.Timestamp:
            return last_date
        else:
            return pd.to_datetime(last_date, format="%Y-%m-%d")

    def _sal_like_remove_non_salary_transactions(self, transaction_mask):
        """
            remove transactions that do not correspond to salary
        """
        keywords_contain = self._dict_salary_like_fp[
            "keywords_exclude_contain"]
        for keyword in keywords_contain:
            transaction_mask = (transaction_mask) & (
                self._dfs.clean_description.str.contains(keyword) == False)
        keywords_exact = self._dict_salary_like_fp[
            "keywords_exclude_exact"]
        for keyword in keywords_exact:
            transaction_mask = (transaction_mask) & (
                self._dfs.clean_description.str.split().apply(
                    lambda x: keyword in x) == False
            )
        return transaction_mask

    def _clean_description_salarylike(self, transactions):
        """
            clean description for salary-like algorithm
        """
        transactions.loc[:, "clean_description_salarylike"] = transactions[
            "clean_description"]
        keywords_empty = self._dict_salary_like_fp[
            "keywords_replace_empty"]
        for keyword in keywords_empty:
            transactions.loc[:, "clean_description_salarylike"] = transactions[
                "clean_description_salarylike"].str.replace(keyword, "")

        keywords_empty_exact = self._dict_salary_like_fp[
            "keywords_replace_empty_exact"]
        transactions.loc[:, "clean_description_salarylike"] = transactions[
            "clean_description_salarylike"].str.split().apply(lambda x:
                                                              " ".join(
                                                                  [a for a in x if (
                                                                      (a in keywords_empty_exact) is False)]
                                                              )
                                                              )

        transactions.loc[:, "clean_description_salarylike"] = transactions[
            "clean_description_salarylike"].str.replace(" +", " ", regex=True)

        return transactions

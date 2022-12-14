from zbta.parsers.fiserv import ReportFiserv
from zbta.btanalyzer.salary_like_tagger import SalaryLikeTagger
from zbta.btanalyzer.internal_transfer_algo import InternalTransferTagger
from zbta.btanalyzer.assets.us_states import __DICT_STATES_US__
from zbta.btanalyzer.assets.categories_general_match import __DICT_CATEGORIES_GENERAL_MATCH__
from zbta.btanalyzer.assets.categories_general_contained import __DICT_CATEGORIES_GENERAL_CONTAINED__
from zbta.btanalyzer.assets.categories_salary_like_fp import __DICT_EXCLUSIONS_SALARY_LIKE_FP__
from zbta.btanalyzer.assets.categories_priorities import __DICT_CATEGORIES_PRIORITIES__
from typing import Dict, List, Union, Optional
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


class BTAnalyzer:
    """Analyzes and Tags the transactions present in the report.
    1. clean up the descriptions
        1.1 lower case
        1.2 clean up states
        1.3 ? 
    2. Categories
        2.1 Contain Keyword vs exact match
        2.2 weekend vs weekdays
        2.3 nweek, Nmonth
    """

    def __init__(
        self,
        report: ReportFiserv,
        # dictionary of words to tag with exact match
        dict_kw_id_match: Dict = __DICT_CATEGORIES_GENERAL_MATCH__,
        # dictionary of words to tag, if they are contained
        dict_kw_id_contained: Dict = __DICT_CATEGORIES_GENERAL_CONTAINED__,
        # use all categories [] or none "none"
        limit_kw_id_match: Union[List, str] = [],
        # use all categories [] or none "none"
        limit_kw_id_contained: Union[List, str] = [],
        # dictionary for cleaning up states
        dict_us_states_cu: Dict = __DICT_STATES_US__,
        # dictionary for salary like clean up 
        dict_salary_like_fp: Dict = __DICT_EXCLUSIONS_SALARY_LIKE_FP__,
        # dictionary of priorities for the categories
        dict_enforce_priorities: Dict = __DICT_CATEGORIES_PRIORITIES__,
        # whether or not to identify week days vs weekend transactions.
        do_weekend_id: bool = True,
        # whether or not to calculate the number of weeks since pull
        do_nweek_nmonth_id: bool = True,
        # tag internal transfers
        do_internal_transfers: bool = True,
        # do salary_like
        do_salary_like: bool = True,
        # enforce priorities on categories
        do_enforce_priorities: bool = True
    ) -> None:
        self._report = report
        self._dfs = self._report.dfs
        self._dfs_daily = self._report.dfs_daily
        self._dict_kw_id_match = dict_kw_id_match
        self._dict_kw_id_contained = dict_kw_id_contained
        self._dict_salary_like_fp = dict_salary_like_fp
        self._dict_enforce_priorities = dict_enforce_priorities
        self._limit_kw_id_match = limit_kw_id_match
        self._limit_kw_id_contained = limit_kw_id_contained
        self._dict_us_states_cu = dict_us_states_cu
        self._do_weekend_id = do_weekend_id
        self._do_nweek_nmonth_id = do_nweek_nmonth_id
        self._do_internal_transfers = do_internal_transfers
        self._do_enforce_priorities = do_enforce_priorities
        self._do_salary_like = do_salary_like
        self._list_acc_types = []  # the types of the accts
        self._names = []
        self._streets = []
        self._cities = []
        self._zipcodes = []
        self._states = []
        self._emails = []
        self._phone_numbers = []
        self._nb_trans = 0
        self._nb_inc_trans = 0
        self._nb_out_trans = 0
        self._nb_overdrafts_trans = 0
        self._nb_overdrafts_days = 0
        self._nb_accounts = 0
        self._clean_up_description()
        self._categorize_transactions()
        self._count_inc_out_over()
        self._validate_transactions()
        self._consolidate_kycs()
        if self._do_internal_transfers:
            self._tag_internal_transfers()
        if self._do_salary_like:
            self._tag_salary_like()
        if self._do_enforce_priorities:
            self._enforce_priorities()

    @property
    def dfs(self) -> pd.DataFrame:
        return self._dfs

    @property
    def dfs_daily(self) -> pd.DataFrame:
        return self._dfs_daily

    @property
    def report(self) -> ReportFiserv:
        return self._report

    @property
    def nb_overdrafts_trans(self) -> int:
        return self._nb_overdrafts_trans

    @property
    def nb_overdrafts_days(self) -> int:
        return self._nb_overdrafts_days

    @property
    def nb_accounts(self) -> int:
        return self._nb_accounts

    @property
    def nb_inc_trans(self) -> int:
        return self._nb_inc_trans

    def _clean_up_description(self) -> None:
        """creates a cleaned up version of the description.
        """
        # 1. lower all + remove common words
        self._lower_description = (
            self._dfs["description"].str.lower()
        )

        self._clean_description = (
            self._lower_description.str.replace(
                '[^a-zA-Z ]',
                '',
                regex=True
            ).str.replace(" +", " ", regex=True)
        )

        self._dfs.loc[:, "clean_description"] = \
            self._clean_description

    def _categorize_transactions(self) -> None:
        """Categorizes the transactions using the dictionaries.
        Two different processes:
        1. Very few words if they are present in the string are smoking guns
        these are identified automatically
        2. The others they must be exactly in there.
        """

        self._split_description = self._lower_description.str.split(
            expand=True)

        self._split_clean_description = self._clean_description.str.split(
            expand=True)

        # Im here. This needs to be re-written/adapted
        # filter out the category to match
        if self._limit_kw_id_match != "none":
            if self._limit_kw_id_match != []:
                if 'is_transfer' not in self._limit_kw_id_match and self._do_internal_transfers:
                    self._limit_kw_id_match.append('is_transfer')
                if 'is_investment' not in self._limit_kw_id_match and self._do_salary_like:
                    self._limit_kw_id_match.append('is_investment')
                if 'is_taxes' not in self._limit_kw_id_match and self._do_salary_like:
                    self._limit_kw_id_match.append('is_taxes')
                self._dict_kw_id_match = {key: val for key, val in self._dict_kw_id_match.items(
                ) if key in self._limit_kw_id_match}

            for category in self._dict_kw_id_match:
                self._dfs[category] = False
                for keyword in self._dict_kw_id_match[category]:
                    # we need to use the full description if the keyword contains a digit
                    # to time this
                    if any(char.isdigit() for char in keyword):
                        keyword_in_description = \
                            (self._split_description == keyword).sum(
                                axis=1).astype(bool)
                    else:
                        keyword_in_description = \
                            (self._split_clean_description == keyword).sum(
                                axis=1).astype(bool)

                    self._dfs[category] = \
                        self._dfs[category] | keyword_in_description
        if self._limit_kw_id_contained != "none":
            if self._limit_kw_id_contained != []:
                if 'is_transfer' not in self._limit_kw_id_contained and self._do_internal_transfers:
                    self._limit_kw_id_contained.append('is_transfer')
                if 'is_investment' not in self._limit_kw_id_contained and self._do_salary_like:
                    self._limit_kw_id_contained.append('is_investment')
                if 'is_taxes' not in self._limit_kw_id_contained and self._do_salary_like:
                    self._limit_kw_id_contained.append('is_taxes')
                self._dict_kw_id_contained = {key: val for key, val in self._dict_kw_id_contained.items(
                ) if key in self._limit_kw_id_contained}

            for category in self._dict_kw_id_contained:
                if category not in self._dfs:
                    self._dfs[category] = False
                for keyword in self._dict_kw_id_contained[category]:
                    # we need to use the full description if the keyword contains a digit
                    if any(char.isdigit() for char in keyword):
                        keyword_in_description = \
                            self._lower_description.str.contains(keyword)
                    else:
                        keyword_in_description = \
                            self._clean_description.str.contains(keyword)

                    self._dfs[category] = \
                        self._dfs[category] | keyword_in_description

        # clean up salary tagging to only positive
        if "is_salary" in self._dfs.columns.tolist():
            self._dfs.loc[
                self._dfs.amount < 0, "is_salary"] = False

        if "is_salary_like" in self._dfs.columns.tolist():
            self._dfs.loc[
                self._dfs.amount < 0, "is_salary_like"] = False

        # Weekdays vs weekends
        if self._do_weekend_id:
            self._dfs.loc[:, "is_weekend"] = (
                self._dfs["date"].dt.weekday >= 5)  # Mon = 0, Sun=6

        # # Nweek, Month, Nday
        if self._do_nweek_nmonth_id:
            self._dfs.loc[:, "month"] = (
                self._dfs["date"].dt.month)
            self._dfs.loc[:, "week"] = (
                self._dfs["date"].dt.isocalendar().week)

            self._dfs.loc[:, "day"] = (
                self._dfs["date"].dt.dayofyear)

    def _add_prop(self, prop, vector):
        """
        Append value to the PII vector if it is not there already.

        Parameters
        ----------
        prop 
            value to append
        vector : list
            vector with the PII information

        Returns
        -------
        list
            updated vector
        """
        if prop not in vector:
            vector += [prop]
        return vector

    def _consolidate_kycs(self):
        """
        Extract and consolidates the PII from the accounts.
        """
        self._names = []
        self._streets = []
        self._cities = []
        self._zipcodes = []
        self._states = []
        self._emails = []
        self._phone_numbers = []

        for account in self._report.accounts:
            for name in account._owners_names:
                name = name.lower()
                self._names = self._add_prop(name, self._names)

            for email in account._email_addresses:
                email = email.lower()
                self._emails = self._add_prop(email, self._emails)

            for phone in account._phone_numbers:
                self._phone_numbers = self._add_prop(
                    phone, self._phone_numbers)

            for street in account._streets:
                street = street.lower()
                self._streets = self._add_prop(street, self._streets)

            for city in account._cities:
                city = city.lower()
                self._cities = self._add_prop(city, self._cities)

            for zipcode in account._zipcodes:
                self._zipcodes = self._add_prop(zipcode, self._zipcodes)

            for state in account._states:
                state = state.upper()
                if state in __DICT_STATES_US__.keys():
                    state = __DICT_STATES_US__[state]
                state = state.replace(" ", "")
                self._states = self._add_prop(state, self._states)


    # TODO this is not being extracted at the moment not present in Fiserv
    # def _fill_out_acc_types(self):
    #    """
    #    Get account types from accounts classes.
    #    """
    #    list_acc_types = [
    #        x._account_type.upper() if "str" in str(type(x.account_type))
    #        else "" for x in self._report.accounts]
    #    list_acc_subtypes = [
    #        x._account_subtype.upper() if "str" in
    #        str(type(x.account_subtype)) else ""
    #        for x in self._report._accts]

    #    self._list_acc_types = ["{} {}".format(x, y) for x, y in zip(
    #        list_acc_types, list_acc_subtypes)]

    def _count_inc_out_over(self) -> None:
        """Calculates the number of incoming outgoing and overdrafts
        in the overall table.
        """
        # self._fill_out_acc_types()
        self._nb_trans = self._dfs.shape[0]

        self._nb_inc_trans = np.sum(
            self._dfs["amount"] > 0)

        self._nb_overdrafts_trans = (
            self._dfs["balance_acct"] < 0).sum()

        self._nb_overdrafts_days = (
            self._dfs_daily["balance"] < 0).sum()

        self._nb_accounts = self._report.nb_accounts

    def _validate_transactions(self) -> None:
        """Validates the number of transactions extracted from 
        the table vs from the separated accounts.
        """
        check_nb_trans = np.sum(
            list(self._report.nb_transactions.values()))
        if self._nb_trans != check_nb_trans:
            logger.debug("Different nb of transactions between table and accts ins: {}, {}".format(
                self._nb_trans, check_nb_trans))

        check_nb_inc_trans = np.sum([
            acc.nb_inc_trans
            for acc in self._report.accounts
            if acc.nb_inc_trans is not None])
        if (self._nb_inc_trans !=
                check_nb_inc_trans):
            logger.debug("Different nb of cashin transactions between table and acct ins: {}, {}".format(
                self._nb_inc_trans,
                check_nb_inc_trans))

        check_nb_overdrafts_trans = np.sum([
            acc.nb_overdrafts
            for acc in self._report.accounts
            if acc.nb_overdrafts is not None])
        if self._nb_overdrafts_trans != check_nb_overdrafts_trans:
            logger.debug("Different nb of overdrafts between table and accts: {}, {}".format(
                self._nb_of_overdrafts_trans, check_nb_overdrafts_trans
            ))

    def _tag_internal_transfers(self) -> None:
       tagger = InternalTransferTagger(self._dfs)
       tagger.tag_internal_transfers()
       self._dfs = tagger.dfs

    def _tag_salary_like(self) -> None:
        tagger = SalaryLikeTagger(
            self._dfs,
            self._report.max_date,
            self._dict_salary_like_fp
        )
        tagger.tag_income_transactions()
        self._dfs = tagger.dfs

    def _enforce_priorities(self) -> None:
        """In some cases multiple categories are exclusive 
        and priority must be enforced.
        """
        for excluded_category in self._dict_enforce_priorities:
            for priority_category in self._dict_enforce_priorities[excluded_category]:
                self._dfs.loc[
                    self._dfs[priority_category],
                    excluded_category
                ] = False

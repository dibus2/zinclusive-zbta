from zbta.parsers.fiserv import ReportFiserv
from zbta.btanalyzer.assets.us_states import __DICT_STATES_US__
from zbta.btanalyzer.assets.categories_general_match import __DICT_CATEGORIES_GENERAL_MATCH__
from zbta.btanalyzer.assets.categories_general_contained import __DICT_CATEGORIES_GENERAL_CONTAINED__
from typing import Dict, List, Union
import pandas as pd


class BTAnalyzer:
    """Analyzes and Tags the transactions present in the report.
    1. clean up the descriptions
        1.1 lower case
        1.2 clean up states
        1.3 ? 
    2. Categories
        2.1 Contain Keyword
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
        dict_us_states_cu: Dict = __DICT_STATES_US__,
        # whether or not to identify week days vs weekend transactions.
        do_weekend_id: bool = True,
        # whether or not to calculate the number of weeks since pull
        do_nweek_nmonth_id: bool = True
    ) -> None:
        self._report = report
        self._dfs = self._report.dfs
        self._dfs_daily = self._report.dfs_daily
        self._dict_kw_id_match = dict_kw_id_match
        self._dict_kw_id_contained = dict_kw_id_contained
        self._limit_kw_id_match = limit_kw_id_match
        self._limit_kw_id_contained = limit_kw_id_contained
        self._dict_us_states_cu = dict_us_states_cu
        self._do_weekend_id = do_weekend_id
        self._do_nweek_nmonth_id = do_nweek_nmonth_id
        self._names = []
        self._streets = []
        self._cities = []
        self._zipcodes = []
        self._states = []
        self._emails = []
        self._phone_numbers = []
        self._clean_up_description()
        self._categorize_transactions()
        self._consolidate_kycs()

    @property
    def dfs(self) -> pd.DataFrame:
        return self._dfs
    
    @property
    def dfs_daily(self) -> pd.DataFrame:
        return self._dfs_daily

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

        #if "is_salary_like" in self._dfs.columns.tolist():
        #    self._dfs.loc[
        #        self._dfs.amount < 0, "is_salary_like"] = False

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
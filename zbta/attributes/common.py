from zbta.btanalyzer.btanalyzer import BTAnalyzer
import inspect
from typing import List, Union
import pandas as pd
import numpy as np
from datetime import timedelta
from functools import wraps
import csv
import logging

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)

class auto_short_doc(object):

    def __init__(self, short_description, full_name):
        self._short_description = short_description
        self._full_name = full_name

    def __call__(self, f):
        @wraps(f)
        def wrapped_f(*args, **kwargs):
            if kwargs != {} and "generate_doc" in kwargs and kwargs["generate_doc"]:
                return self._short_description, self._full_name
            else:
                return f(*args)
        return wrapped_f

class ZBTACore:


    def __init__(self, btanalyzer: BTAnalyzer) -> None:
        self._btanalyzer = btanalyzer
        self._attributes = {}
        self._attribute_names = []
        self._attribute_methods = []
        self._nb_attributes = 0
        self._descriptions = []  # the desciption of all attributes
        self._generic_codes = {} # mapping between method names and attribute id
        self._extract_attributes_names()
        self._extract_info_from_methods()
    
    @property
    def nb_attributes(self) -> int:
        """Returns the number of attributes."""
        return self._nb_attributes

    def _extract_attributes_names(self) -> None:
        """Inspects the attributes implemented in the class.
        """
        self._attribute_names = [
            el[0] for el in inspect.getmembers(self) if "__ZBTA_" in el[0]]
        self._attribute_methods = [
            el[1] for el in inspect.getmembers(self) if "__ZBTA_" in el[0]]
        self._nb_attributes = len(self._attribute_names)

    def _extract_info_from_methods(self):
        """Generates the description list which contains all the info
        stored in the decorator
        """
        # the way the doc is generated is by calling each one of the attributes with the "generate_doc=True" flag
        # this triggers the wrapper to return the short description.
        self._descriptions = []
        self._generic_codes = {}
        for el in inspect.getmembers(self):
             if "__ZBTA" in el[0]:
                self._descriptions.append({'Attribute Name': el[0].split("_")[4],
                               'Attribute Code': el[1](generate_doc=True)[1],
                               'Module': el[0].split('_')[3],
                               'Description': el[1](generate_doc=True)[0],
                               'Method': el[0]})
                self._generic_codes[el[0]] = self._descriptions[-1]['Attribute Code']

    def create_list_attributes_csv(self, output_file):
        """Generates a csv file with the description of the attributes.

        :param output_file: the name of the output_file
        :type output_file: str
        :return: None
        :rtype: None
        """
        if not self._descriptions:
            self._extract_info_from_methods()
        if not "csv" in output_file[-3:]:
            logger.warning("adding .csv to the file extension.")
            output_file += ".csv"
        csv_columns = ['Method', 'Attribute Name', 'Attribute Code', 'Module', 'Description']
        try:
            with open(output_file, "w") as ff:
                writer = csv.DictWriter(ff, fieldnames=csv_columns)
                writer.writeheader()
                for data in self._descriptions:
                    writer.writerow(data)
        except IOError as err:
            logger.error("Error writing to csv: `{}`".format(err))
            pass

    def calculate_attributes(self) -> None:
        """Generates all the attributes.
        """
        self._last_date = get_last_date(self._btanalyzer, "last-date")
        for iaa, aa in enumerate(self._attribute_names):
            self._attributes[self._attribute_names[iaa]] = getattr(self, aa)()


def limit_transaction_dataset(
    btanalyzer: BTAnalyzer,
    last_date: pd.Timestamp,
    ndays: int,
    amt_thr: float,
    is_inc: bool = False,
    is_out: bool = False,
    remove_internal: bool = False,
    categories: List[str] = [],
    whichmonth: int = None
) -> pd.Series:
    """
    Trim the dataset to the relevant transaction window.

    Restricts the dataset to the last N days with transactions higher
    than the selected threshold, that can be Incoming, Outgoing or both,
    and withing given categories.

    Parameters
    ----------
    btanalyzer : BTAnalyzer
        object with all the transactions dataset
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
    whichmonth: int, optional
        which month to look at. If 0, it is current month. If -1, it is 
        previous month, etc

    Returns
    -------
    pd.Series
        a pd series containing booleans that mask the non-relevant
        transactions according to the conditions imposed
    """
    dataset = btanalyzer.dfs
    last_date = get_last_date(btanalyzer, last_date)
    first_date = last_date - timedelta(days=ndays)

    temporal_mask = (dataset.date >= first_date) & (dataset.date <= last_date)
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
    
    if whichmonth is not None:
        monthcheck = last_date.month
        yearcheck = last_date.year

        monthcheck = monthcheck + whichmonth

        while(monthcheck <= 0):
            monthcheck = 12 + monthcheck
            yearcheck -= 1

        temporal_mask = (temporal_mask) & (
            dataset.date.dt.month == monthcheck) & (
            dataset.date.dt.year == yearcheck)

    return temporal_mask & amount_mask & cashflow_mask & category_mask \
        & external_mask


def limit_historical_dataset(
    btanalyzer: BTAnalyzer,
    last_date: pd.Timestamp,
    ndays: int,
    amt_thr: float,
    is_overdraft: bool = False,
    dataset: Union[None, pd.DataFrame] = None,
    whichmonth: Union[None, int] = None
) -> pd.Series:
    """
    Trim the dataset to the relevant balance window.

    Restricts the dataset to the last N days with balances higher
    than the selected threshold, that can be restricted to only
    overdrafted balances.

    Parameters
    ----------
    btanalyzer : BTAnalyzer
        object with all the transactions and balances dataset
    last_date : pd.Timestamp
        date cutoff for the balance dataset
    ndays : int
        days previous to the last date cutoff to consider
    amt_thr : float
        minimum amount (in absolute terms) to consider a transaction
    is_overdraft : bool, optional
        consider only overdrafted balances, by default False
    dataset: pd.DataFrame
        Dataframe with information, default to None
    whichmonth: int
        which month to consider, by default None

    Returns
    -------
    pd.Series
        a pd series containing booleans that mask the non-relevant
        balances according to the conditions imposed
    """

    if dataset is None:
        dataset = btanalyzer.dfs_daily
    last_date = get_last_date(btanalyzer, last_date)
    first_date = last_date - timedelta(days=ndays)

    temporal_mask = (dataset.date >= first_date) & (dataset.date <= last_date)
    amount_mask = dataset.balance.abs() >= amt_thr

    overdraft_mask = pd.Series(
        np.ones(len(dataset)),
        index=dataset.index,
        dtype=bool
    )
    if is_overdraft:
        overdraft_mask = dataset.balance < 0

    if whichmonth is not None:
        monthcheck = last_date.month
        yearcheck = last_date.year

        monthcheck = monthcheck + whichmonth

        while(monthcheck <= 0):
            monthcheck = 12 + monthcheck
            yearcheck -= 1

        temporal_mask = (temporal_mask) & (
            dataset.date.dt.month == monthcheck) & (
            dataset.date.dt.year == yearcheck)

    mask = temporal_mask & amount_mask & overdraft_mask
    return mask, first_date


def get_last_date(
    btanalyzer: BTAnalyzer,
    last_date: str
) -> pd.Timestamp:
    """
    Get the timestamp of the last balance/transaction to be considered.

    Parameters
    ----------
    btanalyzer : btanalyzer
        object with all the transactions dataset
    last_date : str
        data cutoff for the transactions dataset, if "last date", the 
        date from the last balance or transaction in the history is used

    Returns
    -------
    pd.Timestamp
        timestamp of the last transaction to be considered
    """
    if last_date == "last-date":
        logger.debug("Warning: taking last date available")
        return btanalyzer.report.max_date
    elif type(last_date) is pd.Timestamp:
        return last_date
    else:
        return pd.to_datetime(last_date, format="%Y-%m-%d")

from typing import Union, Optional
from zbta.attributes.common import auto_short_doc
from zbta.attributes.common import limit_transaction_dataset
import pandas as pd

import logging

logger = logging.getLogger(__name__)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter(
    "[%(levelname)s] [%(asctime)s] [%(funcName)s] %(message)s")
fh.setFormatter(fh_formatter)
logger.addHandler(fh)
logger.setLevel(logging.ERROR)


class CoreAccountMixin:
    """Contains the Core Account attributes. 
    These are mostly basic, descriptive attributes
    of the accounts such as the total number of transactions, 
    the average amounts, the activity description and so on.
    """
    # NOTE: I timed this, it takes about 3 mili per call.
    def __META_CORE_NbTransactions__(
        self,
        last_date: Optional[Union[str, pd.Timestamp]] = "last-date",
        ndays: int = 90,
        amt_thr: float = 0,
        is_inc: bool = False,
        is_out: bool = False
    ) -> int:

        transaction_mask = limit_transaction_dataset(
            self._btanalyzer,
            last_date=last_date,
            ndays=ndays,
            amt_thr=amt_thr,
            is_inc=is_inc,
            is_out=is_out,
            remove_internal=True
        )

        return self._btanalyzer.dfs[transaction_mask].shape[0]

    def __META_CORE_TotalDollarAmount(
        self,
        last_date: Optional[Union[str, pd.Timestamp]] = "last-date",
        ndays: int = 90,
        amt_thr: float = 0,
        is_inc: bool = False,
        is_out: bool = False
    ) -> int:

        transaction_mask = limit_transaction_dataset(
            self._btanalyzer,
            last_date=last_date,
            ndays=ndays,
            amt_thr=amt_thr,
            is_inc=is_inc,
            is_out=is_out,
            remove_internal=True
        )

        return self._btanalyzer.dfs[transaction_mask]['amount'].abs().sum()

    @auto_short_doc("Total Number of Transaction Ever", "CORE001")
    def __ZBTA_CORE_NbTransactionsEver__(self) -> int:
        """Returns the number of transactions.

        Returns:
            int: the number of transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=1000,
            amt_thr=0,
            is_inc=False,
            is_out=False,
        )

    @auto_short_doc("Total Number of Transaction in the last 90 days", "CORE002")
    def __ZBTA_CORE_NbTransactionsD90__(self) -> int:
        """Returns the number of transactions in the last 90 days.

        Returns:
            int: the number of transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=False,
            is_out=False,
        )

    @auto_short_doc("Total Number of Transaction in the last 60 days", "CORE003")
    def __ZBTA_CORE_NbTransactionsD60__(self) -> int:
        """Returns the number of transactions in the last 60 days.

        Returns:
            int: the number of transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=60,
            amt_thr=0,
            is_inc=False,
            is_out=False,
        )
    
    @auto_short_doc("Total Number of Transaction in the last 30 days", "CORE004")
    def __ZBTA_CORE_NbTransactionsD30__(self) -> int:
        """Returns the number of transactions in the last 30 days.

        Returns:
            int: the number of transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=30,
            amt_thr=0,
            is_inc=False,
            is_out=False,
        )

    @auto_short_doc("Total Number of Incoming Transaction Ever", "CORE005")
    def __ZBTA_CORE_NbIncomingTransactionsEver__(self) -> int:
        """Returns the number of incoming transactions.

        Returns:
            int: the number of transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=1000,
            amt_thr=0,
            is_inc=True,
            is_out=False,
        )

    @auto_short_doc("Total Number of Incoming Transaction in the last 90 days", "CORE006")
    def __ZBTA_CORE_NbIncomingTransactionsD90__(self) -> int:
        """Returns the number of incoming transactions in the last 90 days.

        Returns:
            int: the number of incoming transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=True,
            is_out=False,
        )

    @auto_short_doc("Total Number of Incoming Transaction in the last 60 days", "CORE007")
    def __ZBTA_CORE_NbIncomingTransactionsD60__(self) -> int:
        """Returns the number of incoming transactions in the last 60 days.

        Returns:
            int: the number of incoming transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=60,
            amt_thr=0,
            is_inc=True,
            is_out=False,
        )
    
    @auto_short_doc("Total Number of Incoming Transaction in the last 30 days", "CORE008")
    def __ZBTA_CORE_NbIncomingTransactionsD30__(self) -> int:
        """Returns the number of incoming transactions in the last 30 days.

        Returns:
            int: the number of incoming transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=30,
            amt_thr=0,
            is_inc=True,
            is_out=False,
        )

    @auto_short_doc("Total Number of Outgoing Transaction Ever", "CORE009")
    def __ZBTA_CORE_NbOutgoingTransactionsEver__(self) -> int:
        """Returns the number of outgoing transactions.

        Returns:
            int: the number of outgoing transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=1000,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )

    @auto_short_doc("Total Number of Outgoing Transaction in the last 90 days", "CORE010")
    def __ZBTA_CORE_NbOutgoingTransactionsD90__(self) -> int:
        """Returns the number of outgoing transactions in the last 90 days.

        Returns:
            int: the number of outgoing transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )

    @auto_short_doc("Total Number of Outgoing Transaction in the last 60 days", "CORE011")
    def __ZBTA_CORE_NbOutgoingTransactionsD60__(self) -> int:
        """Returns the number of outgoing transactions in the last 60 days.

        Returns:
            int: the number of outgoing transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=60,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )
    
    @auto_short_doc("Total Number of Outgoing Transaction in the last 30 days", "CORE012")
    def __ZBTA_CORE_NbOutgoingTransactionsD30__(self) -> int:
        """Returns the number of outgoing transactions in the last 30 days.

        Returns:
            int: the number of outgoing transactions
        """
        return self.__META_CORE_NbTransactions__(
            "last-date",
            ndays=30,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )

    @auto_short_doc("Total Dollar Amount in Transactions Ever", "CORE013")
    def __ZBTA_CORE_TotalDollarAmountEver__(self) -> int:
        """Returns the total dollar amount in transactions ever.

        Returns:
            int: the dollar amount in transactions
        """
        return self.__META_CORE_TotalAmount(
            "last-date",
            ndays=1000,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )

    @auto_short_doc("Total Dollar Amount in Transactions in the last 90 days", "CORE014")
    def __ZBTA_CORE_TotalDollarAmountD90__(self) -> int:
        """Returns the total dollar amount in transactions in the last 90 days.

        Returns:
            int: the dollar amount in transactions in the last 90 days
        """
        return self.__META_CORE_TotalDollarAmount(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )

    @auto_short_doc("Total Dollar Amount in Incoming Transactions Ever", "CORE015")
    def __ZBTA_CORE_TotalDollarAmountIncomingEver__(self) -> int:
        """Returns the total dollar amount in incoming transactions ever.

        Returns:
            int: the dollar amount in incoming transactions ever
        """
        return self.__META_CORE_TotalDollarAmount(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=True,
            is_out=False,
        )

    @auto_short_doc("Total Dollar Amount in Incoming Transactions in the last 90 days", "CORE016")
    def __ZBTA_CORE_TotalDollarAmountIncomingD90__(self) -> int:
        """Returns the total dollar amount in incoming transactions in the last 90 days.

        Returns:
            int: the dollar amount in incoming transactions in the last 90 days
        """
        return self.__META_CORE_TotalDollarAmount(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=True,
            is_out=False,
        )

    @auto_short_doc("Total Dollar Amount in Outgoing Transactions Ever", "CORE017")
    def __ZBTA_CORE_TotalDollarAmountOutgoingEver__(self) -> int:
        """Returns the total dollar amount in outgoing transactions ever.

        Returns:
            int: the dollar amount in outgoing transactions ever
        """
        return self.__META_CORE_TotalDollarAmount(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )

    @auto_short_doc("Total Dollar Amount in Outgoing Transactions in the last 90 days", "CORE018")
    def __ZBTA_CORE_TotalDollarAmountOutgoingD90__(self) -> int:
        """Returns the total dollar amount in outgoing transactions in the last 90 days.

        Returns:
            int: the dollar amount in outgoing transactions in the last 90 days
        """
        return self.__META_CORE_TotalDollarAmount(
            "last-date",
            ndays=90,
            amt_thr=0,
            is_inc=False,
            is_out=True,
        )
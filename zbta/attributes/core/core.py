from typing import Union, Optional
from zbta.attributes.common import auto_short_doc
from zbta.attributes.common import limit_transaction_dataset
import pandas as pd


class CoreAccountMixin:
    """Contains the Core Account attributes. 
    These are mostly basic, descriptive attributes
    of the accounts such as the total number of transactions, 
    the average amounts, the activity description and so on.
    """

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

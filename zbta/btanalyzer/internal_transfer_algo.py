import numpy as np
import pandas as pd
from typing import Optional, Tuple
from strsimpy.jaccard import Jaccard


class InternalTransferTagger:
    """Tags Internal Transfers from the transaction table. 
    The main algorithm is described inside the tag_internal_transfers method.
    """

    def __init__(self, dfs: pd.DataFrame) -> None:
        self._dfs = dfs

    def _add_internal_to_dataframe(
        self,
        transactions: np.ndarray
    ) -> None:
        """
        Merge the internal transfer info into the transaction dataframe.

        Parameters
        ----------
        transactions : np.ndarray
            numpy array with the transactions info
        """
        is_internal = np.zeros(len(self._dfs), dtype=np.int64)
        matched_transaction = -np.ones(
            len(self._dfs),
            dtype=np.int64
        )

        for record in transactions:
            if record[4] == 1:
                is_internal[record[0]] = record[4]
                matched_transaction[record[0]] = record[5]

        self._dfs["is_internal"] = is_internal.astype(bool)
        self._dfs["matched_internal"] = matched_transaction

    @property
    def dfs(self) -> pd.DataFrame:
        """Returns the transaction table with tagged internal transfers.

        Returns:
            pd.DataFrame: the transaction table.
        """
        return self._dfs

    @staticmethod
    def _is_internal_transfer(
        transfer: int,
        transactions: np.ndarray,
        descriptions: np.ndarray
    ) -> Optional[int]:
        """
        Check if a transfer is internal or not.

        Parameters
        ----------
        transfer : int
            transaction index of the transfer to be checked
        transactions : np.ndarray
             numpy array with the transactions info

        Returns
        -------
        Optional[int]
            the transaction index of the matching transfer is there is
            one, or None if there isn't
        """
        index = np.where(transactions[:, 0] == transfer)[0]
        description = descriptions[index][0]
        account = transactions[index, 1]
        date = transactions[index, 2]
        amount = transactions[index, 3]

        # mask by date as it is the most restrictive
        # NOTE: This could made a bit  more lenient by adding a small delta
        mask = transactions[:, 2] == date
        transactions_day = transactions[mask]
        descriptions_day = descriptions[mask]
        # require a different account number and opposite amount
        mask = transactions_day[:, 1] != account
        mask = mask & (transactions_day[:, 3] == -amount)
        mask = mask & (transactions_day[:, 4] == 0)

        potential_matches = transactions_day[mask]
        potential_matches_descriptions = descriptions_day[mask]
        # then for each potential matches we retain the one with the 
        # smallest distance (highest similarity) using hte Jacard k=1 shingle
        # distance.
        jacObj = Jaccard(k=1)

        if len(potential_matches) > 0:
            description_similarity = np.zeros(len(potential_matches))
            for i in range(len(potential_matches)):
                description_similarity[i] = jacObj.distance(
                    description,
                    potential_matches_descriptions[i]
                )
            maximum_similarity = np.argmin(description_similarity)
            return potential_matches[maximum_similarity, 0]
        else:
            return None

    @staticmethod
    def _flag_internal_transfer(
        transfer: int,
        matched_transfer: int,
        transactions: np.ndarray
    ) -> np.ndarray:
        """
        Incorporate the internal transfers to the transactions array.

        Parameters
        ----------
        transfer : int
            transaction index of the internal transfer
        matched_transfer : int
            transaction index of the complimentary internal transfer
        transactions : np.ndarray
            numpy array with the transactions info

        Returns
        -------
        np.ndarray
            a modified transactions array with the internal transfers
            information set
        """
        transfer_index = np.where(transactions[:, 0] == transfer)[0]
        transactions[transfer_index, 4] = 1
        transactions[transfer_index, 5] = matched_transfer

        matched_transfer_index = \
            np.where(transactions[:, 0] == matched_transfer)[0]
        transactions[matched_transfer_index, 4] = 1
        transactions[matched_transfer_index, 5] = transfer

        return transactions

    def _create_transactions_array(self) -> Tuple[np.ndarray, np.ndarray]:
        """
        Create a numpy array with the transactions information.

        Transforms the transactional data from the pandas DataFrame to
        a numpy array to speed up the comparisons between transactions
        in order to find internal transfers.
        Note is_transfer is mandatory

        Returns
        -------
        np.ndarray
            a numpy array with this 6 columns: transaction index,
            account_no (int), date (epoch), amount (int, cents),
            is_internal (int, zeros), matched index (int, -1)
        np.ndarray
            a numpy array containing the transaction descriptions
        """
        transfers = self._dfs[
            self._dfs.is_transfer
        ]

        transactions = np.zeros(
            (len(transfers), 6),
            dtype=np.int64
        )

        transactions[:, 0] = \
            transfers.index.to_numpy(copy=True)
        transactions[:, 1] = \
            transfers.account_number.astype('category').cat.codes.to_numpy(copy=True)
        transactions[:, 2] = \
            (transfers.date.view(np.int64)/1e9).view(np.int64).to_numpy(copy=True)
        transactions[:, 3] = \
            (100*transfers.amount).to_numpy(copy=True)
        transactions[:, 4] = np.zeros(len(transfers))
        transactions[:, 5] = np.zeros(len(transfers))

        descriptions = transfers.description.to_numpy(copy=True)

        return transactions, descriptions

    def tag_internal_transfers(self) -> None:
        """Tags internal transfers. The algorithm is the following:
        Iterate through incoming transfers and try to find a matching
        transfer outgoing with opposite amount in one of the other accounts.
        We also require that opposite transfer to happen on the same day.
        """
        transactions, descriptions = self._create_transactions_array()

        prospective_internal_transfers = \
            self._dfs[
                (self._dfs.is_transfer) &
                (self._dfs.amount > 0)
            ].index

        for transfer in prospective_internal_transfers:
            matched_transfer = \
                self._is_internal_transfer(
                    transfer,
                    transactions,
                    descriptions
                )
            if matched_transfer is not None:
                # if a matching transfer has been found --> update hte transaction table
                transactions = self._flag_internal_transfer(
                    transfer,
                    matched_transfer,
                    transactions
                )

        self._add_internal_to_dataframe(transactions)
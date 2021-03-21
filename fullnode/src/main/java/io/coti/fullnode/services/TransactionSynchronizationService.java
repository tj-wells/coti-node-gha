package io.coti.fullnode.services;

import io.coti.basenode.data.AddressTransactionsHistory;
import io.coti.basenode.data.Hash;
import io.coti.basenode.data.TransactionData;
import io.coti.basenode.services.BaseNodeTransactionSynchronizationService;
import io.coti.basenode.services.interfaces.ITransactionService;
import io.coti.fullnode.data.DateAddressTransactionsHistory;
import io.coti.fullnode.model.DateAddressTransactionsHistories;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class TransactionSynchronizationService extends BaseNodeTransactionSynchronizationService {

    @Autowired
    private DateAddressTransactionsHistories dateAddressTransactionsHistories;
    @Autowired
    private TransactionHelper transactionHelper;
    @Autowired
    private ITransactionService transactionService;

    @Override
    protected void insertMissingTransactions(List<TransactionData> missingTransactions, Set<Hash> trustChainUnconfirmedExistingTransactionHashes, AtomicLong completedMissingTransactionNumber, AtomicBoolean finishedToReceive, int offset) {
        int missingTransactionsSize;
        int nextOffSet;
        Map<Hash, AddressTransactionsHistory> addressToTransactionsHistoryMap = new ConcurrentHashMap<>();
        Map<Hash, DateAddressTransactionsHistory> dateAddressToTransactionsHistoryMap = new ConcurrentHashMap<>();
        new TreeMap<LocalDate, Set<Hash>>();
        while ((missingTransactionsSize = missingTransactions.size()) > offset || !finishedToReceive.get()) {
            if (missingTransactionsSize - 1 > offset || (missingTransactionsSize - 1 == offset && missingTransactions.get(offset) != null)) {
                nextOffSet = offset + (finishedToReceive.get() ? missingTransactionsSize - offset : 1);
                for (int i = offset; i < nextOffSet; i++) {
                    TransactionData transactionData = missingTransactions.get(i);
                    transactionService.handleMissingTransaction(transactionData, trustChainUnconfirmedExistingTransactionHashes, missingTransactionExecutorMap);
                    transactionHelper.updateAddressTransactionHistory(addressToTransactionsHistoryMap, transactionData);
                    transactionHelper.updateDateAddressTransactionHistory(dateAddressToTransactionsHistoryMap, transactionData);
                    missingTransactions.set(i, null);
                    completedMissingTransactionNumber.incrementAndGet();
                }
                offset = nextOffSet;
            }
        }
        addressTransactionsHistories.putBatch(addressToTransactionsHistoryMap);
        dateAddressTransactionsHistories.putBatch(dateAddressToTransactionsHistoryMap);
    }
}

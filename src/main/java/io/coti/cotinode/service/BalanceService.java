package io.coti.cotinode.service;

import io.coti.cotinode.data.ConfirmedTransactionData;
import io.coti.cotinode.data.Hash;
import io.coti.cotinode.data.UnconfirmedTransactionData;
import io.coti.cotinode.database.RocksDBConnector;
import io.coti.cotinode.model.ConfirmedTransactions;
import io.coti.cotinode.model.UnconfirmedTransactions;
import io.coti.cotinode.service.interfaces.IBalanceService;
import io.coti.cotinode.service.interfaces.IQueueService;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Service
public class BalanceService implements IBalanceService {

    @Autowired
    private IQueueService queueService;

    @Autowired
    private RocksDBConnector databaseConnector;

    @Autowired
    private ConfirmedTransactions confirmedTransactions;

    @Autowired
    private UnconfirmedTransactions unconfirmedTransactions;

    private Map<Hash, Double> balanceMap;
    private Map<Hash, Double> preBalanceMap;


    private List<Map<Hash, Double>> unconfirmedTransactionList;

    private List<Map<Hash, Double>> confirmedTransactionList;
    @PostConstruct
    private void init() {
        try {
            balanceMap = new ConcurrentHashMap<>();
            preBalanceMap = new ConcurrentHashMap<>();
            unconfirmedTransactionList = new LinkedList<>();
            confirmedTransactionList = new LinkedList<>();

            loadBalanceFromSnapshot();

            List<UnconfirmedTransactionData> unconfirmedTransactionsToDelete = new LinkedList<>();

            // UnconfirmedTransactionFromDB init
            fillUnconfirmedTransactionsToDeleteFromDB(unconfirmedTransactionsToDelete);

            // ConfirmedTransactionFromDB init
            fillConfirmedTransactionMapFromDB();

            //move items from unnconfirmed to confirmed db table
            removeFromUnconfirmedAndFillConfirmedInDB(unconfirmedTransactionsToDelete);

            //move balances from unconfirmed/confirmed Transaction Map To Balance Maps
            insertFromTempDBmapToInMemMap(confirmedTransactionList, balanceMap);
            insertFromTempDBmapToInMemMap(unconfirmedTransactionList, preBalanceMap);
            confirmedTransactionList.clear();
            unconfirmedTransactionList.clear();

        } catch (Exception ex) {
            log.error("Errors on initiation ", ex);
        }
    }

    /**
     * The task will be executed a first time after the initialDelay (because of the init() ) value – and it will
     * continue to be executed according to the fixedDelay
     */
    @Scheduled(fixedDelay = 5000, initialDelay = 1000)
    private void syncBalanceScheduled() {
        updateDbFromQueue();
    }

    private void updateDbFromQueue() {
        ConcurrentLinkedQueue<Hash> updateBalanceQueue = queueService.getUpdateBalanceQueue();
        while (!updateBalanceQueue.isEmpty()) {
            Hash addressHash = updateBalanceQueue.poll();
            UnconfirmedTransactionData unconfirmedTransactionData = unconfirmedTransactions.getByHash(addressHash);
            //dspc = 1
            if (unconfirmedTransactionData.isDoubleSpendPreventionConsensus()) {
                ConfirmedTransactionData confirmedTransactionData = new ConfirmedTransactionData(addressHash);
                confirmedTransactionData.setAddressHashToValueTransferredMapping(unconfirmedTransactionData.getAddressHashToValueTransferredMapping());
                confirmedTransactions.put(confirmedTransactionData);
                unconfirmedTransactions.delete(addressHash);
                for (Map.Entry<Hash, Double> mapEntry : unconfirmedTransactionData.getAddressHashToValueTransferredMapping().entrySet()) {
                    balanceMap.put(mapEntry.getKey(), mapEntry.getValue());
                    log.info("The address {} with the value {} was added to balance map", mapEntry.getKey(), mapEntry.getValue());
                    preBalanceMap.remove(mapEntry.getKey());
                }
            } else { //dspc =0
                unconfirmedTransactionData.setTrustChainConsensus(true);
                unconfirmedTransactions.put(unconfirmedTransactionData);

            }
        }
    }

    private void removeFromUnconfirmedAndFillConfirmedInDB(List<UnconfirmedTransactionData> unconfirmedTransactionsToDelete) {
        for (UnconfirmedTransactionData unconfirmedTransactionData : unconfirmedTransactionsToDelete) {
            confirmedTransactions.put(unconfirmedTransactionData);
            unconfirmedTransactions.delete(unconfirmedTransactionData.getHash());
        }
        unconfirmedTransactionsToDelete.clear();
    }

    private void insertFromTempDBmapToInMemMap(List<Map<Hash, Double>> addressToBalanceMapFromDB , Map<Hash, Double> addressToBalanceMapInMem ) {
        for (Map<Hash, Double> addressToBalanceMap : unconfirmedTransactionList) {
            for (Map.Entry<Hash, Double> entry : addressToBalanceMap.entrySet()) {
                double balance = entry.getValue();
                Hash key = entry.getKey();
                if(preBalanceMap.containsKey(key)) {
                    preBalanceMap.put(key, balance + preBalanceMap.get(key));
                }
                else {
                    preBalanceMap.put(key, balance);
                }
            }
        }

    }


    private void fillConfirmedTransactionMapFromDB() {
        RocksIterator confirmedDBiterator = databaseConnector.getIterator(UnconfirmedTransactions.class.getName());
        confirmedDBiterator.seekToFirst();
        while (confirmedDBiterator.isValid()) {
            ConfirmedTransactionData confirmedTransactionData = (ConfirmedTransactionData) SerializationUtils
                    .deserialize(confirmedDBiterator.value());
            confirmedTransactionList.add(confirmedTransactionData.getAddressHashToValueTransferredMapping());

            confirmedDBiterator.next();

        }
    }

    private void fillUnconfirmedTransactionsToDeleteFromDB(List<UnconfirmedTransactionData> unconfirmedTransactionsToDelete) {

        RocksIterator unconfirmedDBiterator = databaseConnector.getIterator(UnconfirmedTransactions.class.getName());
        unconfirmedDBiterator.seekToFirst();
        while (unconfirmedDBiterator.isValid()) {
            UnconfirmedTransactionData unconfirmedTransactionData = (UnconfirmedTransactionData) SerializationUtils
                    .deserialize(unconfirmedDBiterator.value());
            if (unconfirmedTransactionData.isTrustChainConsensus()) { //tcc =1
                if (unconfirmedTransactionData.isDoubleSpendPreventionConsensus()) { // tcc = 1 + dspc = 1
                    confirmedTransactionList.add(unconfirmedTransactionData.getAddressHashToValueTransferredMapping());
                    unconfirmedTransactionsToDelete.add(unconfirmedTransactionData);
                } else { // tcc = 1 + dspc = 0
                    unconfirmedTransactionList.add(unconfirmedTransactionData.getAddressHashToValueTransferredMapping());
                }
            } else { //tcc = 0      dspc 0/1
                unconfirmedTransactionList.add(unconfirmedTransactionData.getAddressHashToValueTransferredMapping());
                // TCC QUEUE -> QUEUE Service
                queueService.addToTccQueue(unconfirmedTransactionData.getHash());
            }
            unconfirmedDBiterator.next();
        }
    }

    public boolean checkBalancesAndAddToPreBalance(List<Map.Entry<Hash, Double>> pairList) {
        try {
            for (Map.Entry<Hash, Double> mapEntry : pairList) {
                //checkBalance
                double amount = mapEntry.getValue();
                Hash addressHash = mapEntry.getKey();
                if (balanceMap.containsKey(addressHash) && amount + balanceMap.get(addressHash) < 0) {
                    log.error("The address {} with the amount {} is exceeds it's current balance {} ", addressHash.toString(),
                            amount, balanceMap.get(addressHash));
                    return false;
                }
                //checkPreBalance
                if (preBalanceMap.containsKey(addressHash) && amount + preBalanceMap.get(addressHash) < 0) {
                    log.error("The address {} with the amount {} is exceeds it's current preBalance {} ", addressHash.toString(),
                            amount, preBalanceMap.get(addressHash));
                    return false;
                } else {//update preBalance
                    preBalanceMap.put(addressHash, amount + preBalanceMap.get(addressHash));
                }

            }
        } catch (Exception ex) {
            log.error("Exception on checking balances and adding to preBalance {}", ex);
        }
        return true;
    }

    public void insertIntoUnconfirmedDBandAddToTccQeueue(UnconfirmedTransactionData unconfirmedTransactionData) {
        // put it in unconfirmedTransaction table
        databaseConnector.put(UnconfirmedTransactions.class.getName(), unconfirmedTransactionData.getKey().getBytes(),
                unconfirmedTransactionData.getHash().getBytes());
        for (Map.Entry<Hash, Double> mapEntry : unconfirmedTransactionData.getAddressHashToValueTransferredMapping().entrySet()) {
            queueService.addToTccQueue(mapEntry.getKey());
        }
    }

    private void loadBalanceFromSnapshot() {
        String snapshotFileLocation = "./Snapshot.csv";
        File snapshotFile = new File(snapshotFileLocation);

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(snapshotFile));
            String line;

            while ((line = bufferedReader.readLine()) != null) {
                String[] addressDetails = line.split(",");
                if (addressDetails.length != 2) {
                    throw new Exception("Bad csv file format");
                }
                Hash addressHash = new Hash(addressDetails[0].getBytes());
                Double addressAmount = Double.parseDouble(addressDetails[1]);
                log.info("The hash {} was loaded from the snapshot with amount {}", addressHash, addressAmount);

                if (balanceMap.containsKey(addressHash)) {
                    // throw new Exception(String.format("Double address found in CSV file: %s", addressHash));
                    log.error("The address {} was already found in the snapshot", addressHash);
                }
                balanceMap.put(addressHash, addressAmount);

            }
            // copy the balance to preBalance
            preBalanceMap.putAll(balanceMap);
        } catch (Exception e) {
            log.error("Errors on snapshot loading: {}", e);
        }
    }

    public Map<Hash, Double> getBalanceMap() {
        return balanceMap;
    }
    public Map<Hash, Double> getPreBalanceMap() {
        return preBalanceMap;
    }

}

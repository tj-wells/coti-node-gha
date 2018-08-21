package io.coti.common.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MonitorService {
    @Autowired
    private TransactionHelper transactionHelper;
    @Autowired
    private BalanceService balanceService;
    @Autowired
    private TransactionIndexService transactionIndexService;
    @Autowired
    private ClusterService clusterService;

    public void init() {
        log.info("Monitor Service is up");
    }

    @Scheduled(initialDelay = 1000, fixedDelay = 5000)
    public void lastState() {
        log.info("Transactions = {}, TccConfirmed = {}, DspConfirmed = {}, Confirmed = {}, LastIndex = {}, Sources = {}",
                transactionHelper.getTotalTransactions(),
                balanceService.getTccConfirmed(),
                balanceService.getDspConfirmed(),
                balanceService.getTotalConfirmed(),
                transactionIndexService.getLastTransactionIndex().getIndex(),
                clusterService.getTotalSources());
    }
}

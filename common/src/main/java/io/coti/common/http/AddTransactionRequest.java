package io.coti.common.http;

import io.coti.common.data.BaseTransactionData;
import io.coti.common.data.Hash;
import io.coti.common.data.TransactionTrustScoreData;

import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

public class AddTransactionRequest extends Request {
    @NotNull
    public List<BaseTransactionData> baseTransactions;
    @NotNull
    public Hash hash;
    @NotNull
    public String transactionDescription;
    @NotNull
    public Date createTime;
    @NotNull
    public List<TransactionTrustScoreData> trustScoreResults;
    @NotNull
    public Hash senderHash;
}



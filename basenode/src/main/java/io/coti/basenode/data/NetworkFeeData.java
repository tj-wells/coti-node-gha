package io.coti.basenode.data;

import io.coti.basenode.data.interfaces.ITrustScoreNodeValidatable;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

@Data
public class NetworkFeeData extends OutputBaseTransactionData implements ITrustScoreNodeValidatable {
    @NotNull
    private List<TrustScoreNodeResultData> networkFeeTrustScoreNodeResult;
    private BigDecimal reducedAmount;

    private NetworkFeeData() {
        super();
    }

    public NetworkFeeData(Hash addressHash, BigDecimal amount, BigDecimal originalAmount, Date createTime) {
        super(addressHash, amount, originalAmount, createTime);
    }

    @Override
    public List<TrustScoreNodeResultData> getTrustScoreNodeResult() {
        return networkFeeTrustScoreNodeResult;
    }

    @Override
    public void setTrustScoreNodeResult(List<TrustScoreNodeResultData> trustScoreNodeResult) {
        this.networkFeeTrustScoreNodeResult = trustScoreNodeResult;
    }

    public void setReducedAmount(BigDecimal reducedAmount) {
        if (reducedAmount == null || reducedAmount.signum() <= 0) {
            throw new IllegalStateException("Reduced amount can not have non positive amount");
        }
        this.reducedAmount = reducedAmount;
    }
}

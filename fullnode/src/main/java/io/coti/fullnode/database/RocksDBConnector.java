package io.coti.fullnode.database;

import io.coti.basenode.database.BaseNodeRocksDBConnector;
import io.coti.basenode.model.RequestedAddressHashes;
import io.coti.basenode.model.UnconfirmedReceivedTransactionHashes;
import io.coti.fullnode.model.AddressTransactionsByAttachments;
import io.coti.fullnode.model.ExplorerIndexes;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;

@Primary
@Service
public class RocksDBConnector extends BaseNodeRocksDBConnector {

    @Override
    public void setColumnFamily() {
        super.setColumnFamily();
        columnFamilyClassNames.addAll(Arrays.asList(
                ExplorerIndexes.class.getName(),
                RequestedAddressHashes.class.getName(),
                UnconfirmedReceivedTransactionHashes.class.getName(),
                AddressTransactionsByAttachments.class.getName()
        ));
        resetTransactionColumnFamilyNames.addAll(Collections.singletonList(
                AddressTransactionsByAttachments.class.getName()
        ));
    }
}

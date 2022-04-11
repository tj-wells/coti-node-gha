package io.coti.basenode.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.coti.basenode.data.interfaces.IPropagatable;
import lombok.Data;

@Data
public class ConnectionSubscriberData implements IPropagatable {

    private static final long serialVersionUID = 5578761426811343628L;
    private String publisherAddress;
    private NodeType publisherNodeType;
    private boolean info;

    public ConnectionSubscriberData(String publisherAddress, NodeType publisherNodeType, boolean info) {
        this.publisherAddress = publisherAddress;
        this.publisherNodeType = publisherNodeType;
        this.info = info;
    }

    @Override
    @JsonIgnore
    public Hash getHash() {
        return new Hash(publisherAddress);
    }

    @Override
    public void setHash(Hash hash) {
        // no implementation
    }
}

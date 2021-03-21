package io.coti.fullnode.http;

import io.coti.basenode.data.Hash;
import io.coti.basenode.http.interfaces.IRequest;
import lombok.Data;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.util.List;

@Data
public class GetDateAddressTransactionBatchRequest implements IRequest {
    @NotNull(message = "Address Hashes must not be blank")
    private List<@Valid Hash> addresses;
    @NotNull(message = "Dates must not be blank")
    private List<@Valid LocalDate> dates;
}

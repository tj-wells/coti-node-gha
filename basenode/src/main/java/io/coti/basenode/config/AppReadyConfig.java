package io.coti.basenode.config;

import io.coti.basenode.communication.interfaces.IReceiver;
import io.coti.basenode.communication.interfaces.ISender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Configuration
@Slf4j
public class AppReadyConfig {

    @Autowired
    private IReceiver zeroMQReceiver;
    @Autowired
    private ISender zeroMQSender;

    @EventListener(ApplicationReadyEvent.class)
    public void appReady() {
        zeroMQReceiver.initMonitor();
        zeroMQSender.initMonitor();
    }
}
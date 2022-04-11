package io.coti.basenode.communication;

import io.coti.basenode.communication.data.ZeroMQMessageData;
import io.coti.basenode.communication.interfaces.IReceiver;
import io.coti.basenode.communication.interfaces.ISerializer;
import io.coti.basenode.data.interfaces.IPropagatable;
import io.coti.basenode.exceptions.ZeroMQReceiverException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
@Service
public class ZeroMQReceiver implements IReceiver {

    private HashMap<String, Consumer<IPropagatable>> classNameToHandlerMapping;
    private ZMQ.Context zeroMQContext;
    private SocketType socketType;
    private ZMQ.Socket receiver;
    private ZMQ.Socket monitorSocket;
    private BlockingQueue<ZeroMQMessageData> messageQueue;
    private Thread receiverThread;
    private Thread monitorThread;
    private Thread messagesQueueHandlerThread;
    @Autowired
    private ISerializer serializer;
    private final AtomicBoolean monitorInitialized = new AtomicBoolean(false);
    private String receivingPort;

    @Override
    public void init(String receivingPort, HashMap<String, Consumer<IPropagatable>> classNameToHandlerMapping) {
        this.classNameToHandlerMapping = classNameToHandlerMapping;
        zeroMQContext = ZeroMQContext.getZeroMQContext();
        socketType = SocketType.ROUTER;
        this.receivingPort = receivingPort;
        messageQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void initMonitor() {
        monitorInitialized.set(true);
    }

    @Override
    public void startListening() {
        startReceiverThread();
        startMonitorThread();
    }

    private void startReceiverThread() {
        receiverThread = new Thread(() -> {
            receiver = zeroMQContext.socket(socketType);
            if (!receiver.bind("tcp://*:" + receivingPort)) {
                throw new ZeroMQReceiverException("ZeroMQ receiver socket bind failed to receiver port " + receivingPort);
            }
            log.info("ZeroMQ bound to socket in thread ID: {} of thread: {}", Thread.currentThread().getId(), Thread.currentThread().getName());

            startReceivingMessages();
            ZeroMQUtils.closeSocket(receiver);
        }, "ROUTER");
        receiverThread.start();
    }

    private void startReceivingMessages() {
        while (!ZeroMQContext.isContextTerminated() && !Thread.currentThread().isInterrupted()) {
            try {
                String classType = receiver.recvStr();
                log.info("ZeroMQ receiving class type: {} in thread ID: {} of thread: {}", classType, Thread.currentThread().getId(), Thread.currentThread().getName());
                addToMessageQueue(classType);
            } catch (ZMQException e) {
                if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                    log.info("ZeroMQ receiver context terminated");
                    ZeroMQContext.setContextTerminated(true);
                } else {
                    log.error("ZeroMQ exception at receiver thread", e);
                }
            } catch (Exception e) {
                log.error("Error at receiver thread", e);
            }
        }
    }

    private void startMonitorThread() {
        monitorThread = new Thread(() -> {
            monitorSocket = ZeroMQUtils.createAndConnectMonitorSocket(zeroMQContext, receiver);
            log.info("Zero MQ Client Connected!");
            while (!ZeroMQContext.isContextTerminated() && !Thread.currentThread().isInterrupted()) {
                try {
                    ZeroMQUtils.getServerSocketEvent(monitorSocket, socketType, monitorInitialized);
                } catch (ZMQException e) {
                    if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
                        log.info("ZeroMQ receiver context terminated");
                        ZeroMQContext.setContextTerminated(true);
                    } else {
                        log.error("ZeroMQ exception at monitor receiver thread", e);
                    }
                } catch (Exception e) {
                    log.error("Exception at monitor receiver thread", e);
                }
            }
            ZeroMQUtils.closeSocket(monitorSocket);
        }, "MONITOR ROUTER");
        monitorThread.start();
    }

    private void addToMessageQueue(String classType) {
        try {
            if (classNameToHandlerMapping.containsKey(classType)) {
                byte[] message = receiver.recv();
                log.info("ZeroMQ receiving msg in thread ID: {} of thread: {}", Thread.currentThread().getId(), Thread.currentThread().getName());
                messageQueue.put(new ZeroMQMessageData(classType, message));
            }
        } catch (InterruptedException e) {
            log.info("ZMQ receiver interrupted");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void initReceiverHandler() {
        messagesQueueHandlerThread = new Thread(this::handleMessagesQueueTask, "ROUTER HANDLER");
        messagesQueueHandlerThread.start();
    }

    private void handleMessagesQueueTask() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ZeroMQMessageData zeroMQMessageData = messageQueue.take();
                Consumer<IPropagatable> consumer = classNameToHandlerMapping.get(zeroMQMessageData.getChannel());
                if (consumer != null) {
                    consumer.accept(serializer.deserialize(zeroMQMessageData.getMessage()));
                }
            } catch (InterruptedException e) {
                log.info("ZMQ receiver message handler interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("ZMQ receiver message handler task error", e);
            }
        }
        LinkedList<ZeroMQMessageData> remainingMessages = new LinkedList<>();
        messageQueue.drainTo(remainingMessages);
        if (!remainingMessages.isEmpty()) {
            log.info("Please wait to process {} remaining messages", remainingMessages.size());
            remainingMessages.forEach(zeroMQMessageData -> {
                try {
                    Consumer<IPropagatable> consumer = classNameToHandlerMapping.get(zeroMQMessageData.getChannel());
                    if (consumer != null) {
                        consumer.accept(serializer.deserialize(zeroMQMessageData.getMessage()));
                    }
                } catch (Exception e) {
                    log.error("ZMQ receiver message handler task error", e);
                }
            });
        }
    }

    @Override
    public int getQueueSize() {
        if (messageQueue != null) {
            return messageQueue.size();
        } else {
            return -1;
        }
    }

    @Override
    public void shutdown() {
        try {
            if (receiver != null) {
                log.info("Shutting down {}", this.getClass().getSimpleName());
                monitorThread.interrupt();
                monitorThread.join();
                receiverThread.interrupt();
                receiverThread.join();
                messagesQueueHandlerThread.interrupt();
                messagesQueueHandlerThread.join();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted shutdown ZeroMQ receiver");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void disconnect(String address) {
        receiver.disconnect(address);
    }

}

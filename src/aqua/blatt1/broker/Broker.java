package aqua.blatt1.broker;

import aqua.blatt1.common.Direction;
import aqua.blatt1.common.FishModel;
import aqua.blatt1.common.msgtypes.DeregisterRequest;
import aqua.blatt1.common.msgtypes.HandoffRequest;
import aqua.blatt1.common.msgtypes.RegisterRequest;
import aqua.blatt1.common.msgtypes.RegisterResponse;
import messaging.Endpoint;
import messaging.Message;

import javax.swing.*;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {

    private Endpoint endpoint = new Endpoint(4711);
    private ClientCollection clients = new ClientCollection();
    private ExecutorService executorService = Executors.newFixedThreadPool(16);
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean stopRequested = false;

    public static void main(String[] args) {
        new Broker().broker();
    }

    public void broker() {
        showStopDialog();
        while (!stopRequested) {
            executorService.submit(new BrokerTask(endpoint.blockingReceive()));
        }
    }

    private void showStopDialog() {
        final Thread stopThread = new Thread(() -> SwingUtilities.invokeLater(() -> {
            JOptionPane.showMessageDialog(null, "Press ok to stop the server");
            stopRequested = true;
        }));
        stopThread.run();
    }

    public void register(InetSocketAddress socketAddress) {
        lock.writeLock().lock();
        String clientId = "tank" + clients.size() + 1;
        clients.add(clientId, socketAddress);
        endpoint.send(socketAddress, new RegisterResponse(clientId));
        lock.writeLock().unlock();
    }

    public void deregister(InetSocketAddress socketAddress) {
        lock.writeLock().lock();
        clients.remove(clients.indexOf(socketAddress));
        lock.writeLock().unlock();
    }


    public void handoffFish(InetSocketAddress socketAddress, FishModel fish) {
        lock.readLock().lock();
        if (Direction.LEFT == fish.getDirection()) {
            InetSocketAddress leftNeighbourAddress = (InetSocketAddress) clients.getLeftNeighorOf(clients.indexOf(socketAddress));
            endpoint.send(leftNeighbourAddress, new HandoffRequest(fish));
        } else if (Direction.RIGHT == fish.getDirection()) {
            InetSocketAddress rightNeighbourAddress = (InetSocketAddress) clients.getRightNeighorOf(clients.indexOf(socketAddress));
            endpoint.send(rightNeighbourAddress, new HandoffRequest(fish));
        }
        lock.readLock().unlock();
    }

    private final class BrokerTask implements Runnable {

        private Message message;

        public BrokerTask(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            InetSocketAddress senderAddress = message.getSender();
            Serializable payload = message.getPayload();
            if (payload instanceof RegisterRequest) {
                register(senderAddress);
            } else if (payload instanceof DeregisterRequest) {
                deregister(senderAddress);
            } else if (payload instanceof HandoffRequest) {
                FishModel fish = ((HandoffRequest) payload).getFish();
                handoffFish(senderAddress, fish);
            }
        }
    }
}

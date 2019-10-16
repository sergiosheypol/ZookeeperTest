package es.upm.dit.cnvr.lab1;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.math.BigInteger;
import java.util.Random;
import java.util.Scanner;

public class CounterInterface {

    private ZooKeeper zk;
    private String counter = "/counter";

    private static final int SESSION_TIMEOUT = 5000;
    // This is static. A list of zookeeper can be provided for decide where to connect
    String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

    public CounterInterface() {
        // Select a random zookeeper server
        Random rand = new Random();
        int i = rand.nextInt(hosts.length);

        // Create a session and wait until it is created.
        // When is created, the watcher is notified
        try {
            if (zk == null) {
                zk = new ZooKeeper(hosts[i], SESSION_TIMEOUT, cWatcher);
                try {
                    // Wait for creating the session. Use the object lock
                    wait();
                    //zk.exists("/",false);
                } catch (Exception e) {
                    // TODO: handle exception
                }
            }
        } catch (Exception e) {
            System.out.println("Error");
        }
    }

    public void increment(int amount){

        try{
            // Init counter
            Stat counterStat = zk.exists(counter, watcherCounter);

            byte[] data = zk.getData(counter,watcherCounter, counterStat);
            int value = new BigInteger(data).intValue();

            value = value + amount;

            byte[] vByte = BigInteger.valueOf(value).toByteArray();

            int version = counterStat.getVersion();

            Stat r = zk.setData(counter, vByte, version);


        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

    }

    public void prompt() {
        System.out.println("How much do you want to increase the counter? If no value specified, +1");
    }

    // Notified when the session is created
    private Watcher cWatcher = new Watcher() {
        public void process (WatchedEvent e) {
            System.out.println("Created session");
            System.out.println(e.toString());
            notify();
        }
    };
    // Notified when the number of children in /member is updated
    private Watcher  watcherCounter = new Watcher() {
        public void process(WatchedEvent event) {
            System.out.println("------------------Watcher Counter------------------\n");
            try {
                System.out.println("        Incremented!!");
            } catch (Exception e) {
                System.out.println("Exception: watcherCounter");
            }
        }
    };

    public static void main(String[] args) {
        CounterInterface c = new CounterInterface();
        Scanner keyboard = new Scanner(System.in);

        while (true) {

            c.prompt();
            int input;

            try {
                input = keyboard.nextInt();
            } catch (Exception e) {
                System.out.println("Invalid input");
                keyboard = new Scanner(System.in);
                continue;
            }

            if (input == 0) {
                continue;
            }

            c.increment(input);

        }

    }

}

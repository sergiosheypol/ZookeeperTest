package es.upm.dit.cnvr.lab1;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper; 
import org.apache.zookeeper.data.Stat;
import java.math.BigInteger;


public class Counter implements Watcher{
	private static final int SESSION_TIMEOUT = 5000;

	private String counter = "/counter";

	// This is static. A list of zookeeper can be provided for decide where to connect
	String[] hosts = {"127.0.0.1:2181", "127.0.0.1:2181", "127.0.0.1:2181"};

	private ZooKeeper zk;
	
	public Counter() {

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

		// Add the process to the members in zookeeper

		if (zk != null) {
			// Create a folder for members and include this process/server
			try {

				// Init counter
				Stat counterStat = zk.exists(counter, watcherCounter);

				byte[] vByte = BigInteger.valueOf(0).toByteArray();

				if (counterStat == null) {
					String r = zk.create(counter, vByte, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println(r);
				}

				// Output
				byte[] data = zk.getData(counter,watcherCounter, counterStat);
				int value = new BigInteger(data).intValue();
				System.out.println("Value: " + value);


			} catch (KeeperException e) {
				System.out.println("The session with Zookeeper failes. Closing");
				return;
			} catch (InterruptedException e) {
				System.out.println("InterruptedException raised");
			}

		}
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
				System.out.println("        Update!!");
				Stat counterStat = zk.exists(counter, watcherCounter);
                byte[] data = zk.getData(counter,watcherCounter, counterStat);
                int value = new BigInteger(data).intValue();
                System.out.println("Value: " + value);

			} catch (Exception e) {
				System.out.println("Exception: watcherCounter");
			}
		}
	};
	
	@Override
	public void process(WatchedEvent event) {
		try {
			System.out.println("Unexpected invocated this method. Process of the object");
		} catch (Exception e) {
			System.out.println("Unexpected exception. Process of the object");
		}
	}
	
	private void printListMembers (List<String> list) {
		System.out.println("Remaining # members:" + list.size());
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.print(string + ", ");				
		}
		System.out.println();
	}
	
	public static void main(String[] args) {
		Counter zk = new Counter();
		
		try {
			Thread.sleep(300000); 			
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}

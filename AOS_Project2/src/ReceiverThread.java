import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;


public class ReceiverThread extends Thread{

	public Socket clientSocket;
	public ObjectInputStream in;
	public HashMap<String,String> hashMap = new HashMap<String,String>();
	//	private BufferedReader input;

	public ReceiverThread(Socket clientSocket) {
		this.clientSocket = clientSocket;
		try {
			System.out.println("client socket received by receiver thread : " + this.clientSocket);
			//			this.input = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
			this.in = new ObjectInputStream(this.clientSocket.getInputStream());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void run() {

		System.out.println("In receiver thread ... ");
		
		while(true) {
			
			try {
				Message msg = (Message) this.in.readObject();

				if(msg.getMsg_identifier().equals("TEST")) {

				} else if(msg.getMsg_identifier().equals("WRITE")) {
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_write(msg);
				} else if(msg.getMsg_identifier().equals("ACK")) {
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_ack(msg);
				}

			} catch (ClassNotFoundException e) {
				 e.printStackTrace();
			} catch (IOException e) {
				 // e.printStackTrace();
			}
		}
	}

	public void receive_ack(Message object) {
		
		try {					
				// Received ack from primary or secondary, it writes the object to server's file & removes if any hashmap entry exists for that object
				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");
				
				if(hashMap.containsKey(object.getKey())) {
					hashMap.remove(object.getKey());
				}
				
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void receive_write(Message object) {

		try {			
			int hashCode = object.getKey().hashCode();
			int hashValue = hashCode%Node.num_of_servers;
			System.out.println("hashCode : "+hashCode + " hashValue : " + hashValue);

			if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[hashValue])) {
				// Writes the object to file & sends ack to other two replicas
				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");

				Socket socket = Node.serverSocketsArray.get(Node.config.hostnames[(hashValue+1)%Node.num_of_servers]);
				System.out.println("secondary socket : " + socket);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				Message objectAck = object;
				objectAck.setMsg_identifier("ACK");
				oos.writeObject(objectAck);

				Socket socket2 = Node.serverSocketsArray.get(Node.config.hostnames[(hashValue+2)%Node.num_of_servers]);
				System.out.println("secondary socket : " + socket2);
				ObjectOutputStream oos2 = new ObjectOutputStream(socket2.getOutputStream());
				oos2.writeObject(objectAck);

				//						processDataVersion(hashValue, object);

			} else if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[(hashValue+1)%Node.num_of_servers])) {				
				Socket socket = Node.serverSocketsArray.get(Node.config.hostnames[hashValue]);
				ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
				// If it is connected to primary server
				if(Node.isConnected(oos)) {
					hashMap.put(object.getKey(), object.getValue());
					System.out.println("put in hashmap ... ");
				} else {											// if not connected to primary, it acts as primary temporarily & sends ack to third replica server
					Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");
					socket = Node.serverSocketsArray.get(Node.config.hostnames[(hashValue+2)%Node.num_of_servers]);
					oos = new ObjectOutputStream(socket.getOutputStream());
					Message objectAck = object;
					objectAck.setMsg_identifier("ACK");
					oos.writeObject(objectAck);
				}					
			} else if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[(hashValue+2)%Node.num_of_servers])) {
				
				// puts the object in hash map & Waits for object ack from either primary or secondary 
				hashMap.put(object.getKey(), object.getValue());
				System.out.println("put in hashmap ... ");

			} else {
				System.out.println("Invalid hashValue of the object sent to this serve ... ");
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void processDataVersion(int firstReplicaHashValue, Message object) {

		try {
			int localsourceNodeNum = object.getVectorClock().getNode();
			int localObjectDataVersion = object.getVectorClock().getCounter();
			int version = checkDataVersionAtFirstReplica(firstReplicaHashValue, localsourceNodeNum, localObjectDataVersion);

			if(version == 0 || version == 1) {
				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// Return 0 if object versions are same; 
	// Return -1 if local object version is less than that at first replica
	// Return 1 if local object version is greater than that at first replica
	public int checkDataVersionAtFirstReplica(int firstReplicaHashValue, int localsourceNodeNum, int localObjectDataVersion) {

//		Socket firstReplicasocket = null;		
//		int remoteSourceNodeNum	= 0;
		int remoteObjectDataVersion = 0;

		// Logic to extract data from firstReplica server's file
		//		String firstReplicaHostName = "net0"+firstReplicaHashValue+".utdallas.edu";

		if(localObjectDataVersion < remoteObjectDataVersion) {
			return -1;
		} else if(localObjectDataVersion > remoteObjectDataVersion) {
			return 1;
		} else if(localObjectDataVersion == remoteObjectDataVersion) {
			return 0;
		}

		return -1;
	}




	//	public synchronized void run() {
	//
	//		//				System.out.println("ReceiverThread " + Thread.currentThread().getName());
	//		try {
	//			if(this.clientSocket != null) {
	//				while(true) {					
	//					String read = this.input.readLine();		
	//					if(read != null){
	//						System.out.println("\nMessage Received : " + read.toString() + "\n");
	//
	//						if(read.contains("WRITE")) {																
	//							receive_write(read);
	//						}							
	//					}	
	//				}					
	//			}	
	//		} catch (NumberFormatException | IOException e) {
	//			e.printStackTrace();
	//		} 
	//	}







	public void forward_object(String[] tokens, int dest_node_num) {

		try {

			PrintWriter output = null;
			Iterator<Entry<String, Socket>> iter = Node.clientSocketsArray.entrySet().iterator();
			Entry<String, Socket> clientSocket;
			Socket serverSocketForRespectiveClient = null;
			int index = 0;

			while(iter.hasNext()) {

				clientSocket = iter.next();

				index = Integer.valueOf(clientSocket.getValue().getInetAddress().getHostName().toString().substring(3,5))-1;

				if(index == dest_node_num) {
					StringBuffer buffer = new StringBuffer();
					buffer.append(tokens[0]);

					serverSocketForRespectiveClient = Node.serverSocketsArray.get(clientSocket.getKey().toString());

					output = new PrintWriter(serverSocketForRespectiveClient.getOutputStream(), true);

					output.println(buffer.toString());
					System.out.println("\nMessage Sent : " + buffer.toString() +"\n");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}




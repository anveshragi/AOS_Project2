import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
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
	public ObjectInputStream in = null;
	public HashMap<String,String> hashMap = new HashMap<String,String>();
	private Socket userServerSocket;
	//	private BufferedReader input;

	public ReceiverThread(Socket socket) {
		this.clientSocket = socket;

		try {
			//			this.input = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
			in = new ObjectInputStream(clientSocket.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {

		while(true) {

			try {
				Message msg = (Message) this.in.readObject();

				if(msg.getMsg_identifier().equals("TEST")) {

				} else if(msg.getMsg_identifier().equals("INSERT")) {
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_write(msg);
				} else if(msg.getMsg_identifier().equals("INSERTACK")) {
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_insertack(msg);
				} else if(msg.getMsg_identifier().equals("UPDATE")) {
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_update(msg);
				} else if(msg.getMsg_identifier().equals("UPDATEACK")) {
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_updateack(msg);
				} else if(msg.getMsg_identifier().equals("READ")) {
//					System.out.println("Received read form client: "+ this.clientSocket.getInetAddress().getHostName()+"  "+this.clientSocket.getPort()+" "+this.clientSocket.getLocalPort());
					System.out.println("message received : " + msg.getMsg_identifier() + " "+ msg.getKey() + " "+ msg.getValue() + " "+ msg.getVectorClock().getNode() + " "+ msg.getVectorClock().getCounter());
					receive_read(msg);
				}

			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// e.printStackTrace();
			}
		}
	}
	public void receive_read(Message object){
		String value = readFromServersFile(object.getKey());
		if(value != null){
			try {
				int portno=0;
				for(int i = 0 ;i<Node.config.nodeidentifiers.length;i++){
					if(Node.config.hostnames[i].equals(this.clientSocket.getInetAddress().getHostName())){
						portno = Node.config.portnumbers[i];
					}
				}
				ObjectOutputStream oosUser = null;
				userServerSocket = new Socket(this.clientSocket.getInetAddress().getHostName(), portno);
				
				oosUser = new ObjectOutputStream(userServerSocket.getOutputStream());
				
				// Preparing readReply message for the user server
				Message readReply = new Message();
				readReply.setMsg_identifier("ReadReply");
				readReply.setKey(object.getKey());
				readReply.setValue(value);
				
				readReply.setVectorClock(object.getVectorClock());
				
				// Writing the message on to the output stream of user server
				oosUser.writeObject(readReply);
				System.out.println("Message written: "+readReply.getKey() + "  "+ readReply.getValue());
				
				// Close connections when no longer needed
				//oosUser.close();
				//userServerSocket.close();
				
			} catch (IOException e) {                          //exception handled when connecting to user server socket
				e.printStackTrace();
			}
		}else{
			System.out.println("\n Key not found");
			
		}
	}
	public void receive_insertack(Message object) {

		// Received ack from primary or secondary, it writes the object to server's file & removes if any hashmap entry exists for that object
		writeToServersFile(object);

		//				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");

		if(hashMap.containsKey(object.getKey())) {
			hashMap.remove(object.getKey());
		}
	}

	public void receive_updateack(Message object) {

		// Received ack from primary or secondary, it updates the object to server's file & removes if any hashmap entry exists for that object
		updateToServersFile(object);

		//				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");

		if(hashMap.containsKey(object.getKey())) {
			hashMap.remove(object.getKey());
		}
	}
	
	public void receive_write(Message object) {

		try {			
			
			if(checkObjectInFile(object)) {
				
				int portno=0;
				for(int i = 0 ;i<Node.config.nodeidentifiers.length;i++){
					if(Node.config.hostnames[i].equals(this.clientSocket.getInetAddress().getHostName())){
						portno = Node.config.portnumbers[i];
					}
				}
				ObjectOutputStream oosUser = null;
				userServerSocket = new Socket(this.clientSocket.getInetAddress().getHostName(), portno);
				
				oosUser = new ObjectOutputStream(userServerSocket.getOutputStream());
				
				// Preparing readReply message for the user server
				Message keyExists = new Message();
				keyExists.setMsg_identifier("keyExists");
				keyExists.setKey(object.getKey());
				keyExists.setValue(object.getValue());				
				keyExists.setVectorClock(object.getVectorClock());
				
				// Writing the message on to the output stream of user server
				oosUser.writeObject(keyExists);
				
				receive_update(object);
				
			} else {
			
			ObjectOutputStream oos = null;
			Message objectAck = null;

			int hashCode = object.getKey().hashCode();
			int hashValue = Math.abs(hashCode%Node.num_of_servers);
			System.out.println("hashCode : "+hashCode + " hashValue : " + hashValue);

			if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[hashValue])) {

				// Writes the object to file & sends ack to other two replicas
				writeToServersFile(object);

				//				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");

				oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[(hashValue+1)%Node.num_of_servers]);
				objectAck = object;
				objectAck.setMsg_identifier("INSERTACK");
				if(Node.isConnected(oos)) {
				oos.writeObject(objectAck);
				}

				oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[(hashValue+2)%Node.num_of_servers]);
				if(Node.isConnected(oos)) {
				oos.writeObject(objectAck);
				}

				//						processDataVersion(hashValue, object);

			} else if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[(hashValue+1)%Node.num_of_servers])) {				

				oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[hashValue]);

				// If it is connected to primary server
				if(Node.isConnected(oos)) {

					hashMap.put(object.getKey(), object.getValue());
					System.out.println("put in hashmap ... ");

				} else {											// if not connected to primary, it acts as primary temporarily & sends ack to third replica server

					writeToServersFile(object);
					//					Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");

					oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[(hashValue+2)%Node.num_of_servers]);
					objectAck = object;
					objectAck.setMsg_identifier("INSERTACK");
					if(Node.isConnected(oos)) {
					oos.writeObject(objectAck);
					}
				}					
			} else if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[(hashValue+2)%Node.num_of_servers])) {

				// puts the object in hash map & Waits for object ack from either primary or secondary 
				hashMap.put(object.getKey(), object.getValue());
				System.out.println("put in hashmap ... ");

			} else {
				System.out.println("Invalid hashValue of the object sent to this server ... ");
			}	

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void receive_update(Message object) {

		try {			
			ObjectOutputStream oos = null;
			Message objectAck = null;

			int hashCode = object.getKey().hashCode();
			int hashValue = Math.abs(hashCode%Node.num_of_servers);
			System.out.println("hashCode : "+hashCode + " hashValue : " + hashValue);

			if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[hashValue])) {

				// Updates the object to file & sends ack to other two replicas
				updateToServersFile(object);

				oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[(hashValue+1)%Node.num_of_servers]);
				objectAck = object;
				objectAck.setMsg_identifier("UPDATEACK");
				if(Node.isConnected(oos)) {
				oos.writeObject(objectAck);
				}

				if(Node.isConnected(oos)) {
				oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[(hashValue+2)%Node.num_of_servers]);
				oos.writeObject(objectAck);
				}
				//						processDataVersion(hashValue, object);

			} else if(InetAddress.getLocalHost().getHostName().toString().equals(Node.config.hostnames[(hashValue+1)%Node.num_of_servers])) {				

				oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[hashValue]);

				// If it is connected to primary server
				if(Node.isConnected(oos)) {

					hashMap.put(object.getKey(), object.getValue());
					System.out.println("put in hashmap ... ");

				} else {											// if not connected to primary, it acts as primary temporarily & sends ack to third replica server

					updateToServersFile(object);
					//					Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");

					oos = Node.outputStreamsOfserverSocketsForClients.get(Node.config.hostnames[(hashValue+2)%Node.num_of_servers]);
					objectAck = object;
					objectAck.setMsg_identifier("UPDATEACK");
					if(Node.isConnected(oos)) {
					oos.writeObject(objectAck);
					}
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

	public synchronized boolean checkObjectInFile(Message object) {
		
		try {
			String filename = "File" + Node.node_num + ".txt";			
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new FileReader(filename));

			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split(" ");

				if(tokens[0].equals(object.getKey())) {
					return true;
				}
			}
			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return false;
	}

	public synchronized static void writeToServersFile(Message object) {

		try {
			String filename = "File" + Node.node_num + ".txt";						
			BufferedWriter bw = new BufferedWriter(new FileWriter(filename,true));

			bw.write(object.getKey() + " " + object.getValue() + "\n");

			bw.close();

			System.out.println("Time when object written to file " + System.currentTimeMillis());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String readFromServersFile(String ObjectKey) {

		try {
			String filename = "File" + Node.node_num + ".txt";			
			@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new FileReader(filename));

			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] tokens = line.split(" ");

				if(tokens[0].equals(ObjectKey)) {
					return tokens[1];
				}
			}
			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public synchronized static void updateToServersFile(Message object) {

		try {
			String oldfilename = "File" + Node.node_num + ".txt";
			String newfilename = "temp" + Node.node_num + ".txt";

			BufferedReader reader = new BufferedReader(new FileReader(oldfilename));
			BufferedWriter bw = new BufferedWriter(new FileWriter(newfilename,true));
//			String newline= System.getProperty("line.separator");
			String line = null;
			while ((line = reader.readLine()) != null) {
				
				String[] tokens = line.split(" ");
                String temp = null;
				if(tokens[0].equals(object.getKey())) {
					System.out.println("Update function: String replaced: "+ tokens[1]+" with: "+ object.getValue());
					temp = line.replace(tokens[1], object.getValue());
					System.out.println("new string is :" + temp);
				}
				if(temp != null) {
					bw.write(temp);
					bw.newLine();
				}
				else {
					bw.write(line);
					bw.newLine();
				}					
			}

			reader.close();
			bw.close();

			File oldfile = new File(oldfilename);
			oldfile.delete();

			File newfile = new File(newfilename);
			newfile.renameTo(oldfile);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void processDataVersion(int firstReplicaHashValue, Message object) {

		int localsourceNodeNum = object.getVectorClock().getNode();
		int localObjectDataVersion = object.getVectorClock().getCounter();
		int version = checkDataVersionAtFirstReplica(firstReplicaHashValue, localsourceNodeNum, localObjectDataVersion);

		if(version == 0 || version == 1) {
			writeToServersFile(object);
			//				Node.bw.write(object.getKey() + " " + object.getValue() + " " + object.getVectorClock().getNode() + object.getVectorClock().getCounter() + "\n");
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




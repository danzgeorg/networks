import java.net.*;
import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  Daniel Georgiev
//  240015628
//  dan.georgiev:@city.ac.uk


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {
	
	private String nodeName;
	private DatagramSocket socket;
	private HashMap<Integer, ArrayList<String[]>> addressStore;
	private String ipAddress;
	private int port;
	private HashMap<String, String> dataStore;
	private ConcurrentHashMap<String, PendingRequest> pendingRequests;
	private ConcurrentHashMap<String, byte[]> recentResponses;
	private ConcurrentHashMap<String, byte[]> receivedResponses;
	private ConcurrentHashMap<String, String[]> relayMap;

    public void setNodeName(String nodeName) throws Exception {
	this.nodeName = nodeName;
	addressStore = new HashMap<>();
	dataStore =  new HashMap<>();
	pendingRequests = new ConcurrentHashMap<>();
	recentResponses = new ConcurrentHashMap<>();
	receivedResponses = new ConcurrentHashMap<>();
	relayMap = new ConcurrentHashMap<>();
    }

    public void openPort(int portNumber) throws Exception {
    socket = new DatagramSocket(portNumber);
    this.port = portNumber;
    this.ipAddress = InetAddress.getLocalHost().getHostAddress();
    storeAddress(nodeName, ipAddress + ":" + portNumber);
    }

    public void handleIncomingMessages(int delay) throws Exception {
    	long startTime = System.currentTimeMillis();
    	socket.setSoTimeout(1000);
    	while (true) {
    		if (delay > 0) {
    			long elapsed = System.currentTimeMillis() - startTime;
    			if (elapsed >= delay) break;
    		}
    		try {
    			byte[] buf = new byte[65536];
    			DatagramPacket packet = new DatagramPacket(buf, buf.length);
    			socket.receive(packet);
    			byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
    			handlePacket(data, packet.getAddress(), packet.getPort());
    		} catch (java.net.SocketTimeoutException e) {
    			checkRetransmissions();
    		}
    	}
    }
    
    private void handlePacket(byte[] data, InetAddress address, int port) {
    try {
        if (data.length < 4) return;
        String duplicateKey = data[0] + ":" + data[1] + ":" + address + ":" + port;
        if (recentResponses.containsKey(duplicateKey)) {
        	socket.send(new DatagramPacket(recentResponses.get(duplicateKey), 
        		recentResponses.get(duplicateKey).length, address, port));
        	return;
        }
        byte[] txID = new byte[]{data[0], data[1]};
        if (data[2] != 0x20) return;
        char msgType = (char) data[3];
        // If this looks like a response, store it for waitForResponse
        if (msgType == 'H' || msgType == 'O' || msgType == 'S' || 
        	msgType == 'F' || msgType == 'X' || msgType == 'A' || msgType == 'R' || msgType == 'D') {
        String responseKey = (data[0] & 0xFF) + ":" + (data[1] & 0xFF);
        // Check if this is a relay response
        if (relayMap.containsKey(responseKey)) {
        	String[] original = relayMap.remove(responseKey);
        	InetAddress originalAddr = InetAddress.getByName(original[0]);
        	int originalPort = Integer.parseInt(original[1]);
        	byte[] rewritten = Arrays.copyOf(data, data.length);
        	rewritten[0] = (byte) Integer.parseInt(original[2]);
        	rewritten[1] = (byte) Integer.parseInt(original[3]);
        	socket.send(new DatagramPacket(rewritten, rewritten.length, originalAddr, originalPort));
        	return;
        }
        receivedResponses.put(responseKey, data);
        return;
    }
        switch (msgType) {
            case 'G':
                handleG(txID, address, port);
                break;
            case 'N':
            	handleN(txID, data, address, port);
            	break;
            case 'E':
            	handleE(txID, data, address, port);
            	break;
            case 'R':
            	handleR(txID, data, address, port);
            	break;
            case 'W':
            	handleW(txID, data, address, port);
            	break;
            case 'V':
            	handleV(txID, data, address, port);
            	break;
            case 'C':
            	handleC(txID, data, address, port);
            	break;
            default:
                // Unknown message type, ignore
                break;
        	}
    	} catch (Exception e) {
        // Log and discard malformed messages
        System.err.println("Error handling packet: " + e.getMessage());
    	}
    }
    
    private void handleG(byte[] txID, InetAddress address, int port) throws Exception {
    byte[] encoded = encodeCRN(nodeName).getBytes(StandardCharsets.UTF_8);
    byte[] response = new byte[4 + encoded.length];
    response[0] = txID[0];
    response[1] = txID[1];
    response[2] = 0x20;
    response[3] = 'H';
    System.arraycopy(encoded, 0, response, 4, encoded.length);
    DatagramPacket packet = new DatagramPacket(response, response.length, address, port);
    socket.send(packet);
    }
    
    private void handleN(byte[] txID, byte[] data, InetAddress address, int port) throws Exception {
    	if (data.length < 36) return;
    	if (data.length < 4 + 32) return;
    	byte[] targetHash = Arrays.copyOfRange(data, 4, 36);
    	List<String[]> closest = getClosestAddresses(targetHash, 3);
    	StringBuilder sb = new StringBuilder();
    	for (String[] pair : closest) {
    		sb.append(encodeCRN(pair[0]));
    		sb.append(encodeCRN(pair[1]));
    		}
    		byte[] encoded = sb.toString().getBytes(StandardCharsets.UTF_8);
    		byte[] response = new byte[4 + encoded.length];
    		response[0] = txID[0];
    		response[1] = txID[1];
    		response[2] = 0x20;
    		response[3] = 'O';
    		System.arraycopy(encoded, 0, response, 4, encoded.length);
    		DatagramPacket packet = new DatagramPacket(response, response.length, address, port);
    		socket.send(packet);
    	}
    
    private void handleV(byte[] txID, byte[] data, InetAddress address, int port) throws Exception {
    	if (data.length < 6) return;
    	// Decode the target node name
    	String targetName = decodeCRN(data, 4);
    	int offset = 4 + encodeCRN(targetName).getBytes(StandardCharsets.UTF_8).length;
    
    	// Get the embedded message
    	byte[] embedded = Arrays.copyOfRange(data, offset, data.length);
    
    	// Look up the target node's address
    	List<String[]> closest = getClosestAddresses(HashID.computeHashID(targetName), 1);
    	if (closest.isEmpty()) return;
    
    	String[] parts = closest.get(0)[1].split(":");
    	InetAddress targetAddr = InetAddress.getByName(parts[0]);
    	int targetPort = Integer.parseInt(parts[1]);
    
    	// Generate a new txID for the forwarded message
    	byte[] newTxID = generateTxID();
    
    	// Rewrite the txID in the embedded message
    	byte[] forwarded = Arrays.copyOf(embedded, embedded.length);
    	forwarded[0] = newTxID[0];
    	forwarded[1] = newTxID[1];
    
    	// Store the mapping: newTxID -> original sender info
    	String newKey = (newTxID[0] & 0xFF) + ":" + (newTxID[1] & 0xFF);
    	relayMap.put(newKey, new String[]{
    			address.getHostAddress(),
    			String.valueOf(port),
    			String.valueOf(txID[0] & 0xFF),
    			String.valueOf(txID[1] & 0xFF)
    	});
    
    	// Forward the message
    	sendRequest(newTxID, forwarded, targetAddr, targetPort);
    }
    
    private void handleE(byte[] txID, byte[] data, InetAddress address, int port) throws Exception {
    	if (data.length < 5) return;
    	String key = decodeCRN(data, 4);
    	byte response_code;
    	if (conditionA(key)) {
    		response_code = 'Y';
    		} else if (conditionB(key)) {
    			response_code = 'N';
    		} else {
    			response_code = '?';
    		}
    		byte[] response = new byte[4];
    		response[0] = txID[0];
    		response[1] = txID[1];
    		response[2] = 0x20;
    		response[3] = response_code;
    		DatagramPacket packet = new DatagramPacket(response, response.length, address, port);
    		socket.send(packet);
    }
    
    private void handleR(byte[] txID, byte[] data, InetAddress address, int port) throws Exception {
    	if (data.length < 5) return;
    	String key = decodeCRN(data, 4);
    	byte[] response;
    	if (conditionA(key)) {
    		String value = dataStore.get(key);
    		byte[] encoded = encodeCRN(value).getBytes(StandardCharsets.UTF_8);
    		response = new byte[4 + encoded.length];
    		response[0] = txID[0];
    		response[1] = txID[1];
    		response[2] = 0x20;
    		response[3] = 'S';
    		response[4] = 'Y';
    		System.arraycopy(encoded, 0, response, 5, encoded.length);
    		} else {
    			response = new byte[5];
    			response[0] = txID[0];
    			response[1] = txID[1];
    			response[2] = 0x20;
    			response[3] = 'S';
    			response[4] = (byte)(conditionB(key) ? 'N' : '?');
    		}
    		DatagramPacket packet = new DatagramPacket(response, response.length, address, port);
   			socket.send(packet);
    	}
    
    private void handleW(byte[] txID, byte[] data, InetAddress address, int port) throws Exception {
    	if (data.length < 6) return;
    	int offset = 4;
    	String key = decodeCRN(data, offset);
    	offset += encodeCRN(key).getBytes(StandardCharsets.UTF_8).length;
    	String value = decodeCRN(data, offset);
    	byte response_code;
    	if (conditionA(key)) {
    		dataStore.put(key, value);
    		response_code = 'R';
    		} else if (conditionB(key)) {
    			dataStore.put(key, value);
    			response_code = 'A';
    		} else {
    			response_code = 'X';
    		}
    		byte[] response = new byte[4];
    		response[0] = txID[0];
    		response[1] = txID[1];
    		response[2] = 0x20;
    		response[3] = response_code;
    		DatagramPacket packet = new DatagramPacket(response, response.length, address, port);
    		socket.send(packet);
    	}
    
    private synchronized void handleC(byte[] txID, byte[] data, InetAddress address, int port) throws Exception {
    	if (data.length < 7) return;
    	int offset = 4;
    	String key = decodeCRN(data, offset);
    	offset += encodeCRN(key).getBytes(StandardCharsets.UTF_8).length;
    	String requestedValue = decodeCRN(data, offset);
    	offset += encodeCRN(requestedValue).getBytes(StandardCharsets.UTF_8).length;
    	String newValue = decodeCRN(data, offset);
    	byte response_code;
    	if (conditionA(key)) {
    		String current = dataStore.get(key);
    		if (current.equals(requestedValue)) {
    			dataStore.put(key, newValue);
    			response_code = 'R';
    		} else {
    			response_code = 'N';
    		}
    	} else if (conditionB(key)) {
    		dataStore.put(key, newValue);
    		response_code = 'A';
    	} else {
    		response_code = 'X';
    	}
    	byte[] response = new byte[4];
    	response[0] = txID[0];
    	response[1] = txID[1];
    	response[2] = 0x20;
    	response[3] = response_code;
    	socket.send(new DatagramPacket(response, response.length, address, port));
    }
    
    public boolean isActive(String nodeName) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public void pushRelay(String nodeName) throws Exception {
	throw new Exception("Not implemented");
    }

    public void popRelay() throws Exception {
        throw new Exception("Not implemented");
    }

    public boolean exists(String key) throws Exception {
	throw new Exception("Not implemented");
    }
    
    public String read(String key) throws Exception {
    	byte[] keyHash = HashID.computeHashID(key);
    	List<String[]> closest = getClosestAddresses(keyHash, 3);
    	if (conditionA(key)) {
    		return dataStore.get(key);
    	}
    	for (String[] pair : closest) {
    		String[] parts = pair[1].split(":");
    		InetAddress addr = InetAddress.getByName(parts[0]);
    		int destPort = Integer.parseInt(parts[1]);
    		byte[] txID = generateTxID();
    		byte[] encoded = encodeCRN(key).getBytes(StandardCharsets.UTF_8);
    		byte[] request = new byte[4 + encoded.length];
    		request[0] = txID[0];
    		request[1] = txID[1];
    		request[2] = 0x20;
    		request[3] = 'R';
    		System.arraycopy(encoded, 0, request, 4, encoded.length);
    		sendRequest(txID, request, addr, destPort);
    		byte[] rdata = waitForResponse(txID, 5000);
    		if (rdata != null && rdata[3] == 'S' && rdata[4] == 'Y') {
    			return decodeCRN(rdata, 5);
    		}
    	}
    	return null;
    }

    public boolean write(String key, String value) throws Exception {
    	byte[] keyHash = HashID.computeHashID(key);
    	List<String[]> closest = getClosestAddresses(keyHash, 3);
    	boolean success = false;
    	if (conditionB(key)) {
    		dataStore.put(key, value);
    		return true;
    	}
    	for (String[] pair : closest) {
    		String[] parts = pair[1].split(":");
    		InetAddress addr = InetAddress.getByName(parts[0]);
    		int destPort = Integer.parseInt(parts[1]);
    		byte[] txID = generateTxID();
    		byte[] encodedKey = encodeCRN(key).getBytes(StandardCharsets.UTF_8);
    		byte[] encodedValue = encodeCRN(value).getBytes(StandardCharsets.UTF_8);
    		byte[] request = new byte[4 + encodedKey.length + encodedValue.length];
    		request[0] = txID[0];
    		request[1] = txID[1];
    		request[2] = 0x20;
    		request[3] = 'W';
    		System.arraycopy(encodedKey, 0, request, 4, encodedKey.length);
    		System.arraycopy(encodedValue, 0, request, 4 + encodedKey.length, encodedValue.length);
    		sendRequest(txID, request, addr, destPort);
    		byte[] buf = new byte[65536];
    		DatagramPacket response = new DatagramPacket(buf, buf.length);
    		socket.setSoTimeout(5000);
    		try {
    			socket.receive(response);
    			byte[] rdata = Arrays.copyOf(response.getData(), response.getLength());
    			if (rdata[3] == 'A' || rdata[3] == 'R') {
    				success = true;
    			}
    		} catch (java.net.SocketTimeoutException e) {
    			// Try next node
    		}
	}
	return success;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
    	byte[] keyHash = HashID.computeHashID(key);
    	List<String[]> closest = getClosestAddresses(keyHash, 3);
    	for (String[] pair : closest) {
    		String[] parts = pair[1].split(":");
    		InetAddress addr = InetAddress.getByName(parts[0]);
    		int destPort = Integer.parseInt(parts[1]);
    		byte[] txID = generateTxID();
    		byte[] encodedKey = encodeCRN(key).getBytes(StandardCharsets.UTF_8);
    		byte[] encodedCurrent = encodeCRN(currentValue).getBytes(StandardCharsets.UTF_8);
    		byte[] encodedNew = encodeCRN(newValue).getBytes(StandardCharsets.UTF_8);
    		byte[] request = new byte[4 + encodedKey.length + encodedCurrent.length + encodedNew.length];
    		request[0] = txID[0];
    		request[1] = txID[1];
    		request[2] = 0x20;
    		request[3] = 'C';
    		System.arraycopy(encodedKey, 0, request, 4, encodedKey.length);
    		System.arraycopy(encodedCurrent, 0, request, 4 + encodedKey.length, encodedCurrent.length);
    		System.arraycopy(encodedNew, 0, request, 4 + encodedKey.length + encodedCurrent.length, encodedNew.length);
    		sendRequest(txID, request, addr, destPort);
    		byte[] rdata = waitForResponse(txID, 5000);
    		if (rdata != null && (rdata[3] == 'R' || rdata[3] == 'A')) {
    			return true;
    		}
    	}
    	return false;
    }
    
    private String encodeCRN(String s) {
    int spaces = 0;
    for (int i = 0; i < s.length(); i++) {
        if (s.charAt(i) == ' ') {
            spaces++;
        	}
    	}
    return spaces + " " + s + " ";
    }
    
    private String decodeCRN(byte[] data, int offset) throws Exception {
    	if (offset >= data.length) throw new Exception("Invalid CRN: offset out of bounds");
    	int i = offset;
    	while (i < data.length && data[i] != 0x20) {
    		i++;
    	}
    	if (i >= data.length) throw new Exception("Invalid CRN: no space after count");
    	i++; // skip the space after the number
    	if (i > data.length) throw new Exception("Invalid CRN: no content after separator");
    	int start = i;
    	while (i < data.length && data[i] != 0x20) {
    		i++;
    	}
    	if (i > data.length) throw new Exception("Invalid CRN: no trailing space");
    	return new String(data, start, i - start, "UTF-8");
    }
    
    private int computeDistance(byte[] hash1, byte[] hash2) {
    	int matchingBits = 0;
    	for (int i = 0; i < hash1.length; i++) {
    		int xor = (hash1[i] & 0xFF) ^ (hash2[i] & 0xFF);
    		if (xor == 0) {
    			matchingBits += 8;
    		} else {
    			int mask = 0x80;
    			while (mask > 0 && (xor & mask) == 0) {
    				matchingBits++;
    				mask >>= 1;
    			}
    			break;
    		}
    	}
    	return 256 - matchingBits;
    }
    
    public void storeAddress(String name, String address) throws Exception {
    byte[] hash1 = HashID.computeHashID(name);
    byte[] hash2 = HashID.computeHashID(nodeName);
    int distance = computeDistance(hash1, hash2);
    ArrayList<String[]> list = addressStore.getOrDefault(distance, new ArrayList<>());
    for (String[] pair : list) {
        if (pair[0].equals(name)) {
            pair[1] = address;
            addressStore.put(distance, list);
            return;
        	}
    	}
    if (list.size() < 3) {
        list.add(new String[]{name, address});
        addressStore.put(distance, list);
    	}
    }
    
    private List<String[]> getClosestAddresses(byte[] targetHash, int max) throws Exception {
    List<String[]> all = new ArrayList<>();
    for (ArrayList<String[]> list : addressStore.values()) {
        for (String[] pair : list) {
            all.add(pair);
        	}
    	}
    all.sort((a, b) -> {
        try {
            byte[] hashA = HashID.computeHashID(a[0]);
            byte[] hashB = HashID.computeHashID(b[0]);
            return computeDistance(hashA, targetHash) - computeDistance(hashB, targetHash);
        	} catch (Exception e) {
            return 0;
        	}
    	});
    return all.subList(0, Math.min(max, all.size()));
    }
  
    private boolean conditionA(String key) {
    return dataStore.containsKey(key);
    }

	private boolean conditionB(String key) throws Exception {
		byte[] keyHash = HashID.computeHashID(key);
		byte[] myHash = HashID.computeHashID(nodeName);
		int myDistance = computeDistance(keyHash, myHash);
		List<String[]> closest = getClosestAddresses(keyHash, 3);
		for (String[] pair : closest) {
			byte[] pairHash = HashID.computeHashID(pair[0]);
			int pairDistance = computeDistance(pairHash, keyHash);
			if (pairDistance < myDistance) {
            return false;
            }
        }
        return true;
    }
    
    private byte[] generateTxID() {
    byte[] txID = new byte[2];
    Random rand = new Random();
    do { txID[0] = (byte) rand.nextInt(256); } while (txID[0] == 0x20);
    do { txID[1] = (byte) rand.nextInt(256); } while (txID[1] == 0x20);
    return txID;
    }
    
    private void sendRequest(byte[] txID, byte[] message, InetAddress address, int port) throws Exception {
    	String key = txID[0] + ":" + txID[1] + ":" + address + ":" + port;
    	pendingRequests.put(key, new PendingRequest(txID, message, address, port));
    	socket.send(new DatagramPacket(message, message.length, address, port));
    }
    
    private void checkRetransmissions() throws Exception {
    	// Clean up old entries to prevent memory exhaustion
    	if (recentResponses.size() > 1000) {
    		recentResponses.clear();
    	}
    	if (relayMap.size() > 1000) {
    		relayMap.clear();
    	}
    	long now = System.currentTimeMillis();
    	for (Map.Entry<String, PendingRequest> entry : pendingRequests.entrySet()) {
    		PendingRequest pr = entry.getValue();
    		if (now - pr.sentTime >= 5000) {
    			if (pr.retries >= 3) {
    				pendingRequests.remove(entry.getKey());
    			} else {
    				pr.retries++;
    				pr.sentTime = now;
    				socket.send(new DatagramPacket(pr.message, pr.message.length, pr.address, pr.port));
    			}
    		}
    	}
    }
    
    private void removePending(byte[] txID, InetAddress address, int port) {
    	String key = txID[0] + ":" + txID[1] + ":" + address + ":" + port;
    	pendingRequests.remove(key);
    }
    
    private byte[] waitForResponse(byte[] txID, int timeoutMs) throws Exception {
    	String key = (txID[0] & 0xFF) + ":" + (txID[1] & 0xFF);
    	long start = System.currentTimeMillis();
    	while (System.currentTimeMillis() - start < timeoutMs) {
    		if (receivedResponses.containsKey(key)) {
    			return receivedResponses.remove(key);
    		}
    		byte[] buf = new byte[65536];
    		DatagramPacket packet = new DatagramPacket(buf, buf.length);
    		socket.setSoTimeout(100);
    		try {
    			socket.receive(packet);
    			byte[] data = Arrays.copyOf(packet.getData(), packet.getLength());
    			handlePacket(data, packet.getAddress(), packet.getPort());
    		} catch (java.net.SocketTimeoutException e) {
    			checkRetransmissions();
    		}
    	}
    	return null;
    }
    
}

class PendingRequest {
    byte[] txID;
    byte[] message;
    InetAddress address;
    int port;
    long sentTime;
    int retries;

    PendingRequest(byte[] txID, byte[] message, InetAddress address, int port) {
        this.txID = txID;
        this.message = message;
        this.address = address;
        this.port = port;
        this.sentTime = System.currentTimeMillis();
        this.retries = 0;
    }
}

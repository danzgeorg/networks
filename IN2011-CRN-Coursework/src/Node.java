import java.net.*;
import java.io.*;
import java.util.*;
import java.nio.charset.StandardCharsets;

// IN2011 Computer Networks
// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


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

    public void setNodeName(String nodeName) throws Exception {
	this.nodeName = nodeName;
    }

    public void openPort(int portNumber) throws Exception {
	socket = new DatagramSocket(portNumber);
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
        // No packet arrived, loop again
    		}
		}
    }
    
    private void handlePacket(byte[] data, InetAddress address, int port) {
    try {
        if (data.length < 4) return;
        byte[] txID = new byte[]{data[0], data[1]};
        if (data[2] != 0x20) return;
        char msgType = (char) data[3];
        switch (msgType) {
            case 'G':
                handleG(txID, address, port);
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
	throw new Exception("Not implemented");
    }

    public boolean write(String key, String value) throws Exception {
	throw new Exception("Not implemented");
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
	throw new Exception("Not implemented");
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
    int i = offset;
    while (i < data.length && data[i] != 0x20) {
        i++;
    	}
    if (i >= data.length) throw new Exception("Invalid CRN encoding");
    i++; // skip the space after the number
    int start = i;
    while (i < data.length && data[i] != 0x20) {
        i++;
    	}
    // i is now pointing at what might be the last space
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
    
    public static void main(String[] args) throws Exception {
    Node n = new Node();
    n.setNodeName("N:testnode");
    n.openPort(20110);
    System.out.println("Node listening on port 20110...");
    n.handleIncomingMessages(0);
    }
}

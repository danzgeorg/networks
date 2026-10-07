import java.net.*;

class AzureLabTest {
    private static final int DEFAULT_PORT = 20110;
    private static final int BOOTSTRAP_WAIT_MS = 12_000;

    public static void main(String[] args) {
        try {
            Config config = parseArgs(args);
            printBanner(config);
            Node node = new Node();
            String nodeName = "N:" + config.emailAddress;
            node.setNodeName(nodeName);
            node.openPort(config.port);
            System.out.println("[OK] Node started");
            System.out.println(" node name : " + nodeName);
            System.out.println(" bind port : " + config.port);
            System.out.println();
            System.out.println("[STEP 1] Waiting for initial contact...");
            node.handleIncomingMessages(BOOTSTRAP_WAIT_MS);
            System.out.println("[OK] Initial waiting stage finished");
            System.out.println();
            System.out.println("[STEP 2] Reading known poem entries...");
            int versesFound = 0;
            for (int i = 0; i < 7; i++) {
                String key = "D:jabberwocky" + i;
                String value = node.read(key);
                if (value == null) {
                    System.out.println("[WARN] Could not read key: " + key);
                } else {
                    versesFound++;
                    System.out.println("[OK] " + key + " -> " + value);
                }
            }
            System.out.println("[INFO] Poem entries found: " + versesFound + " / 7");
            System.out.println();
            System.out.println("[STEP 3] Writing a marker value...");
            String markerKey = "D:" + config.emailAddress;
            String markerValue = "It works!";
            boolean writeSuccess = node.write(markerKey, markerValue);
            System.out.println("[INFO] write(" + markerKey + ") returned: " + writeSuccess);
            String markerReadBack = node.read(markerKey);
            if (markerReadBack == null) {
                System.out.println("[WARN] Marker value could not be read back.");
            } else {
                System.out.println("[OK] Read-back succeeded: " + markerReadBack);
            }
            System.out.println();
            System.out.println("[STEP 4] Advertising this node's address...");
            String addressValue = config.ipAddress + ":" + config.port;
            boolean advertiseSuccess = node.write(nodeName, addressValue);
            System.out.println("[INFO] write(" + nodeName + ", " + addressValue + ") returned: " + advertiseSuccess);
            System.out.println();
            System.out.println("[STEP 5] Handling incoming messages...");
            System.out.println(" Press Ctrl+C when you want to stop.");
            node.handleIncomingMessages(0);
            } catch (IllegalArgumentException e) {
            System.err.println("Argument error: " + e.getMessage());
            printUsage();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest");
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    private static Config parseArgs(String[] args) {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Expected 2 or 3 arguments.");
        }
        String emailAddress = args[0].trim();
        String ipAddress = args[1].trim();
        int port = DEFAULT_PORT;
        if (!emailAddress.contains("@")) {
            throw new IllegalArgumentException("First argument must be your email address.");
        }
        if (!ipAddress.startsWith("10.")) {
            throw new IllegalArgumentException("Second argument must be the Azure lab IP.");
        }
        if (args.length == 3) {
            port = Integer.parseInt(args[2].trim());
        }
        return new Config(emailAddress, ipAddress, port);
    }

    private static void printBanner(Config config) {
        System.out.println("==================================================");
        System.out.println(" AzureLabTest - CRN Azure smoke test");
        System.out.println("==================================================");
        System.out.println("Email : " + config.emailAddress);
        System.out.println("Azure IP : " + config.ipAddress);
        System.out.println("Port : " + config.port);
        System.out.println("==================================================");
        System.out.println();
    }

    private static void printUsage() {
        System.err.println("Usage: java AzureLabTest your.email@city.ac.uk 10.x.x.x [port]");
    }

    private static class Config {
        final String emailAddress;
        final String ipAddress;
        final int port;
        Config(String emailAddress, String ipAddress, int port) {
            this.emailAddress = emailAddress;
            this.ipAddress = ipAddress;
            this.port = port;
        }
    }
}
CRN Coursework 1 - README

BUILD INSTRUCTIONS
------------------
javac *.java

RUN INSTRUCTIONS
----------------
java AzureLabTest your.email@city.ac.uk 10.x.x.x [port]
java LocalTest [numberOfNodes]

FEATURES COMPLETE
-----------------
- CRN string encoding and decoding
- SHA-256 hashing and distance calculation
- Name messages (G/H)
- Nearest messages (N/O)
- Address key/value storage with max-3-per-distance rule
- Data key/value storage
- Key existence messages (E/F)
- Read messages (R/S)
- Write messages (W/X)
- Compare-and-swap messages (C/D)
- UDP reliability: retransmission after 5 seconds
- UDP reliability: maximum 3 retries
- UDP reliability: duplicate packet detection
- UDP reliability: transaction ID matching and reordering
- Relay messages (V) with correct transaction ID rewriting
- Robustness: malformed message handling
- Robustness: resource exhaustion protection

KNOWN LIMITATIONS
-----------------
- Data rebalancing is not fully implemented
- Passive and active address mapping may be incomplete

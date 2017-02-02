/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {

	vector<Node> newMemList;
    //vector<Node> failList;
    //vector<Node> addList;
	bool change = false;

	 //Get the current membership list from Membership Protocol / MP1
	newMemList = getMembershipList();
    // Sort the list based on the hashCode
    sort(newMemList.begin(), newMemList.end());

	//Construct the ring, if it isn't already constructed
    if (ring.size() == 0){
        ring = newMemList;
        //Find your own position in the ring
        myRingPos.emplace_back(Node(memberNode->addr));
        //After initially constructing the ring, set/sort the variables for neighbors that require replicas.
        hasMyReplicas = findNeighborsUp(myRingPos);
        sort(hasMyReplicas.begin(), hasMyReplicas.end());
        haveReplicasOf= findNeighborsDown(myRingPos);
        sort(haveReplicasOf.begin(), haveReplicasOf.end());
    }

	//Compare new and current rings by iteration(or count if nodes don't change their hash) when a node has failed or joined
    if (ring.size() != newMemList.size()){
        //Rings are different, set changed and call stab protocol
        change = true;
    } else {
        //Ring size is the same, so let's iterate through to check for changes.
        for (int i = 0; i < ring.size(); i++){
            if (ring.at((unsigned long) i).getHashCode() != newMemList.at((unsigned long) i).getHashCode()){
                //Rings differ, call stab
                change = true;
                break;
            }
        }
    }
    /*
     * Wrote this in case we need to clarify the exact fails/adds.
    //Since both lists are guaranteed to be sorted, run the "set difference" algorithm on them, and output to a vector
    //Diff the ring against the newMemList to find those that have failed.
    set_difference(ring.begin(), ring.end(), newMemList.begin(), newMemList.end(), back_inserter(failList));
    //Diff the newMemList against the ring to find those that have been added.
    set_difference(newMemList.begin(), newMemList.end(), ring.begin(), ring.end(), back_inserter(addList));

    //If the lists contain anything, there has been a change, so flag for stab.
    if (addList.size() > 0 | failList.size() > 0) {
        change = true;
    }
    */

	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    if (ht->currentSize() > 0 & change){
        stabilizationProtocol();
    }

    //Cleanup
    newMemList.clear();

    return;
}

/**
 * FUNCTION NAME: getMembershipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		if (this->memberNode->memberList.at(i).getheartbeat() > 0) {
            Address addressOfThisMember(to_string(memberNode->memberList.at(i).getid()) + ":" +
                                        to_string(memberNode->memberList.at(i).getport()));
            curMemList.emplace_back(Node(addressOfThisMember));
        }
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: findNeighborsUp
 *
 * DESCRIPTION: This functions finds a node's next two neighbors in the ring who have it's replicas.
 *
 * RETURNS:
 * vector<Node> with next 2 nodes
 */
vector<Node> MP2Node::findNeighborsUp(vector<Node> searchNode) {

    vector<Node> upNeighborAddrVec;
    if (ring.size() >= 3) {
        // grab next two nodes in ring after searchNode
        for (int i=0; i < ring.size(); i++) {
            if (searchNode.at(0).getHashCode() == ring.at((unsigned long) i).getHashCode()) {
                //Loop back around the ring when hitting the end of vector
                if ((i + 1) == ring.size()){
                    upNeighborAddrVec.emplace_back(ring.at(ring.size()));
                    upNeighborAddrVec.emplace_back(ring.at(0));
                } else if (i == ring.size()){
                    upNeighborAddrVec.emplace_back(ring.at(0));
                    upNeighborAddrVec.emplace_back(ring.at(1));
                } else {
                    upNeighborAddrVec.emplace_back(ring.at((unsigned long) (i + 1)));
                    upNeighborAddrVec.emplace_back(ring.at((unsigned long) (i + 2)));
                }
                break;
            }
        }
    }
    return upNeighborAddrVec;

}

/**
 * FUNCTION NAME: findNeighborsDown
 *
 * DESCRIPTION: This functions finds a node's previous two neighbors in the ring whose replicas it has.
 *
 * RETURNS:
 * vector<Node> with previous 2 nodes
 */
vector<Node> MP2Node::findNeighborsDown(vector<Node> searchNode) {

    vector<Node> downNeighborAddrVec;
    if (ring.size() >= 3) {
        // grab previous two nodes in ring before searchNode
        for (int i=0; i < ring.size(); i++) {
            if (searchNode.at(0).getHashCode() == ring.at((unsigned long) i).getHashCode()) {
                //Loop back around the ring when hitting the beginning of vector
                if (i == 1){
                    downNeighborAddrVec.emplace_back(ring.at(0));
                    downNeighborAddrVec.emplace_back(ring.at(ring.size()));
                } else if (i == 0){
                    downNeighborAddrVec.emplace_back(ring.at(ring.size()));
                    downNeighborAddrVec.emplace_back(ring.at(ring.size()-1));
                } else {
                    downNeighborAddrVec.emplace_back(ring.at((unsigned long) (i - 1)));
                    downNeighborAddrVec.emplace_back(ring.at((unsigned long) (i - 2)));
                }
                break;
            }
        }
    }
    return downNeighborAddrVec;

}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    //Find the Nodes to send this key to.
    vector<Node> targetNodes = findNodes(key);
    if (targetNodes.size() > 0) {
        int curTransId = ++trans_id;

        //Construct and Send to the primary node
        Message primaryMessage(curTransId, memberNode->addr, CREATE, key, value, PRIMARY);
        emulNet->ENsend(&memberNode->addr, &targetNodes.at(0).nodeAddress, primaryMessage.toString());

        if (targetNodes.size() > 1) {
            //Construct and send to the secondary node
            Message secondaryMessage(curTransId, memberNode->addr, CREATE, key, value, SECONDARY);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(1).nodeAddress, secondaryMessage.toString());
        }
        if (targetNodes.size() > 2) {
            //Construct and send to the tertiary node
            Message tertiaryMessage(curTransId, memberNode->addr, CREATE, key, value, TERTIARY);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(2).nodeAddress, tertiaryMessage.toString());
        }
    }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
    //Find the Nodes to for this key.
    vector<Node> targetNodes = findNodes(key);
    if (targetNodes.size() > 0) {
        //Increment the trans_id
        int curTransId = ++trans_id;

        //Construct and Send to the primary node
        Message primaryMessage(curTransId, memberNode->addr, READ, key);
        emulNet->ENsend(&memberNode->addr, &targetNodes.at(0).nodeAddress, primaryMessage.toString());

        if (targetNodes.size() > 1) {
            //Construct and send to the secondary node
            Message secondaryMessage(curTransId, memberNode->addr, READ, key);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(1).nodeAddress, secondaryMessage.toString());
        }
        if (targetNodes.size() > 2) {
            //Construct and send to the tertiary node
            Message tertiaryMessage(curTransId, memberNode->addr, READ, key);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(2).nodeAddress, tertiaryMessage.toString());
        }
    }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
    //Find the Nodes to send this key to.
    vector<Node> targetNodes = findNodes(key);
    if (targetNodes.size() > 0) {
        int curTransId = ++trans_id;

        //Construct and Send to the primary node
        Message primaryMessage(curTransId, memberNode->addr, UPDATE, key, value, PRIMARY);
        emulNet->ENsend(&memberNode->addr, &targetNodes.at(0).nodeAddress, primaryMessage.toString());

        if (targetNodes.size() > 1) {
            //Construct and send to the secondary node
            Message secondaryMessage(curTransId, memberNode->addr, UPDATE, key, value, SECONDARY);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(1).nodeAddress, secondaryMessage.toString());
        }
        if (targetNodes.size() > 2) {
            //Construct and send to the tertiary node
            Message tertiaryMessage(curTransId, memberNode->addr, UPDATE, key, value, TERTIARY);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(2).nodeAddress, tertiaryMessage.toString());
        }
    }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
    //Find the Nodes to delete from.
    vector<Node> targetNodes = findNodes(key);
    if (targetNodes.size() > 0) {
        int curTransId = ++trans_id;

        //Construct and Send to the primary node
        Message primaryMessage(curTransId, memberNode->addr, DELETE, key);
        emulNet->ENsend(&memberNode->addr, &targetNodes.at(0).nodeAddress, primaryMessage.toString());

        if (targetNodes.size() > 1) {
            //Construct and send to the secondary node
            Message secondaryMessage(curTransId, memberNode->addr, DELETE, key);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(1).nodeAddress, secondaryMessage.toString());
        }
        if (targetNodes.size() > 2) {
            //Construct and send to the tertiary node
            Message tertiaryMessage(curTransId, memberNode->addr, DELETE, key);
            emulNet->ENsend(&memberNode->addr, &targetNodes.at(2).nodeAddress, tertiaryMessage.toString());
        }
    }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    //Insert key, value, replicaType into the hash table
    bool keyExists = false;
    bool keyResponse;
    Entry newEntryVal(value, par->globaltime, replica);

    //Check hashtable for the key before creating.
    map<string, string>::iterator it = ht->hashTable.begin();
    while (it != ht->hashTable.end()) {
        string keyString = it->first;
        if (keyString == key){
            //This key already exists. We need to update instead of create.
            keyExists = true;
            break;
        }
        it++;
    }

    //Create or update the key.
    if (!keyExists){
        //Create
        keyResponse = ht->create(key, newEntryVal.convertToString());
    } else {
        //Update
        keyResponse = ht->update(key, newEntryVal.convertToString());
    }

    return keyResponse;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    //Read key from local hash table and return value
    bool keyExists = false;
    string keyResponse;

    //Check hashtable for the key.
    map<string, string>::iterator it = ht->hashTable.begin();
    while (it != ht->hashTable.end()) {
        string keyString = it->first;
        if (keyString == key){
            //This key does exists.
            keyExists = true;
            break;
        }
        it++;
    }

    //Return the key value if it exists.
    if (keyExists){
        keyResponse = ht->read(key);
    } else {
        //Key doesn't exist, return 0.
        return 0;
    }

    return keyResponse;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    //Update key in local hash table and return true or false
    bool keyExists = false;
    bool keyResponse = false;
    Entry newEntryVal(value, par->globaltime, replica);

    //Check hashtable for the key before creating.
    map<string, string>::iterator it = ht->hashTable.begin();
    while (it != ht->hashTable.end()) {
        string keyString = it->first;
        if (keyString == key){
            //This key already exists. Time to update.
            keyExists = true;
            break;
        }
        it++;
    }

    //Update key or return default of false.
    if (keyExists){
        //Update
        keyResponse = ht->update(key, newEntryVal.convertToString());
    }

    return keyResponse;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
    //Delete the key from the local hash table
    bool keyExists = false;
    bool keyResponse = false;

    //Check hashtable for the key before creating.
    map<string, string>::iterator it = ht->hashTable.begin();
    while (it != ht->hashTable.end()) {
        string keyString = it->first;
        Entry checkValue = Entry(it->second);
        if (keyString == key){
            //This key exists. Time to delete.
            keyExists = true;
            break;
        }
        it++;
    }

    //Delete key or return default of false.
    if (keyExists){
        //Delete
        keyResponse = ht->deleteKey(key);
    }

    return keyResponse;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {

	char * data;
	int size;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
        //Pop a message from the queue
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		//TODO: Process the messages from client calls, into the server functions, then reply back to client.
        //TODO: Process the return messages from server read/write Replies, ONLY if QUORUM(2 nodes) of replies are received(for READ), otherwise fail it.
        //TODO: When processing server create/update/delete, make sure the key exists first.
        //TODO: Create vector(of replyQueue) to store transaction ID and it's count, of only reply/readreply type messages.
        //TODO: Cleanup vector after timeout. Disregard transactions after timeout for specific transID. Cleanup after count hits 3 too.
        //TODO: server readKey response can be 0, indicating that the key doesn't exist.
	}

}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 * 				NOTE! This function only tells you where a key should be, not that the key is actually on the found nodes' HT
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at((unsigned long) i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
        emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
        return true;
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there are always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {

    vector<Node> newUpNeighbors;
    vector<Node> newDownNeighbors;
    //Rerun findNeighbors to see if values differ. If different, then copy keys as needed, and update neighbor variables.
    newUpNeighbors = findNeighborsUp(myRingPos);
    sort(newUpNeighbors.begin(), newUpNeighbors.end());
    bool upNeighborChanged = false;
    newDownNeighbors = findNeighborsDown(myRingPos);
    sort(newDownNeighbors.begin(), newDownNeighbors.end());

    //See if the neighbors are the same, if not, set new ones
    if ((newUpNeighbors.at(0).getHashCode() != hasMyReplicas.at(0).getHashCode()) | (newUpNeighbors.at(1).getHashCode() != hasMyReplicas.at(1).getHashCode())){
        //Update Neighbor variable with updated ring data
        hasMyReplicas = newUpNeighbors;
        upNeighborChanged = true;
    }

    if ((newDownNeighbors.at(0).getHashCode() != haveReplicasOf.at(0).getHashCode()) | (newDownNeighbors.at(1).getHashCode() != haveReplicasOf.at(1).getHashCode())){
        //Update Neighbor variable with updated ring data
        haveReplicasOf = newDownNeighbors;
    }

    //For each key in this node's DHT, run findNodes(). If it doesn't return a vector with myRingPos, replicate to the correct node.
    map<string, string>::iterator it = ht->hashTable.begin();
    while (it != ht->hashTable.end()){
        string keyString = it->first;
        Entry checkValue = Entry(it->second);
        vector<Node> keyLocations = findNodes(keyString);

        if (checkValue.replica == PRIMARY) {
            //This node is primary for this key, now see if it should be.
            if (keyLocations.at(0).getHashCode() != myRingPos.at(0).getHashCode()) {
                //This node is no longer the primary for that key, replicate to that node and it's neighbors.
                //Get the next trans_id
                int curTransId = ++trans_id;

                //Construct the message and send to new primary
                Message newPrimaryMessage(curTransId, memberNode->addr, CREATE, keyString, checkValue.value, PRIMARY);
                emulNet->ENsend(&memberNode->addr, &keyLocations.at(0).nodeAddress, newPrimaryMessage.toString());

                //Replicate to other nodes
                if (keyLocations.at(1).getHashCode() != myRingPos.at(0).getHashCode()) {
                    //Replicate to new secondary
                    Message newSecondaryMessage(curTransId, memberNode->addr, CREATE, keyString, checkValue.value,
                                             SECONDARY);
                    emulNet->ENsend(&memberNode->addr, &keyLocations.at(0).nodeAddress, newSecondaryMessage.toString());

                } else if (keyLocations.at(2).getHashCode() != myRingPos.at(0).getHashCode()) {
                    //Replicate to new tertiary
                    Message newTertiaryMessage(curTransId, memberNode->addr, CREATE, keyString, checkValue.value,
                                            TERTIARY);
                    emulNet->ENsend(&memberNode->addr, &keyLocations.at(0).nodeAddress, newTertiaryMessage.toString());

                } else if (keyLocations.at(1).getHashCode() == myRingPos.at(0).getHashCode()) {
                    //This node is the new secondary for that key, just update the type.
                    checkValue.replica = SECONDARY;
                    ht->update(keyString, checkValue.convertToString());

                } else {
                    //Regardless of neighbor location, this node is set to tertiary.
                    checkValue.replica = TERTIARY;
                    ht->update(keyString, checkValue.convertToString());
                }
            }
            //Now that the replica types are set correctly, broadcast out the primary keys to neighbors, if they changed.
            if (upNeighborChanged){
                //Grab next trans_id
                int nxtTransId = ++trans_id;

                if (hasMyReplicas.size() > 1) {
                    //Construct and send to the secondary node
                    Message secondaryMessage(nxtTransId, memberNode->addr, CREATE, keyString);
                    emulNet->ENsend(&memberNode->addr, &hasMyReplicas.at(0).nodeAddress, secondaryMessage.toString());
                }
                if (hasMyReplicas.size() > 2) {
                    //Construct and send to the tertiary node
                    Message tertiaryMessage(nxtTransId, memberNode->addr, CREATE, keyString);
                    emulNet->ENsend(&memberNode->addr, &hasMyReplicas.at(1).nodeAddress, tertiaryMessage.toString());
                }
            }
        }
        it++;
    }
}


















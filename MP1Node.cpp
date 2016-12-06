/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
/*
 * Note: Do you even C++ Bro? Cause this code is whack!
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "initthisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    //int id = *(int*)(&memberNode->addr.addr);
    //int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 1;
    memberNode->pingCounter = 1;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    //MessageHdr *msg;
#ifdef DEBUGLOG
    //static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        string joinStr = "0";
        joinStr = joinStr + ',' + memberNode->addr.getAddress() + ',' + to_string(memberNode->heartbeat);
        //char *msg = new char[joinStr.size() + 1];
        //copy(joinStr.begin(), joinStr.end(), msg);
        //msg[joinStr.size()] = '\0'; // don't forget the terminating 0
        char * msg = strdup(joinStr.c_str());
        int msgsize = (int)joinStr.size()+1;
#ifdef DEBUGLOG
        //sprintf(s, "Trying to join...");
        //log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, msg, msgsize);

        //delete[]msg;
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     * Clean up these pointers
     * EmulNet *emulNet;
     Log *log;
     Params *par;
     Member *memberNode;
     *Remove node from group first
     * Clean up memberlist
     *
     */
    //TODO
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilities!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {

    //Rebuild data string, then split into vector elements.
    string callBackData(data);
    vector<string> dataVec;
    split(callBackData, ',', dataVec);

    int requestType = stoi(dataVec[0]);
    //long timestamp = (long) time(NULL);

    if (requestType == 0) {
        //JOINREQ
        //Add node to memberlist and return memberlist in char array.
        cout << "                Processing JOINREQ on node: ";
        printAddress(&memberNode->addr);
        cout << "JOINREQ msgsize: " << size << endl;
        cout<<"JoinReqString: "<<callBackData<<endl;
        //Set incoming node variables from vector
        Address addr(dataVec[1]);
        long heartbeat = stol(dataVec[2]);

        //Clear the dataVec vector since we don't need it anymore
        dataVec.clear();

        cout << "Joiner Address: ";
        printAddress(&addr);

        //If memberlist is empty, add yourself.
        if (memberNode->memberList.size() == 0) {
            MemberListEntry *IntroMLE = new MemberListEntry((int) memberNode->addr.addr[0],
                                                            (short) memberNode->addr.addr[4], memberNode->heartbeat,
                                                            par->globaltime);
            memberNode->memberList.push_back(*IntroMLE);
            memberNode->myPos = memberNode->memberList.begin();
        }

        //Build MLE from joiner data
        MemberListEntry *joinerMLE = new MemberListEntry((int) addr.addr[0], (short) addr.addr[4], heartbeat,
                                                         par->globaltime);
        //Add it to memberlist
        memberNode->memberList.push_back(*joinerMLE);

        //Log addition of member to memberlist
        log->logNodeAdd(&memberNode->addr, &addr);

        //Build return message
        //Create a string from the memberlist
        string list = to_string(1);
        for (int i = 0; i < (int)memberNode->memberList.size(); i++) {
            list = list + "," + to_string(memberNode->memberList[i].getid()) + ":" +
                   to_string(memberNode->memberList[i].getport()) + ":" +
                   to_string(memberNode->memberList[i].getheartbeat()) + ":" +
                   to_string(memberNode->memberList[i].gettimestamp());
        }

        //Build the JoinRep message
        //repMsg->msgType = JOINREP;
        //char *repMsg = new char[list.size() + 1];
        //copy(list.begin(), list.end(), repMsg);
        //repMsg[list.size()] = '\0'; // don't forget the terminating 0
        char *repMsg = strdup(list.c_str());
        cout << "JoinReq outmsg MemberList String: " << string(repMsg) << endl;

        //Build msgsize
        int msgsize = (int)list.size()+1;

        //Clear list until next time
        list.clear();

        //Send the JoinRep message
        emulNet->ENsend(&memberNode->addr, &addr, repMsg, msgsize);

        // don't forget to free the string after finished using it
        //delete[]repMsg;

        return 1;
    }

    if (requestType == 1) {
        //JOINREP
        //Change char pointer into string then parse into memberlist
        cout << "                Processing JOINREP on node: ";
        printAddress(&memberNode->addr);
        cout << "JOINREP msgsize: " << size << endl;
        cout << "joinrep data: " << callBackData << endl;

        //Build and populate the memberlist with dataVec that holds request type(at [0]) and memberlist
        vector<string> tempMle;
        for (int i = 0; i < (int)dataVec.size()-1; i++) {
            split(dataVec[i+1],':',tempMle);
            memberNode->memberList.push_back(MemberListEntry(stoi(tempMle[0]), (short)stoi(tempMle[1]), stol(tempMle[2]), stol(tempMle[3])));
            tempMle.clear();
        }
        //Clear the dataVec vector since we don't need it anymore
        dataVec.clear();

        //Loop Build address and Log the node add
        for (int i = 0; i < (int)memberNode->memberList.size()-1; i++){
            Address addAddr(to_string(memberNode->memberList[i].getid()) + ":" +
                            to_string(memberNode->memberList[i].getport()));
            log->logNodeAdd(&memberNode->addr, &addAddr);
        }

        //Set myPos
        memberNode->myPos = prev(memberNode->memberList.end());

        //Successfully joined the group
        memberNode->inGroup = true;

        return 1;
    }

    if (requestType == 2) {
        //GOSSIP
        cout << "                Processing GOSSIP message on node: ";
        printAddress(&memberNode->addr);
        cout << "GOSSIP msgsize: " << size << endl;
        cout << "GOSSIP Data: " << callBackData << endl;
        //Build and populate the temp memberlist
        vector<string> tempMle;
        vector<MemberListEntry> tempMemList;
        for (int i = 0; i < (int)dataVec.size()-1; i++) {
            split(dataVec[i+1],':',tempMle);
            tempMemList.push_back(MemberListEntry(stoi(tempMle[0]), (short)stoi(tempMle[1]), stol(tempMle[2]), stol(tempMle[3])));
            tempMle.clear();
        }
        //Clear the dataVec vector since we don't need it anymore
        dataVec.clear();

        //See if there are any nodes in the tempMemList that the membernode doesn't have, then add them.
        //TODO: this may need to change to a MemberListEntry assignment operator instead of a push_back
        if (tempMemList.size() > memberNode->memberList.size()) {
            int missingNodes = (int)(tempMemList.size()) - (int)memberNode->memberList.size();
            cout<<"Missing Nodes size: "<<missingNodes<<endl;
            for (int i=(int)(memberNode->memberList.size()); i < (int)(tempMemList.size()); i++){
                memberNode->memberList.push_back(tempMemList[i]);
                //Build address and Log the node add
                Address addAddr(to_string(tempMemList[i].getid()) + ":" +
                                 to_string(tempMemList[i].getport()));
                log->logNodeAdd(&memberNode->addr, &addAddr);
                cout<<"Node ";
                printAddress(&addAddr);
                cout<<" Added by ";
                printAddress(&memberNode->addr);
            }
        }

        //Now compare heartbeats of each node between memberlists, then update heartbeat and timestamp accordingly.
        for (int i=0; i < (int)tempMemList.size(); i++){
            if ((tempMemList[i].getheartbeat() > memberNode->memberList[i].getheartbeat()) && memberNode->memberList[i].getheartbeat() != 0){
                memberNode->memberList[i].setheartbeat(tempMemList[i].getheartbeat());
                memberNode->memberList[i].settimestamp(par->globaltime);
            }
        }
        //Clear the tempMemList vector since we don't need it anymore
        tempMemList.clear();

        return 1;

    }

return 0;

}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
    void MP1Node::nodeLoopOps() {
    cout << "                Starting nodeLoopOps on node: ";
    printAddress(&memberNode->addr);
    //Create a timestamp of the current time
    //long timestamp = (long) time(NULL);

    //Find my location in the memberlist
    int myLoc = 0;
    for (int i=0; i < (int)memberNode->memberList.size(); i++){
        if (memberNode->memberList[i].getid() == (int)memberNode->addr.addr[0]){
            myLoc = i;
        }
    }

    //Update own heartbeat and timestamp in membernode and in memberlist
    //memberNode->heartbeat = memberNode->heartbeat +1;
    memberNode->memberList[myLoc].setheartbeat(++memberNode->heartbeat);
    memberNode->memberList[myLoc].settimestamp(par->globaltime);

    //Loop through memberlist to check for timed-out members
    for(int i=0; i < (int)memberNode->memberList.size(); i++){
        if((par->globaltime - memberNode->memberList[i].gettimestamp()) > TREMOVE){
            if (memberNode->memberList[i].getheartbeat() != 0) {
                //Flag node as failed.
                memberNode->memberList[i].setheartbeat(0);
                //Build address and Log the removal of the member
                Address remAddr(to_string(memberNode->memberList[i].getid()) + ":" +
                                to_string(memberNode->memberList[i].getport()));
                cout << "Node ";
                printAddress(&remAddr);
                cout << " Failed by ";
                printAddress(&memberNode->addr);
                log->logNodeRemove(&memberNode->addr, &remAddr);
            }
        }
    }



    if (memberNode->pingCounter % 5 == 0) {

        //Create a string to hold memlist, with first value of 2(GOSSIP msgtype)
        string gosMemList = to_string(2);

        //Build a memberlist into string
        for (int i = 0; i < (int) memberNode->memberList.size(); i++) {
            gosMemList = gosMemList + "," + to_string(memberNode->memberList[i].getid()) + ":" +
                         to_string(memberNode->memberList[i].getport()) + ":" +
                         to_string(memberNode->memberList[i].getheartbeat()) + ":" +
                         to_string(memberNode->memberList[i].gettimestamp());
        }
        cout << "NodeLoops MemberList String: " << gosMemList << endl;

        //Build char pointer from gosMemList to use in ENsend
        char *gosMsg = new char[gosMemList.size() + 1];
        copy(gosMemList.begin(), gosMemList.end(), gosMsg);
        gosMsg[gosMemList.size()] = '\0'; // don't forget the terminating 0
        //char *gosMsg = strdup(gosMemList.c_str());

        //Build msgsize
        int msgsize = (int)gosMemList.size()+1;

        //Clear gosMemList until next time
        //delete gosMemList;

        //Find non-failed nodes to gossip to and place node location into vector. Also exclude self from possible gossip targets.
        vector<int> nonFail;
        for (int i = 0; i < (int) memberNode->memberList.size(); i++) {
            if ((memberNode->memberList[i].getheartbeat() > 0) && (i != myLoc)) {
                nonFail.push_back(i);
            }
        }

        //Randomize the non-failed nodes
        random_shuffle(nonFail.begin(), nonFail.end());

        //Loop send message for selected members
        for (int i = 0; i < 4; i++) {
            if (i < (int)nonFail.size()) {
                //Build an address for each node in nonFail.
                Address sendAddr(to_string(memberNode->memberList[nonFail[i]].getid()) + ":" +
                                 to_string(memberNode->memberList[nonFail[i]].getport()));

                //Send the gossip message.
                cout << i + 1 << "th address to be gossiped to: ";
                printAddress(&sendAddr);
                emulNet->ENsend(&memberNode->addr, &sendAddr, gosMsg, msgsize);
            }
        }

        //delete msgsize;
        //Clear nonFail
        nonFail.clear();
        // don't forget to free the string after finished using it
        delete[]gosMsg;
    }

    //Increment the ping counter
    memberNode->pingCounter = memberNode->pingCounter +1;

    return;
    }

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
    int MP1Node::isNullAddress(Address *addr) {
        return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
    }

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
    Address MP1Node::getJoinAddress() {
        Address joinaddr;

        memset(&joinaddr, 0, sizeof(Address));
        *(int *) (&joinaddr.addr) = 1;
        *(short *) (&joinaddr.addr[4]) = 0;

        return joinaddr;
    }

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
    void MP1Node::initMemberListTable(Member *memberNode) {
        memberNode->memberList.clear();
    }

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
    void MP1Node::printAddress(Address *addr) {
        printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
               addr->addr[3], *(short *) &addr->addr[4]);
    }

/**
 * FUNCTION NAME: split
 *
 * DESC: Needed something to quickly split strings on delim.
 */
void MP1Node::split(const string &s, char delim, vector<string> &elems) {
    stringstream ss;
    ss.str(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}




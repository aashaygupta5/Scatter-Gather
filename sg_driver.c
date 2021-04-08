////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : Aashay Gupta
//   Last Modified : 
//

// Include Files

// Project Includes
#include <sg_driver.h>
#include <sg_service.h>
//#include <sg_cache.c>
#include <string.h>

// Defines

//
// Global Data

char dataind1;
uint32_t magic = SG_MAGIC_VALUE;
//int indicator = 0;
//int indicator2 = 0;
int nfile = 0;
int counter = 0;
int count = 0;
#define filesize 500

struct file_metadata{
	char filename[1024];
	int size;
	int pos;
	int status;
	//int nblocks;
	SgFHandle fd;
	//SG_Node_ID local;
	SG_Node_ID remote[SG_MAX_BLOCKS_PER_FILE];
	SG_Block_ID block[SG_MAX_BLOCKS_PER_FILE];
	//SG_Node_ID nodeid[SG_MAX_BLOCKS_PER_FILE];
	//SG_SeqNum rseqn[SG_MAX_BLOCKS_PER_FILE];
};

struct table{
	SG_Node_ID rmt;
	SG_SeqNum rrsn; 
};

struct table tablearray[2048];

int search(SG_Node_ID remote){
	int i;
	for(i = 0; i < 2048; i++){
		if(tablearray[i].rmt == remote){
			return i;
		}
	}
	return -1;	
}


//struct table* item;
/*
SG_Node_ID hash(SG_Node_ID remote){
	logMessage(LOG_ERROR_LEVEL, "%d", remote / (2 ^ 64));
	return remote / (2^64);
}
*/



void insert(SG_Node_ID remote, SG_SeqNum rrsn){
	//struct table *item = (struct table*) malloc(sizeof(struct table));
	//item->rmt = remote;
	//item->rrsn = rrsn;
	int x;
	for(x = 0; x < 2048; x++){
		if(tablearray[x].rmt == remote){
			tablearray[x].rrsn += 1;
		}
	}
	
	tablearray[count].rmt = remote;
	tablearray[count].rrsn = rrsn;
	
	count++;
	
	
	
	//int index;
	//index = search(remote);
	
	//if(tablearray[index].rmt == remote){
	//	tablearray[index].rrsn += 1;
	//}
	
	
	//if(tablearray[index].rmt == remote){
	//	tablearray[index].rrsn += 1;
	//}
	
	//while(tablearray[index] != NULL){
	//	index++;
	//	index %= 264;
	//}
	//tablearray[index] = item;
}


 
typedef struct file_metadata fm; 

fm files[filesize];

// Driver file entry

// Global data
int sgDriverInitialized = 0; // The flag indicating the driver initialized
SG_Block_ID sgLocalNodeId;   // The local node identifier
SG_SeqNum sgLocalSeqno;      // The local sequence number
//SG_SeqNum sgRemoteSeqno;     // The remote sequence number

// Driver support functions
int sgInitEndpoint( void ); // Initialize the endpoint

//
// Functions

//
// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char *path) {

    // First check to see if we have been initialized
    if (!sgDriverInitialized) {

        // Call the endpoint initialization 
        if ( sgInitEndpoint() ) {
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed." );
            return( -1 );
        }

        // Set to initialized
        sgDriverInitialized = 1;
    }
    // FILL IN THE REST OF THE CODE
       
       
    //initSGCache(SG_MAX_CACHE_ELEMENTS);   
          
    for(nfile = 0; nfile < filesize; nfile++){
    	
    	if(nfile == counter){
    		memcpy(&files[nfile].filename, path, strlen(path));
    		//&file[nfile].file = &files[nfile];
    		files[nfile].pos = 0;
    		files[nfile].size = 0;
    		files[nfile].status = 1;
    		files[nfile].fd = nfile;
    		//files[nfile].nblocks = 0;
    		//nhandle++;
    		counter += 1;
    		break;
    	}

    	
    	//nfile++;
    }
    //logMessage(LOG_ERROR_LEVEL, "%d", files[nfile].fd);
     
    // Return the file handle 
    return( files[nfile].fd );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char *buf, size_t len) {

    if(files[fh].status == 0){
    	return -1;
    }
    
    SG_Node_ID rem;
    SG_Block_ID blk;
    SG_System_OP op;
    SG_SeqNum ssn, rsn;
    SG_Packet_Status ret;
    char pkt[SG_BASE_PACKET_SIZE], rpkt[SG_DATA_PACKET_SIZE];
    char temp[1024];
    
    //memcpy(buf, "\0", 1);
    
    //size_t totallen = len;
    size_t pktlen, rpktlen;
    
    
    //logMessage(LOG_ERROR_LEVEL, "%d", files[fh].pos / 1024);
    
    rem = files[fh].remote[files[fh].pos / 1024];
    blk = files[fh].block[files[fh].pos / 1024];
    
    int index3;
    if( search(rem) != -1){
    	index3 = search(rem);
    }
    tablearray[index3].rrsn+=1;
    //logMessage(LOG_ERROR_LEVEL, "%d", rem);
    //logMessage(LOG_ERROR_LEVEL, "%d", blk);
    
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem,   // Remote ID
                                    blk,  // Block ID
                                    SG_OBTAIN_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    tablearray[index3].rrsn,  // Receiver sequence number
                                    NULL, pkt, &pktlen)) != SG_PACKT_OK ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
      	return( -1 );
    }

    // Send the packet
    rpktlen = SG_DATA_PACKET_SIZE;
    if ( sgServicePost(pkt, &pktlen, rpkt, &rpktlen) ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      	return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&sgLocalNodeId, &rem, &blk, &op, &ssn, 
    	                               &rsn, temp, rpkt, rpktlen)) != SG_PACKT_OK ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
      	return( -1 );
    }
    
    //tablearray[index3].rrsn = rsn;
    
    //logMessage(LOG_ERROR_LEVEL, "buf %s", buf);
    
    if(files[fh].pos % 1024 == 0){
    	memcpy(buf, temp, 256);
    	//memcpy(&buf[512], "\0", 1);
    	//logMessage(LOG_ERROR_LEVEL, "%s", buf);
    }
    if(files[fh].pos % 1024 == 256){
    	memcpy(buf, temp+256, 256);
    	//memcpy(&buf[1024], "\0", 1);
    	//logMessage(LOG_ERROR_LEVEL, "after % 1024 == len %s", buf);
    }
    if(files[fh].pos % 1024 == 512){
    	memcpy(buf, temp+512, 256);
    }
    if(files[fh].pos % 1024 == 768){
    	memcpy(buf, temp+768, 256);
    }
    
    
    
    // Sanity check the return value
    if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      	return( -1 );
    }
    
    
    
    //if(indicator2 <= indicator){
    //files[fh].remote[files[fh].pos / 1024] = rem;
    //files[fh].block[files[fh].pos / 1024] = blk;
    //	indicator2++;
    //}
    
    //if(indicator2 >= indicator){
    //	indicator2 = 0;
    //}
    
    //files[fh].size = size;
    files[fh].pos += len;
      
    // Return the bytes processed
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char *buf, size_t len) {

    if(files[fh].status == 0){
    	return -1;
    }
    
    
    //if(files[fh].pos
    if(files[fh].pos % 1024 != 0){
    	SG_Node_ID rem1;
    	SG_Block_ID blk1;
    	SG_System_OP op1;
    	SG_SeqNum ssn1, rsn1;
    	SG_Packet_Status ret1;
    	char pkt1[SG_BASE_PACKET_SIZE], rpkt1[SG_DATA_PACKET_SIZE];
    	char buf2[1024];
    		
    	//buf2[1024] = 0;	
    	size_t pktlen1, rpktlen1;
    	
    	rem1 = files[fh].remote[files[fh].pos / 1024];
    	blk1 = files[fh].block[files[fh].pos / 1024];
    	
    	int index2; 
    	if( search(rem1) != -1){
    		index2 = search(rem1);
    	}
    	
    	tablearray[index2].rrsn += 1;
    	
    	//logMessage(LOG_ERROR_LEVEL, "before obtaining %d", rem1);
        //logMessage(LOG_ERROR_LEVEL, "%d", blk1);
    	
    	pktlen1 = SG_BASE_PACKET_SIZE;
    	if ( (ret1 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem1,   // Remote ID
                                    blk1,  // Block ID
                                    SG_OBTAIN_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    tablearray[index2].rrsn,  // Receiver sequence number
                                    NULL, pkt1, &pktlen1)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret1 );
      		return( -1 );
    	}

    	// Send the packet
    	rpktlen1 = SG_DATA_PACKET_SIZE;
    	if ( sgServicePost(pkt1, &pktlen1, rpkt1, &rpktlen1) ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      		return( -1 );
    	}

    	// Unpack the recieived data
    	if ( (ret1 = deserialize_sg_packet(&sgLocalNodeId, &rem1, &blk1, &op1, &ssn1, 
    	                               &rsn1, buf2, rpkt1, rpktlen1)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret1 );
      		return( -1 );
    	}
    	
    	//tablearray[index2].rrsn = rsn1;
	
	//files[fh].rseqn[files[fh].pos / 1024] = rsn1;
	
	//logMessage(LOG_ERROR_LEVEL, "BUF DATA: %s", buf);
	
	//logMessage(LOG_ERROR_LEVEL, "data being obtained: %s", buf2);
	//logMessage(LOG_ERROR_LEVEL, "after obtaining %d", rem1);
        //logMessage(LOG_ERROR_LEVEL, "%d", blk1);	
	
    	// Sanity check the return value
    	if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      		return( -1 );
    	}
    	
    	//SG_Node_ID rem2;
    	//SG_Block_ID blk2;
    	SG_System_OP op2;
    	SG_SeqNum ssn2, rsn2;
    	SG_Packet_Status ret2;
    	char pkt2[SG_DATA_PACKET_SIZE], rpkt2[SG_BASE_PACKET_SIZE];
    	
    	size_t pktlen2, rpktlen2;
    	
    	//logMessage(LOG_ERROR_LEVEL, "POS % 1024 %d", files[fh].pos % 1024);
    	
    	if(files[fh].pos % 1024 == 256){
    		memcpy(&buf2[256], buf, 256);
    		//memcpy(&buf2[1024], "\0", 1);
    	}
    	
    	if(files[fh].pos % 1024 == 512){
    		memcpy(&buf2[512], buf, 256);
    		//memcpy(&buf2[1024], "\0", 1);
    	}
    	
    	if(files[fh].pos % 1024 == 768){
    		memcpy(&buf2[768], buf, 256);
    		//memcpy(&buf2[1024], "\0", 1);
    	}
    	
    	//buf1[1024] = 0;
    	
    	
    	
    	//logMessage(LOG_ERROR_LEVEL, "buf2 data %s", buf2);
    	
    	tablearray[index2].rrsn += 1;
    	
    	pktlen2 = SG_DATA_PACKET_SIZE;   	
    	if ( (ret2 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem1,   // Remote ID
                                    blk1, // Block ID
                                    SG_UPDATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    tablearray[index2].rrsn,  // Receiver sequence number
                                    buf2, pkt2, &pktlen2)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet 2[%d].", ret2 );
      		return( -1 );
    	}

    	// Send the packet
    	rpktlen2 = SG_BASE_PACKET_SIZE;
    	if ( sgServicePost(pkt2, &pktlen2, rpkt2, &rpktlen2) ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      		return( -1 );
    	}

    	// Unpack the recieived data
    	if ( (ret2 = deserialize_sg_packet(&sgLocalNodeId, &rem1, &blk1, &op2, &ssn2, 
    	                               &rsn2, NULL, rpkt2, rpktlen2)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret2 );
      		return( -1 );
    	}
	
	//logMessage(LOG_ERROR_LEVEL, "after updating %d", rem1);
        //logMessage(LOG_ERROR_LEVEL, "%d", blk1);
	
    	// Sanity check the return value
    	if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      		return( -1 );
    	}
    	
    	tablearray[index2].rrsn = rsn2;
    	
	files[fh].remote[files[fh].pos / 1024] = rem1;
    	files[fh].block[files[fh].pos / 1024] = blk1;
    	    	
    	//files[fh].rseqn[files[fh].pos / 1024] = rsn2; 
    	//files[fh].size += len; 
    	files[fh].pos += len;
    	
    	
    	
    	
    	//logMessage(LOG_ERROR_LEVEL, "%d", files[fh].pos / 1024);
    		
    	return len;	
    }
    
    
    
    if(files[fh].pos % 1024 == 0){
    	if(files[fh].pos == files[fh].size){
    		
    		SG_Node_ID rem3;
    		SG_Block_ID blk3;
    		SG_System_OP op3;
    		SG_SeqNum ssn3, rsn3;
    		SG_Packet_Status ret3;
    		char pkt3[SG_DATA_PACKET_SIZE], rpkt3[SG_BASE_PACKET_SIZE];
    		char buf3[1024];
    		
    		//logMessage(LOG_ERROR_LEVEL, "buF IN 1024 == 0 %s", buf);
    		//buf3[1024] = (char) NULL;
    		
    		memcpy(buf3, buf, 256);
    		//memcpy(&buf3[512], "\0", 1); 
    		
    		
    		//logMessage(LOG_ERROR_LEVEL, "buf being passed to creaTe %s", buf3);
    		
    		
    		size_t pktlen3, rpktlen3; 
    		
    		// Setup the packet
    		pktlen3 = SG_DATA_PACKET_SIZE;
    		if ( (ret3 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_CREATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    buf3, pkt3, &pktlen3)) != SG_PACKT_OK ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization[%d].", ret3 );
      			return( -1 );
    		}

    		// Send the packet
    		rpktlen3 = SG_BASE_PACKET_SIZE;
    		if ( sgServicePost(pkt3, &pktlen3, rpkt3, &rpktlen3) ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet" );
      			return( -1 );
    		}

    		// Unpack the recieived data
    		if ( (ret3 = deserialize_sg_packet(&sgLocalNodeId, &rem3, &blk3, &op3, &ssn3, 
    	                               &rsn3, NULL, rpkt3, rpktlen3)) != SG_PACKT_OK ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization [%d]", ret3 );
      			return( -1 );
	    	}

	    	// Sanity check the return value
	    	if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
	      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      			return( -1 );
	    	}
	    	
	    	files[fh].remote[files[fh].pos / 1024] = rem3;
    		files[fh].block[files[fh].pos / 1024] = blk3;
	    	
	    	files[fh].size += 1024; 
	    	
    		files[fh].pos += 256;
    		//files[fh].nblocks += 1;
    		
    		insert(rem3, rsn3);	
    		   		
    		
    		 
    		
    		//files[fh].rseqn[files[fh].pos / 1024] = rsn3;
    		
    		
    		return len;
    		
    	}
    	
    	else{
    		SG_Node_ID rem4;
    		SG_Block_ID blk4;
    		SG_System_OP op4;
    		SG_SeqNum ssn4, rsn4;
    		SG_Packet_Status ret4;
    		char pkt4[SG_BASE_PACKET_SIZE], rpkt4[SG_DATA_PACKET_SIZE];
    		char buf1[1024];
    		
    		size_t pktlen4, rpktlen4; 
    		
    		
    		rem4 = files[fh].remote[files[fh].pos / 1024];
    		blk4 = files[fh].block[files[fh].pos / 1024];
    		
    		int index1;
    		if( search(rem4) != -1){
    			index1 = search(rem4);
    		}
    		
    		tablearray[index1].rrsn += 1;
    		
    		pktlen4 = SG_BASE_PACKET_SIZE;
    		if ( (ret4 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem4,   // Remote ID
                                    blk4,  // Block ID
                                    SG_OBTAIN_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    tablearray[index1].rrsn,  // Receiver sequence number
                                    NULL, pkt4, &pktlen4)) != SG_PACKT_OK ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret4 );
      			return( -1 );
    		}

    		// Send the packet
    		rpktlen4 = SG_DATA_PACKET_SIZE;
    		if ( sgServicePost(pkt4, &pktlen4, rpkt4, &rpktlen4) ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      			return( -1 );
    		}

    		// Unpack the recieived data
    		if ( (ret4 = deserialize_sg_packet(&sgLocalNodeId, &rem4, &blk4, &op4, &ssn4, 
    	                               &rsn4, buf1, rpkt4, rpktlen4)) != SG_PACKT_OK ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret4 );
      			return( -1 );
    		}
    		
    		tablearray[index1].rrsn = rsn4;
    		//files[fh].rseqn[files[fh].pos / 1024] = rsn4;

		//logMessage(LOG_ERROR_LEVEL, "data being obtained: %s", buf1);
		//logMessage(LOG_ERROR_LEVEL, "%d", rem4);
        	//logMessage(LOG_ERROR_LEVEL, "%d", blk4);	
	
    		// Sanity check the return value
    		if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      			return( -1 );
    		}
    		
    		//logMessage(LOG_ERROR_LEVEL, "BUF AFTER OBTAINING IN 1024: %s", buf1);
    		
    		if(files[fh].pos % 1024 == 0){
    			memcpy(buf1, buf, 256);
    			//memcpy(&buf1[512], "\0", 1);
    		}
    		
    		
    		//logMessage(LOG_ERROR_LEVEL, "BUF1 AFTER COPYING IN 1024: %s", buf1);
    		
    		//SG_Node_ID rem5;
    		//SG_Block_ID blk5;
    		SG_System_OP op5;
    		SG_SeqNum ssn5, rsn5;
    		SG_Packet_Status ret5;
    		char pkt5[SG_DATA_PACKET_SIZE], rpkt5[SG_BASE_PACKET_SIZE];
    		
    		size_t pktlen5, rpktlen5;
    		
    		//rem5 = files[fh].remote[files[fh].pos / 1024];
    		//blk5 = files[fh].block[files[fh].pos / 1024];	
		
		tablearray[index1].rrsn += 1;
		
		pktlen5 = SG_DATA_PACKET_SIZE;   	
    		if ( (ret5 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem4,   // Remote ID
                                    blk4, // Block ID
                                    SG_UPDATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    tablearray[index1].rrsn,  // Receiver sequence number
                                    buf1, pkt5, &pktlen5)) != SG_PACKT_OK ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet 2[%d].", ret5 );
      			return( -1 );
    		}

    		// Send the packet
    		rpktlen5 = SG_BASE_PACKET_SIZE;
    		if ( sgServicePost(pkt5, &pktlen5, rpkt5, &rpktlen5) ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      			return( -1 );
    		}

    		// Unpack the recieived data
    		if ( (ret5 = deserialize_sg_packet(&sgLocalNodeId, &rem4, &blk4, &op5, &ssn5, 
    	                               &rsn5, NULL, rpkt5, rpktlen5)) != SG_PACKT_OK ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret5 );
      			return( -1 );
    		}
	
		//logMessage(LOG_ERROR_LEVEL, "%d", rem4);
        	//logMessage(LOG_ERROR_LEVEL, "%d", blk4);
	
    		// Sanity check the return value
    		if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      			logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      			return( -1 );
    		}		
		
		tablearray[index1].rrsn = rsn5;
		
		files[fh].remote[files[fh].pos / 1024] = rem4;
    		files[fh].block[files[fh].pos / 1024] = blk4;	
		//files[fh].rseqn[files[fh].pos / 1024] = rsn5;
		files[fh].pos += 256;	
		
		
		    	
    	}
	
	
	
    }
    
    /*
    //int size = strlen(buf);
    logMessage(LOG_ERROR_LEVEL, "data being passed to write   %s", buf);
    
    SG_Node_ID rem1, rem2;
    SG_Block_ID blk1, blk2;
    SG_System_OP op1, op2;
    SG_SeqNum ssn1, ssn2, rsn1, rsn2;
    SG_Packet_Status ret1, ret2;
    char pkt1[SG_BASE_PACKET_SIZE], rpkt1[SG_DATA_PACKET_SIZE];
    char pkt2[SG_DATA_PACKET_SIZE], rpkt2[SG_BASE_PACKET_SIZE];
    char buf1[1024];
   
    //char buf2[1024];
    
    //size_t totallen = len;
    size_t pktlen, rpktlen;
    
    //sgLocalSeqno = SG_INITIAL_SEQNO;
    //if(files[fh].pos != 0 || ((files[fh].pos % 1024) != 0)){
    //	return -1;
    //}
    
          
      
    if(files[fh].pos % 1024 != 0){
    
        logMessage(LOG_ERROR_LEVEL, "%d", files[fh].pos / 1024);        
    	rem1 = files[fh].remote[files[fh].pos / 1024];
    	blk1 = files[fh].block[files[fh].pos / 1024];
    	
    	//logMessage(LOG_ERROR_LEVEL, "%d", files[fh].pos);
    	pktlen = SG_BASE_PACKET_SIZE;
    	if ( (ret1 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem1,   // Remote ID
                                    blk1,  // Block ID
                                    SG_OBTAIN_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, pkt1, &pktlen)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret1 );
      		return( -1 );
    	}

    	// Send the packet
    	rpktlen = SG_DATA_PACKET_SIZE;
    	if ( sgServicePost(pkt1, &pktlen, rpkt1, &rpktlen) ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      		return( -1 );
    	}

    	// Unpack the recieived data
    	if ( (ret1 = deserialize_sg_packet(&sgLocalNodeId, &rem1, &blk1, &op1, &ssn1, 
    	                               &rsn1, buf1, rpkt1, rpktlen)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret1 );
      		return( -1 );
    	}

	//logMessage(LOG_ERROR_LEVEL, "data being obtained: %s", buf1);
	logMessage(LOG_ERROR_LEVEL, "%d", rem1);
        logMessage(LOG_ERROR_LEVEL, "%d", blk1);	
	
    	// Sanity check the return value
    	if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      		return( -1 );
    	}	
    	
    	//if(files[fh].pos % 1024 < len){
    	//	memcpy(buf, buf1, 512);
    	//}
    	
    	if(files[fh].pos % 1024 == len){
    		memcpy(buf1+512, buf, 512);
    	}
    	//buf1[1024] = 0;
    	
    	logMessage(LOG_ERROR_LEVEL, "data going to be updated: %s", buf1);
    	
    	
    	pktlen = SG_DATA_PACKET_SIZE;   	
    	if ( (ret1 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    rem1,   // Remote ID
                                    blk1, // Block ID
                                    SG_UPDATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    buf1, pkt1, &pktlen)) != SG_PACKT_OK ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet 2[%d].", ret1 );
      	return( -1 );
    }

    	// Send the packet
    	rpktlen = SG_BASE_PACKET_SIZE;
    	if ( sgServicePost(pkt1, &pktlen, rpkt1, &rpktlen) ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      		return( -1 );
    	}

    	// Unpack the recieived data
    	if ( (ret1 = deserialize_sg_packet(&sgLocalNodeId, &rem1, &blk1, &op1, &ssn1, 
    	                               &rsn1, buf, rpkt1, rpktlen)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret1 );
      		return( -1 );
    	}
	
	logMessage(LOG_ERROR_LEVEL, "%d", rem1);
        logMessage(LOG_ERROR_LEVEL, "%d", blk1);
	
    	// Sanity check the return value
    	if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      		return( -1 );
    	}
    	//int counter = 0;
    	//for(indicator = 0; indicator < SG_MAX_BLOCKS_PER_FILE; indicator++){
    		
    	//	if(indicator == counter){
    			 
    	//files[fh].size += len; 
    	files[fh].pos = files[fh].pos + len;
    	
    	logMessage(LOG_ERROR_LEVEL, "%d", files[fh].pos / 1024);
    		
    	files[fh].remote[files[fh].pos / 1024] = rem1;
    	files[fh].block[files[fh].pos / 1024] = blk1;
			
	//counter++;
	//break;    
    			//indicator++;
    	 //	}
    	 	
    	 
    	 //}
    	 return len;
    }
    
    if(files[fh].pos % 1024 == 0){
        
        
           
    	// Setup the packet
    	pktlen = SG_DATA_PACKET_SIZE;
    	if ( (ret2 = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_CREATE_BLOCK,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    buf, pkt2, &pktlen)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization[%d].", ret2 );
      		return( -1 );
    	}

    	// Send the packet
    	rpktlen = SG_BASE_PACKET_SIZE;
    	if ( sgServicePost(pkt2, &pktlen, rpkt2, &rpktlen) ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet" );
      		return( -1 );
    	}

    	// Unpack the recieived data
    	if ( (ret2 = deserialize_sg_packet(&sgLocalNodeId, &rem2, &blk2, &op2, &ssn2, 
    	                               &rsn2, buf, rpkt2, rpktlen)) != SG_PACKT_OK ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization [%d]", ret2 );
      		return( -1 );
    	}

    	// Sanity check the return value
    	if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      		logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      		return( -1 );
    	}
    
    
    	//counter = 0;
    	//for(indicator = 0; indicator < SG_MAX_BLOCKS_PER_FILE; indicator++){
    		
    	//	if(indicator == counter){
    			
    
    	files[fh].size += len; 
    	files[fh].pos = files[fh].size;
    	files[fh].nblocks += 1;
    	
    	logMessage(LOG_ERROR_LEVEL, "%d", files[fh].pos / 1024);
    			
    	files[fh].remote[files[fh].pos / 1024] = rem2;
    	files[fh].block[files[fh].pos / 1024] = blk2;
			
			
	logMessage(LOG_ERROR_LEVEL, "%d", rem2);
    logMessage(LOG_ERROR_LEVEL, "%d", blk2);
        		
	//		break;    
    			//indicator++;
    	 //	}
    	 //	counter++;
    	 //}
    
    	//files[fh].remote[indicator] = rem;
    	//files[fh].block[indicator] = blk;
    
    	//files[fh].size += len; 
    	//files[fh].pos = files[fh].size;
    	logMessage( LOG_ERROR_LEVEL, "%d", files[fh].pos);
    	//indicator++;
    	//logMessage( LOG_ERROR_LEVEL, "%d", indicator);
    }
    */
    // Log the write, return bytes written
    
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {
	//logMessage(LOG_ERROR_LEVEL, "%d %d %d", files[fh].status, files[fh].pos, files[fh].size);	
	
    if(files[fh].status == 0 || files[fh].pos > files[fh].size){
    	return -1;
    }
    
    files[fh].pos = off;
    
    // Return new position
    return( off );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) {

    if(files[fh].status == 0){
    	return -1;
    }
    
    files[fh].status = 0;
    files[fh].size = 0;
    files[fh].pos = 0;
    nfile = 0;
    
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {

    SG_Node_ID rem;
    SG_Block_ID blk;
    SG_System_OP op;
    SG_SeqNum ssn, rsn;
    SG_Packet_Status ret;
    char pkt[SG_BASE_PACKET_SIZE], rpkt[SG_BASE_PACKET_SIZE];
    
    //size_t totallen = len;
    size_t pktlen, rpktlen;
    
    //sgLocalSeqno = SG_INITIAL_SEQNO;
    //if(files[fh].pos != 0 || ((files[fh].pos % 1024) != 0)){
    //	return -1;
    //}
       
    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_STOP_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, pkt, &pktlen)) != SG_PACKT_OK ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
      	return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(pkt, &pktlen, rpkt, &rpktlen) ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
      	return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&sgLocalNodeId, &rem, &blk, &op, &ssn, 
    	                               &rsn, NULL, rpkt, rpktlen)) != SG_PACKT_OK ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
      	return( -1 );
    }

    // Sanity check the return value
    if ( sgLocalNodeId == SG_NODE_UNKNOWN ) {
      	logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", sgLocalNodeId );
      	return( -1 );
    }
    
    
    
    // Log, return successfully
    logMessage( LOG_INFO_LEVEL, "Shut down Scatter/Gather driver." );
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
                                     SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char *data,
                                     char *packet, size_t *plen) {

    if(packet == NULL){
		return SG_PACKT_PDATA_BAD;
	}
	
	if(loc == 0){
		return SG_PACKT_LOCID_BAD;
	}
	
	if(rem == 0){
		return SG_PACKT_REMID_BAD;
	}
	
	if(blk == 0){
		return SG_PACKT_BLKID_BAD;
	}
	
	if(op < 0 || op > 6){
		return SG_PACKT_OPERN_BAD;
	}
	
	if(sseq == 0){
		return SG_PACKT_SNDSQ_BAD;
	}
	
	if(rseq == 0){
		return SG_PACKT_RCVSQ_BAD;
	}
	
	if(magic == SG_MAGIC_VALUE){
		//printf("%d\n", *packet);
		memcpy(&packet[0], &magic, 4);
		//logMessage(LOG_ERROR_LEVEL, "[ERROR] serialize_sg_packet: good magic number [%u]", packet[0]);
	}
	
	memcpy(&packet[4], &loc, 8);
		//packet += sizeof(SG_Node_ID);
		//logMessage(LOG_ERROR_LEVEL, "[ERROR] serialize_sg_packet: good local id [%u]", packet[4]);
	memcpy(&packet[12], &rem, 8);
			//packet += sizeof(SG_Node_ID);
		//logMessage(LOG_ERROR_LEVEL, "[ERROR] serialize_sg_packet: good remote id [%u]", &rem);
	
	memcpy(&packet[20], &blk, 8);
		//packet += sizeof(SG_Block_ID);
		//logMessage(LOG_ERROR_LEVEL, "[ERROR] serialize_sg_packet: bad local id [%u]", loc);
	

	if(op >= 0 && op < 7){
			
		memcpy(&packet[28], &op, 4);
		//packet += sizeof(SG_System_OP);
		//logMessage(LOG_ERROR_LEVEL, "[ERROR] serialize_sg_packet: gd blk [%u]", blk);
	}
		
	memcpy(&packet[32], &sseq, 2);
		
	memcpy(&packet[34], &rseq, 2);
			
	if(data == NULL){
		dataind1 = 0;
			
		memcpy(&packet[36], &dataind1, 1);
		
		size_t basesize = SG_BASE_PACKET_SIZE;
		plen = &basesize;
		
		if(magic == SG_MAGIC_VALUE){
			memcpy(&packet[37], &magic, 4);
			return SG_PACKT_OK;
		}
		if(magic != SG_MAGIC_VALUE){
			return SG_PACKT_PDATA_BAD;
		}
		
		
	}
	
	if(data != NULL){
		dataind1 = 1;
		memcpy(&packet[36], &dataind1, 1);
		size_t packetsize = SG_DATA_PACKET_SIZE;
		plen = &packetsize;
		
		memcpy(&packet[37], data, SG_BLOCK_SIZE);
		if(magic == SG_MAGIC_VALUE){
			memcpy(&packet[1061], &magic, 4);
			return SG_PACKT_OK;
		}
		if(magic != SG_MAGIC_VALUE){
			return SG_PACKT_PDATA_BAD;
		}
	}
	return SG_PACKT_OK;
	
	
	

    // Return the system function return value
    //return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID *loc, SG_Node_ID *rem, SG_Block_ID *blk,
                                       SG_System_OP *op, SG_SeqNum *sseq, SG_SeqNum *rseq, char *data,
                                       char *packet, size_t plen) {

    if(packet == NULL){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] bad packet");
        	return SG_PACKT_PDATA_BAD;
        }
               
          	
        memcpy(&magic, &packet[0], 4);
        
        memcpy(loc, &packet[4], 8);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good local id [%u]", packet[4]);
        memcpy(rem, &packet[12], 8);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good remote id [%u]", packet[12]);
        	
        memcpy(blk, &packet[20], 8);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good blk id [%u]", packet[20]);
        memcpy(op, &packet[28], 4);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good op id [%u]", packet[28]);
        	
        memcpy(sseq, &packet[32], 2);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good sseq id [%u]", packet[32]);
        memcpy(rseq, &packet[34], 2);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good rseq id [%u]", packet[34]);
        memcpy(&dataind1, &packet[36], 1);
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good dataind id [%u]", packet[36]);
        	
        if(magic != SG_MAGIC_VALUE){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad magic [%u]", packet[0]);
        	return SG_PACKT_PDATA_BAD;
        }
        	
        if(*loc == 0){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad locid [%u]", packet[4]);
        	return SG_PACKT_LOCID_BAD;
        }
        	
        if(*rem == 0){
        	//logMessage(LOG_ERROR_LEVEL, "bad rem in deserialize");
        	return SG_PACKT_REMID_BAD;
        }
        	
        	
        if(*blk == 0){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad blkid [%u]", packet[20]);
        	return SG_PACKT_BLKID_BAD;
        }
        	        	        	
        if((*op < 0) || (*op > 6)){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad op [%u]", packet[28]);
        	return SG_PACKT_OPERN_BAD;
        }
        	
        if(*sseq == 0){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad sseq [%u]", packet[32]);
        	return SG_PACKT_SNDSQ_BAD;
        }
        	
        	
        if(*rseq == 0){
        	//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad rseq [%u]", packet[34]);
        	return SG_PACKT_RCVSQ_BAD;
        }
        
        //logMessage(LOG_ERROR_LEVEL, "[ERROR] reached");
        	
             	        	       	
        if(dataind1 == 0){
        
        	if(plen == SG_BASE_PACKET_SIZE){
        		memcpy(&magic, &packet[37], 4);
        		//logMessage(LOG_ERROR_LEVEL, "copied magic");
        		
        		if(magic == SG_MAGIC_VALUE){
        			//logMessage(LOG_ERROR_LEVEL, "magic okay");
        			return SG_PACKT_OK;
        		}
        		
        		if(magic != SG_MAGIC_VALUE){
        			//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad magic [%u]", magic);
        			return SG_PACKT_PDATA_BAD;
        		}
        	}
        	
        	//if(plen != SG_BASE_PACKET_SIZE){
        	//	logMessage(LOG_ERROR_LEVEL, "base packet size error");
        	//	return SG_PACKT_BLKLN_BAD;
        	//}
        }
        
        if(dataind1 == 1){
      		if(data == NULL){
      			//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad data");
      			return SG_PACKT_BLKDT_BAD;
      		}
      		
      		if(plen == SG_DATA_PACKET_SIZE){
      			memcpy(data, &packet[37], SG_BLOCK_SIZE);
			//logMessage(LOG_ERROR_LEVEL, "copied data");
        		memcpy(&magic, &packet[1061], 4);
        		//logMessage(LOG_ERROR_LEVEL, "copied magic");
        	
        		
        		if(magic == SG_MAGIC_VALUE){
        			//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: good magic [%u]", magic);
        			return SG_PACKT_OK;
        		}
        		if(magic != SG_MAGIC_VALUE){
        			//logMessage(LOG_ERROR_LEVEL, "[ERROR] deserialize_sg_packet: bad magic [%u]", magic);
        			return SG_PACKT_PDATA_BAD;
        		}
        	}
        	
        	if(plen != SG_DATA_PACKET_SIZE){
        		//logMessage(LOG_ERROR_LEVEL, "bad plen");
        		return SG_PACKT_BLKLN_BAD;
        	}
        }
      
    return SG_PACKT_OK;	
	


    // Return the system function return value
    //return( 0 );
}

//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint( void ) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    // Local and do some initial setup
    logMessage( LOG_INFO_LEVEL, "Initializing local endpoint ..." );
    
    sgLocalSeqno = SG_INITIAL_SEQNO;

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID
                                    SG_NODE_UNKNOWN,   // Remote ID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_INIT_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Sanity check the return value
    if ( loc == SG_NODE_UNKNOWN ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc );
        return( -1 );
    }

    // Set the local node ID, log and return successfully
    sgLocalNodeId = loc;
    //sgRemoteSeqno = srem;
    //logMessage(LOG_ERROR_LEVEL, "%d", srem);
    
    logMessage( LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId );
    return( 0 );
}

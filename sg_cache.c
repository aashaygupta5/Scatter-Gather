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
#include <stdlib.h>
#include <cmpsc311_log.h>
#include <sys/time.h>
#include <string.h>

// Project Includes
#include <sg_cache.h>

// Defines
//The data structure to hold the cache data
struct cachedata{   
	int line;
	SG_Node_ID remoteid[SG_MAX_BLOCKS_PER_FILE];
	SG_Node_ID blockid[SG_MAX_BLOCKS_PER_FILE];
	SGDataBlock *datablock;
	uint32_t time;
	uint32_t size;
};

uint32_t maxelem = SG_MAX_CACHE_ELEMENTS;

// Functional Prototypes

static struct cachedata *cache = NULL;

//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache( uint16_t maxElements ) {
    
    maxelem = maxElements;
    cache = (struct cachedata*) malloc(sizeof(struct cachedata) * maxelem);
    
    if(cache != NULL){
    	cache->time = 0;
    	cache->size = 0;
    	
    	int i;
    	for(i = 0; i < maxElements; i++){
    		cache->remoteid[i] = 0;
    		cache->blockid[i] = 0;
    	}
    	cache->datablock = NULL;
    }
    
    else{
    	return -1;
    }
    
    
    
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache( void ) {

    free(cache);   //frees the allocated memory
    
     
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock( SG_Node_ID nde, SG_Block_ID blk ) {

    struct cachedata * get;
    
    int i;
    for(i = 0; i < cache->size; i++){
        //looking for remote id in cache
    	if(cache->remoteid[i] == nde){
    		get = &cache[i];   //if remote id found
    		break;
    	}
    	else{
    		get = NULL;    //if not found
    	}
    	
    	if(get == NULL){
    		return NULL;
    	}
    	
    }
    
    return get->datablock;  //returning the datablock
    
    // Return successfully
    //return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock( SG_Node_ID nde, SG_Block_ID blk, char *block ) {

    struct cachedata * put;
    
    int i;
    for(i = 0; i < cache->size; i++){
        //looking for remote id in cache
    	if(cache->remoteid[i] == nde){
    		put = &cache[i];   //if remote id found
    		break;
    	}
    	else{
    		put = NULL;    //if not found
    	}
    }
    
    //if(put != NULL)
    
    if(put == NULL){
    	if(cache->size != SG_MAX_CACHE_ELEMENTS){
    		put = &cache[cache->size];
    		cache->size += 1;
    	}
    	else{
    		if(cache->size == 0){
    			put = NULL;
    			return -1;
    		}
    		
    		int j;
    		for(j = 0; j < cache->size; j++){
    			if(put == NULL){
    				put = &cache[j];
    			}
    			else if(put->time > cache[j].time){
    				put = &cache[j];
    			}
    		}
    	}
    	
    	put->time += 1;    //increasing the time after putting the data
    	//put->remoteid = nde;
    	//put->blockid = blk;
    	memcpy(put->datablock, block, 512);   //copying data to cache
    	
    }
    
    // Return successfully
    return( 0 );
}

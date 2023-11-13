#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h> //for thread handling
#include <unistd.h>	//for usleep

//header files for queue and dnslookup
#include "queue.h"
#include "util.h"

//constants
#define MINARGS 3 
#define USAGE "<inputFilePath> <outputFilePath>"
#define MAX_RESOLVER_THREADS 10
#define MAX_NAME_LENGTH 1025
#define INPUTFS "%1024s"
#define MAX_IP_LENGTH INET6_ADDRSTRLEN

struct threadArgs{ //hold necessary member variables to perform thread safe operations

	FILE* inputFile; //FILE pointer for input file
	FILE* outputFile; //FILE pointer for output file
	queue fifoQueue; //queue storing urls

	//shared resources must be protected by mutex
	pthread_mutex_t queueLock;
	pthread_mutex_t fileLock;

	//command line arguments
	char** argv;
	int argc;

};

//used to signal when all requests have been processed
int allRequestProcessed = 0;

void* requestThread(void* inputFileName){

	struct threadArgs *args = (struct threadArgs*) inputFileName; 
	char buff[MAX_NAME_LENGTH]; //will hold a url at a time from inputFile
	char* url; //hold the url from buff

	//open input file for reading, -2 to exclude executable and output file
	int numInputFiles = args->argc-2;
	for(int i = 1; i <= numInputFiles; i++){ //loop through all input files
		//open input file for reading
		args->inputFile = fopen(args->argv[i], "r");
		//if bogus input file exit
	    if(!args->inputFile){
			perror("Bogus input file path");
			return (void*)EXIT_FAILURE;
    	}
    	//fscanf is memory safe, will not read more than 1024 characters
		//read in a line from input file into buff
		while(fscanf(args->inputFile, INPUTFS, buff) > 0){
            	pthread_mutex_lock(&args->queueLock); //acquire lock so other threads cannot access queue
            	while(queue_is_full(&args->fifoQueue)){ //if queue is full
                	pthread_mutex_unlock(&args->queueLock); //release lock
                	usleep(rand() % 100); //random sleep period between 0 and 100 microseconds
                	pthread_mutex_lock(&args->queueLock); //acquire lock
            	}
            	if(!queue_is_full(&args->fifoQueue)){ //if queue is not full
                	url = malloc(MAX_NAME_LENGTH); //dynamatically allocate memory for url so it can be pushed to queue
                	strncpy(url, buff, MAX_NAME_LENGTH); //copy buff into url 
                	queue_push(&args->fifoQueue, url); //push url into queue
                	pthread_mutex_unlock(&args->queueLock); //release lock so other threads can access it
            	}
    	}
		fclose(args->inputFile);//close current input file and move on to the next if it exists
	}
	return NULL;
}


void* resolverThread(void* inputFileName){
	struct threadArgs *args = (struct threadArgs*) inputFileName; 
	char* buff;	//will hold a url at a time from fifoQueue
	char ip[MAX_IP_LENGTH]; //holds ip address

	//loop until queue is empty or all requests have been processed
	while(!queue_is_empty(&args->fifoQueue) || !allRequestProcessed){
		pthread_mutex_lock(&args->queueLock); //acquire lock so other thread cannot access queue
		if(!queue_is_empty(&args->fifoQueue)){ //if queue is not empty
			buff = queue_pop(&args->fifoQueue); //popping url from queue to buff
       		pthread_mutex_unlock(&args->queueLock); //release lock so other threads can access queue
			if(buff){
				//generates ip from given url
      			if(dnslookup(buff, ip, sizeof(ip)) == UTIL_FAILURE){
        			fprintf(stderr, "dnslookup error: %s\n", buff);
        			strncpy(ip, "", sizeof(ip));
      			}
			    pthread_mutex_lock(&args->fileLock); //acquire lock on file to have exclusive access
			    fprintf(args->outputFile, "%s,%s\n", buff, ip); //print format: hostname(url),ip(blank if bogus)
			    pthread_mutex_unlock(&args->fileLock); //unlock file so other threads can access it
				free(buff); //deallocate memory for buff
			}
		}else{ //otherwise queue is empty
			pthread_mutex_unlock(&args->queueLock); //release lock for other threads to access queue
		}
	}
	return NULL;
}

int main(int argc, char* argv[]){
	struct threadArgs args;  
	args.argv = argv; //array of arguments
	args.argc = argc; //num arguments
    pthread_mutex_init(&args.queueLock, NULL); //initialize queue threads
    pthread_mutex_init(&args.fileLock, NULL); //initialize file threads
    queue_init(&args.fifoQueue, QUEUEMAXSIZE); //queue max size is 50 

    pthread_t requestThreads; //1 thread per input file
    pthread_t resolverThreads[MAX_RESOLVER_THREADS];

    //check for valid number of arguments
    //at least 3 fields: executable, input file, output file
    if(argc < MINARGS){
		fprintf(stderr, "Not enough arguments: %d\n", (argc - 1));
		fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
		return EXIT_FAILURE;
    }
	
	//open output file for writing which is the last argument
	args.outputFile = fopen(argv[(argc-1)], "w");
    //if bogus output file exit
    if(!args.outputFile){
		perror("Bogus output file path");
		return EXIT_FAILURE;
    }

	//create request threads
	pthread_create(&requestThreads, NULL, requestThread, (void*)&args);

	//create resolver threads
    for(int i = 0; i < MAX_RESOLVER_THREADS; i++)
    	pthread_create(&resolverThreads[i], NULL, resolverThread, (void*)&args);
    
	//wait for request threads to finish
    pthread_join(requestThreads,  NULL);

	//signal resolver threads that all requests have been processed
    allRequestProcessed = 1; 

	//wait for resolver threads to finish
    for(int i = 0; i < MAX_RESOLVER_THREADS; i++)
    	pthread_join(resolverThreads[i], NULL);

    //destroy mutexes
    pthread_mutex_destroy(&args.queueLock);
	pthread_mutex_destroy(&args.fileLock);

	//close output file
    fclose(args.outputFile);
	//free queue memory
    queue_cleanup(&args.fifoQueue);
}
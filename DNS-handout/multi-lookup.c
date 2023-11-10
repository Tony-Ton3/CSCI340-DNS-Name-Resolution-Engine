//dependencies
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>	

//header files for queue and dnslookup
#include "queue.h"
#include "util.h"

#define MIN_ARGS 3 //at least 3 fields: executable, input file, output file
#define MAX_RESOLVER_THREADS 10
#define MAX_NAME_LENGTH 1025
#define USAGE "<inputFilePath> <outputFilePath>"
#define INPUTFS "%1024s"


// Global variables
queue hostnameQueue;
pthread_mutex_t queueLock;
pthread_mutex_t fileLock;
FILE* outputfp = NULL;		//Holds the output file
int requestThreadsFinished = 0;

void* requestThread(void* inputFileName)
{
	char hostname[MAX_NAME_LENGTH];	//Holds the individual hostname
	char* payload;
	FILE* inputFile = fopen(inputFileName, "r");

	/* Read File and Process*/
	while(fscanf(inputFile, INPUTFS, hostname) > 0)
	{
		while(1){
			pthread_mutex_lock(&queueLock);
			if(queue_is_full(&hostnameQueue)){
				pthread_mutex_unlock(&queueLock);
				usleep( rand() % 100 );
			}else{ //queue is not full
				payload = malloc(MAX_NAME_LENGTH);
				strncpy(payload, hostname, MAX_NAME_LENGTH); 
				queue_push(&hostnameQueue, payload);
				pthread_mutex_unlock(&queueLock);
				break;
			}
		}
	}
	/* Close Input File */
	fclose(inputFile);
	return NULL;
}

void* resolverThread()
{
	char* hostname;	//Should contain the hostname
	char firstipstr[INET6_ADDRSTRLEN]; //Holds the resolved IP address

	while(!queue_is_empty(&hostnameQueue) || !requestThreadsFinished)
	{
		pthread_mutex_lock(&queueLock);
		if(!queue_is_empty(&hostnameQueue))
		{
			hostname = queue_pop(&hostnameQueue);

			if(hostname != NULL)
			{
				pthread_mutex_unlock(&queueLock);

				int dnsRC = dnslookup(hostname, firstipstr, sizeof(firstipstr));

				if(dnsRC == UTIL_FAILURE)
			    {
					fprintf(stderr, "dnslookup error: %s\n", hostname);
					strncpy(firstipstr, "", sizeof(firstipstr));
			    }
			    pthread_mutex_lock(&fileLock);
				fprintf(outputfp, "%s,%s\n", hostname, firstipstr);
				pthread_mutex_unlock(&fileLock);
			}
			free(hostname);
		}
		else	//Queue is empty
		{
			pthread_mutex_unlock(&queueLock);
		}
	}
	return NULL;
}

int main(int argc, char* argv[])
{
	int numFiles = argc - 2;
    /* Local Vars */

    pthread_mutex_init(&queueLock, NULL);
    pthread_mutex_init( &fileLock, NULL);

    queue_init(&hostnameQueue, 50);
    
    pthread_t requestThreads[argc-1];
    pthread_t resolverThreads[MAX_RESOLVER_THREADS];

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);


    //check for valid number of arguments
    if(argc < MIN_INPUT_FILES){
		fprintf(stderr, "Not enough arguments: %d\n", (argc - 1));
		fprintf(stderr, "Usage:\n %s %s\n", argv[0], USAGE);
		return EXIT_FAILURE;
    }

    //open output file
    outputfp = fopen(argv[(argc-1)], "w");
    if(!outputfp)
    {
		perror("Error Opening Output File");
		return EXIT_FAILURE;
    }

    //loop throught inputfiles
    for(int i=1; i<(argc-1); i++)	//Allocate 1 thread for every iteration here
    {
		int rc = pthread_create(&requestThreads[i-1], &attr, requestThread, argv[i]);
		if(rc)
		{
			printf("Request thread broke\n");
		}
    }

    /* Create resolver threads */
    for(int i = 0; i < MAX_RESOLVER_THREADS; ++i)
    {
    	int rc = pthread_create(&resolverThreads[i], &attr, resolverThread, NULL);
    	if(rc)
    	{
    		printf("Resolver thread broke\n");
    	}
    }

    /* Join on the request threads */
    for(int i = 0; i < numFiles; ++i)
    {
    	int rc = pthread_join( requestThreads[i],  NULL);
    	if(rc)
    	{
    		printf("Request thread broke");
    	}
    }
    requestThreadsFinished = 1;

    /* Join on the resolver threads */
    for(int i = 0; i < MAX_RESOLVER_THREADS; ++i)
    {
    	int rc = pthread_join( resolverThreads[i], NULL);
		if(rc)
    	{
    		printf("Resolver thread broke");
    	}    
    }

    /* Close Output File */
    fclose(outputfp);
    queue_cleanup(&hostnameQueue);
    /* Destroy mutexes */
    pthread_mutex_destroy(&queueLock);
	pthread_mutex_destroy( &fileLock);

    return EXIT_SUCCESS;
}
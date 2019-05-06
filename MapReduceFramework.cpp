#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>

struct ThreadContext {

};


typedef struct {
    JobState state;
    pthread_t* threads;
    ThreadContext* contexts;
    InputVec& inputVec;
    OutputVec& outputVec;
    std::atomic<int> atomic_index;
}JobContext;

void* runit(void* args)
{

}

void executeJob(int multiThreadLevel,const InputVec& inputVec){
    //Init the context:
    pthread_t threads[multiThreadLevel];
    std::atomic<int> atomic_index(0);
    JobState state = {UNDEFINED_STAGE, 0.0};
    ThreadContext contexts[multiThreadLevel];
    int old = 0;
    while(old < multiThreadLevel)
    {
        old = atomic_index.fetch_add(1);
        pthread_create(threads + old, nullptr, runit, contexts + old);
    }
    }

}



/*
 * The function produces a (K2*, V2*) pair
 * */
void emit2 (K2* key, V2* value, void* context){
    //TODO: Gets called by map function of client. Sort the values. Activate Barrier?

}

/*
 * The function produces a (K3*, V3*) pair/
 * */
void emit3 (K3* key, V3* value, void* context){
    //TODO: Gets called by the reduce function of client. Save the values into the output Vector.
}

/*
 * This function starts running the MapReduce Algorithm.
 * client - The task the framework should run
 * inputVec- a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
 * outputVec- a vector of type std::vector<std::pair<K3*, V3*>>
 * multiThreadLevel- the number of worker threads to be used for running the algorithm
 * */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //TODO: Create the struct with the context of the job. Start each thread with the map function.
    executeJob(multiThreadLevel,inputVec);
}
/*
 * The function gets the job handle returned by startMapReduceJob and waits until its finished
 * */
void waitForJob(JobHandle job);

/*
 * The function gets a job handle and checks for its current state in a given JobState struct
 * */
void getJobState(JobHandle job, JobState* state);

/*
 * Releases all resources of a job. After calling, job will be invalid.
 * */
void closeJobHandle(JobHandle job);







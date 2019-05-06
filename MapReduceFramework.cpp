#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>
#include <algorithm>


typedef struct JobContext {
    JobState state;
    pthread_t* threads;
    const InputVec& inputVec;
    std::atomic<int>* atomic_index;
    OutputVec& outputVec;
    const int mTL;
    const MapReduceClient& client;
    std::vector<std::vector<std::pair<K2*,V2*>>> intermediaryVec;

}JobContext;


struct ThreadContext {
    int threadID;
    JobContext *context;
};


void* runThread(void* threadContext){

    auto* tC= (ThreadContext*)threadContext;
    InputVec inVec = tC->context->inputVec;
    auto old = (size_t)tC->context->atomic_index->fetch_add(1);
    //Map Phase:
    while (old < inVec.size()){
        std::pair<K1*,V1*> kv = inVec.at(old);
        tC->context->client.map(kv.first, kv.second, threadContext);
        old = (size_t)tC->context->atomic_index->fetch_add(1);
    }
    //Sort Phase:
    std::sort();



}

void executeJob(JobContext* context){

    std::vector <void*> arr;
    for (int i = 0; i < context->mTL; ++i)
    {
        arr.push_back(new ThreadContext{i,context});
        pthread_create(context->threads + i, nullptr, runThread, arr[i]);

    }
    //delete all

}



/*
 * The function produces a (K2*, V2*) pair
 * */
void emit2 (K2* key, V2* value, void* context){
    auto tC = (ThreadContext*) context;
    tC->context->intermediaryVec.at((size_t)tC->threadID).push_back({key,value});
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
                            InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //TODO: Create the struct with the context of the job. Start each thread with the map function.
    //Init the context:
    JobState state = {UNDEFINED_STAGE, 0.0};
    pthread_t threads[multiThreadLevel];
    auto *atomic_index = new std::atomic<int>(0);
    JobContext context = {state, threads, inputVec, atomic_index, outputVec, multiThreadLevel, client};


    executeJob(&context);
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







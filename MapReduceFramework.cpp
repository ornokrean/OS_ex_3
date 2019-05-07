#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>
#include <algorithm>
#include <iostream>
#include "Barrier.h"
#include <semaphore.h>

typedef struct JobContext
{
    JobState state;
    pthread_t *threads;
    const InputVec &inputVec;
    std::atomic<int> *atomic_index;
    OutputVec &outputVec;
    int mTL;
    const MapReduceClient &client;
    std::vector<IntermediateVec> *intermediaryVecs;
    //
    std::vector<IntermediateVec> *reduceVecs;
    // Barrier for after the sort phase
    Barrier *barrier;
    //Indicates the shuffle phase has finished
    bool finishedShuffle = false;
    //Indicates the job has finished
    bool finishedRun = false;
    //Mutex for the vectors worked on
    pthread_mutex_t *vecMutex;
    //Mutex for the state
    pthread_mutex_t *stateMutex;
    sem_t *sem;


    JobContext(JobState state, pthread_t *threads, const InputVec &inputVec, std::atomic<int>
    *atomic_index, OutputVec &outputVec, const int mTL,
               const MapReduceClient &client,
               std::vector<IntermediateVec> *intermediaryVecs, std::vector<IntermediateVec> *reduceVec,
               Barrier *barrier,
               pthread_mutex_t *vecMutex, pthread_mutex_t *stateMutex, sem_t *sem) : state(state), threads(threads),
                                                                                     inputVec(inputVec),
                                                                                     atomic_index(atomic_index),
                                                                                     outputVec(outputVec),
                                                                                     mTL(mTL), client(client),
                                                                                     intermediaryVecs(intermediaryVecs),
                                                                                     reduceVecs(reduceVec),
                                                                                     barrier(barrier),
                                                                                     vecMutex(vecMutex),
                                                                                     stateMutex(stateMutex), sem(sem) {}

} JobContext;


struct ThreadContext
{
    int threadID;
    JobContext *context;
};

int compare(IntermediatePair first, IntermediatePair second)
{
    return first.first < second.first;
}

void shuffle(void *context)
{
    auto jC = (JobContext *) context;
    int x = 1;

    auto *emptyVecs = new std::vector<int>;


    while (x)
    {
        //Is this actually necessary?
        //Find the maximal key
        K2 *max = nullptr;

        // Get an initial max key - to validate not working with a null key
        for (auto &vec: *jC->intermediaryVecs)
        {
            if (!vec.empty())
            {
                max = vec.back().first;
                break;
            }
        }
        if (max == nullptr)
        {
            //There are no pairs!!!!
        }
        //Find the max between the last elements of each intermediary vector:
        for (auto &vec: *jC->intermediaryVecs)
        {
            if (!vec.empty())
            {
                if (max < vec.back().first)
                {
                    max = vec.back().first;
                }
            }
        }
        IntermediateVec *maxVec;
        for (auto &vec:*jC->intermediaryVecs){
            if (!vec.empty()){
                if (!(max<vec.back().first) && !(vec.back().first<max)){

                }
            }

        }



//        for (int i = 0; i < jC->mTL; ++i)
//        {
//            //TODO: Fix the damage I created:
//
//            if (jC->intermediaryVecs[i].back().first > max)
//            {
//                max = jC->intermediaryVecs[i].back().first;
//            }
//        }
//        // for each max
//        IntermediateVec vec;
//        for (int j = 0; j < jC->mTL; ++j)
//        {
//            if (!(*jC->intermediaryVecs[j].back().first < *max) && !(*max < *jC->intermediaryVecs[j]
//                    .back().first))
//            {
//                vec.push_back(jC->intermediaryVecs[j].back());
//                jC->intermediaryVecs[j].pop_back();
//            }
//        }
//        jC->reduceVecs.push_back(vec);
    }
}


void *runThread(void *threadContext)
{

    auto *tC = (ThreadContext *) threadContext;
    InputVec inVec = tC->context->inputVec;
    auto tID = (size_t) tC->threadID;
    auto old = (size_t) tC->context->atomic_index->fetch_add(1);
    //Map Phase:
    while (old < inVec.size())
    {
        std::cout << "Thread number " << tID << " accessing inputVec at: " << old << "\n";
        InputPair kv = inVec.at(old);
        tC->context->client.map(kv.first, kv.second, threadContext);
        std::cout << kv.first << "   " << kv.second << "\n";
        old = (size_t) tC->context->atomic_index->fetch_add(1);
    }
    //Sort Phase:
    std::sort(tC->context->intermediaryVecs.at(tID).begin(),
              tC->context->intermediaryVecs.at(tID).end(), compare);
    //Barrier:
    tC->context->barrier->barrier();

    //Shuffle:
    if (tC->threadID == 0)
    {
        shuffle(tC->context);
    }
    //Reduce:
    //Access reduceVecs at the first free spot and run it

    return nullptr;
}

void executeJob(JobContext *context)
{
    std::vector<void *> arr;
    //TODO: Fix thread number issue
    for (int i = 0; i < context->mTL; ++i)
    {
        arr.push_back(new ThreadContext{i, context});
        pthread_create(context->threads + i, nullptr, runThread, arr[i]);
    }
    //delete all
}


/*
 * The function produces a (K2*, V2*) pair
 * */
void emit2(K2 *key, V2 *value, void *context)
{
    auto tC = (ThreadContext *) context;
    tC->context->intermediaryVecs.at((size_t) tC->threadID).push_back({key, value});
}

/*
 * The function produces a (K3*, V3*) pair/
 * */
void emit3(K3 *key, V3 *value, void *context)
{
    //TODO: Gets called by the reduce function of client. Save the values into the output Vector.
}

/*
 * This function starts running the MapReduce Algorithm.
 * client - The task the framework should run
 * inputVec- a vector of type std::vector<std::pair<K1*, V1*>>, the input elements
 * outputVec- a vector of type std::vector<std::pair<K3*, V3*>>
 * multiThreadLevel- the number of worker threads to be used for running the algorithm
 * */
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel)
{
    //TODO: Create the struct with the context of the job. Start each thread with the map function.
    //Init the context:
    JobState state = {UNDEFINED_STAGE, 0.0};
    pthread_t threads[multiThreadLevel];
    auto *atomic_index = new std::atomic<int>(0);
    auto *intermediaryVecs = new std::vector<IntermediateVec>((size_t) multiThreadLevel);
    auto *reduceVecs = new std::vector<IntermediateVec>((size_t) multiThreadLevel);
    auto *barrier = new Barrier(multiThreadLevel);
    auto *vecMutex = new pthread_mutex_t();
    auto *stateMutex = new pthread_mutex_t();
    auto *sem = new sem_t;
    auto context = new JobContext(state, threads, inputVec, atomic_index, outputVec,
                                  multiThreadLevel,
                                  client, intermediaryVecs, reduceVecs, barrier, vecMutex, stateMutex, sem);


    executeJob(context);
    return (JobHandle) context;
}

/*
 * The function gets the job handle returned by startMapReduceJob and waits until its finished
 * */
void waitForJob(JobHandle job)
{
    auto *jb = (JobContext *) job;

}

/*
 * The function gets a job handle and checks for its current state in a given JobState struct
 * */
void getJobState(JobHandle job, JobState *state) {}

/*
 * Releases all resources of a job. After calling, job will be invalid.
 * */
void closeJobHandle(JobHandle job) {}







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

/*
 * Compare Function for pairs
 * */
int compare(IntermediatePair first, IntermediatePair second)
{
    return first.first < second.first;
}

void reduce(void *context)
{
    //While there are still reduce vectors to work on
    //Addendum: Or shuffle phase hasn't finished, since we don't want to stop working while there are still possible
    //vectors that haven't been shuffled yet

    auto *tc = (ThreadContext *) context;
    //Vector reduced each step
    auto *reduceVec = new IntermediateVec;

    while (!tc->context->reduceVecs->empty() || !tc->context->finishedShuffle)
    {
        pthread_mutex_lock(tc->context->vecMutex);
        if (tc->context->reduceVecs->empty())
        {
            pthread_mutex_unlock(tc->context->vecMutex);
            //State: Shuffle hasn't finished but there are no vectors to work on yet
            continue;
        }
        //Get the vector to reduce:
        *reduceVec = tc->context->reduceVecs->back();
        tc->context->reduceVecs->pop_back();

        pthread_mutex_unlock(tc->context->vecMutex);

        tc->context->client.reduce(reduceVec, tc->context);




    }

    delete (reduceVec);
    reduceVec = nullptr;


}


void shuffle(void *context)
{
    auto jC = (JobContext *) context;
    int numOfEmptyVecs = 0;
    K2 *max = nullptr;
    auto *maxVec = new IntermediateVec;
    //Run while there are still non empty vectors:
    while (numOfEmptyVecs != jC->mTL)
    {
        //Is this actually necessary?
        //Find the maximal key


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
        // Create a vector for all pairs with max key:
        for (auto &vec:*jC->intermediaryVecs)
        {
            //Skip empty vectors
            if (vec.empty())
            {
                continue;
            }
            //Get all pairs with key max from the current vector
            while (!(max < vec.back().first) && !(vec.back().first < max))
            {
                maxVec->push_back(vec.back());
                vec.pop_back();
            }
            //Update empty vectors
            if (vec.empty())
            {
                numOfEmptyVecs++;
            }

        }

        // Lock the reduce Vector with a mutex: We dont want a thread accessing it while we are updating it
        pthread_mutex_lock(jC->vecMutex);
        jC->reduceVecs->emplace_back(*maxVec);
        pthread_mutex_unlock(jC->vecMutex);
        //TODO: IS this really necessary?
    }
    //Indicate Shuffle phase has finished:
    jC->finishedShuffle = true;

    delete (maxVec);
    maxVec = nullptr;
    delete (max); //TODO: Maybe this is troublesome, dunno
    max = nullptr;
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
    std::sort(tC->context->intermediaryVecs->at(tID).begin(),
              tC->context->intermediaryVecs->at(tID).end(), compare);
    //Barrier:
    tC->context->barrier->barrier();

    //Shuffle:
    if (tC->threadID == 0)
    {
        shuffle(tC->context);
    }
    //Reduce:
    reduce(threadContext);


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
    tC->context->intermediaryVecs->at((size_t) tC->threadID).push_back({key, value});
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
    auto *intermediaryVecs = new std::vector<IntermediateVec>((unsigned long) multiThreadLevel);
    auto *reduceVecs = new std::vector<IntermediateVec>((unsigned long) multiThreadLevel);
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







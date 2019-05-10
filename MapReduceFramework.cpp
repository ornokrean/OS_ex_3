#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>
#include <algorithm>
#include <iostream>
#include "Barrier.h"
#include <semaphore.h>


using namespace std;



struct ThreadContext;
typedef struct JobContext
{
    //State struct
    JobState state;
    //The threads for the current job
    pthread_t *threads;
    //Vector for thread contexts: Only used for safe deleting once the job is over
    std::vector<ThreadContext *> *threadContexts;
    //The Input vector
    const InputVec &inputVec;
    //Shared atomic index for the map phase
    std::atomic<int> *atomic_index;
    //Output vector
    OutputVec &outputVec;
    //Number of threads:
    int mTL;
    //The Client
    const MapReduceClient &client;
    //Vector of IntermediateVec for the results of map+sort phases
    std::vector<IntermediateVec> *intermediaryVecs;
    //Vector of IntermediateVec for the results of reduce phase
    std::vector<IntermediateVec> *reduceVecs;
    // Barrier for after the sort phase
    Barrier *barrier;

    //Mutex for the vectors worked on
    pthread_mutex_t *vecMutex;
    //Mutex for the state: Protecting change of key counts
    pthread_mutex_t *keyMutex;
    //Semaphore for the reduce phase-handling thread "queue"
    sem_t *sem;
    //Indicates the shuffle phase has finished
    bool finishedShuffle = false;
    //Indicates that thread joining was already done: prevents double calling
    bool joiningDone = false;
    //Number of processed keys in current stage
    int numOfProcessedKeys = 0;
    //Number of total keys to be processed in the current stage
    unsigned long numOfTotalKeys;


    JobContext(JobState state, pthread_t *threads, std::vector<ThreadContext *> *threadContexts,
               const InputVec &inputVec, std::atomic<int> *atomic_index, OutputVec &outputVec,
               const int mTL, const MapReduceClient &client,
               std::vector<IntermediateVec> *intermediaryVecs,
               std::vector<IntermediateVec> *reduceVec, Barrier *barrier,
               pthread_mutex_t *vecMutex, pthread_mutex_t *keyMutex, sem_t *sem,
               unsigned long totalKeys) :
            state(state), threads(threads), threadContexts(threadContexts), inputVec(inputVec),
            atomic_index(atomic_index), outputVec(outputVec), mTL(mTL), client(client),

            intermediaryVecs(intermediaryVecs), reduceVecs(reduceVec), barrier(barrier),
            vecMutex(vecMutex), keyMutex(keyMutex), sem(sem), numOfTotalKeys(totalKeys)
    {}

} JobContext;


struct ThreadContext
{
    int threadID;
    JobContext *context;

    ThreadContext(int threadID, JobContext *context) : threadID(threadID), context(context)
    {}

};

int getMaxVector(const JobContext *jC, int numOfEmptyVecs, const K2 *max, IntermediateVec &maxVec);

K2 *findMaxKey(const JobContext *jC, K2 *max);

/*
 * Compare Function for pairs
 * */
int compare(IntermediatePair first, IntermediatePair second)
{
    return *first.first < *second.first;
}

/*
 * Reduce Phase Handler
 * */
void reduce(void *context)
{
    //While there are still reduce vectors to work on
    //Addendum: Or shuffle phase hasn't finished, since we don't want to stop working while there
    // are still possible vectors that haven't been shuffled yet

    auto *tc = (ThreadContext *) context;
    //Vector reduced each step
    auto *toReduce = new IntermediateVec;

    while (!tc->context->reduceVecs->empty() || !tc->context->finishedShuffle)
    {
        sem_wait(tc->context->sem);
        pthread_mutex_lock(tc->context->vecMutex);
        if (tc->context->reduceVecs->empty())
        {
            pthread_mutex_unlock(tc->context->vecMutex);
            //State: Shuffle hasn't finished but there are no vectors to work on yet
            continue;
        }
        //Get the vector to reduce:

        *toReduce = tc->context->reduceVecs->back();
        tc->context->reduceVecs->pop_back();


        pthread_mutex_unlock(tc->context->vecMutex);

        tc->context->client.reduce(toReduce, tc->context);
        pthread_mutex_lock(tc->context->keyMutex);
        tc->context->numOfProcessedKeys += toReduce->size();
        pthread_mutex_unlock(tc->context->keyMutex);


    }

    delete (toReduce);
    toReduce = nullptr;
}

/*
 * Shuffle Phase Handler
 * */
void shuffle(void *context)
{
    auto jC = (JobContext *) context;
    int numOfEmptyVecs = 0;
    K2 *max = nullptr;
    IntermediateVec maxVec;
    //Run while there are still non empty vectors:
    while (numOfEmptyVecs < jC->mTL)
    {

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
            break;
            //There are no pairs!!!!
        }
        //Find the max between the last elements of each intermediary vector:
        max = findMaxKey(jC, max);
        // Create a vector for all pairs with max key:
        numOfEmptyVecs = getMaxVector(jC, numOfEmptyVecs, max, maxVec);
        // Lock the reduce Vector with a mutex: We don't want a thread accessing it while we are
        // updating it
        pthread_mutex_lock(jC->vecMutex);
        jC->reduceVecs->emplace_back(maxVec);
        pthread_mutex_unlock(jC->vecMutex);
        sem_post(jC->sem);
        maxVec.clear();
        max = nullptr;
    }


    //Indicate Shuffle phase has finished:
    jC->finishedShuffle = true;

    //Free all threads to work on reducing
    for (int i = 0; i < jC->mTL; ++i)
    {
        sem_post(jC->sem);
    }


    delete (max);
    max = nullptr;
}

/*
 * This function finds the max between the last elements of each intermediary vector given.
 *
 * */
K2 *findMaxKey(const JobContext *jC, K2 *max)
{
    for (auto &vec: *jC->intermediaryVecs)
    {
        if (!vec.empty())
        {
            if (*max < *vec.back().first)
            {
                max = vec.back().first;
            }
        }
    }
    return max;
}

/*
 * This function finds the max vector and Creates a vector for all pairs with max key
 * */
int getMaxVector(const JobContext *jC, int numOfEmptyVecs, const K2 *max, IntermediateVec &maxVec)
{
    for (auto &vec:*jC->intermediaryVecs)
    {
        //Skip empty vectors
        if (!vec.empty())
        {
            //Get all pairs with key max from the current vector as long as it has some
            while (!vec.empty() && !(*max < *vec.back().first) && !(*vec.back().first < *max))
            {
                maxVec.push_back(vec.back());
                vec.pop_back();
            }
            //Update empty vectors
            if (vec.empty())
            {
                numOfEmptyVecs++;
            }
        }
    }
    return numOfEmptyVecs;
}

/*
 * Map phase Handler
 * */
void map(void *threadContext)
{
    auto *tC = (ThreadContext *) threadContext;
    auto old = (size_t) tC->context->atomic_index->fetch_add(1);
    //Set the stage to Map if it isn't already
    if (tC->context->state.stage != MAP_STAGE)
    {
        tC->context->state.stage = MAP_STAGE;
    }
    while (old < tC->context->inputVec.size())
    {
        InputPair kv = tC->context->inputVec.at(old);
        tC->context->client.map(kv.first, kv.second, threadContext);
        old = (size_t) tC->context->atomic_index->fetch_add(1);
        pthread_mutex_lock(tC->context->keyMutex);
        tC->context->numOfProcessedKeys++;
        pthread_mutex_unlock(tC->context->keyMutex);
    }
}

/*
 * Run life of a thread:
 * */
void *runThread(void *threadContext)
{
    auto *tC = (ThreadContext *) threadContext;
    auto tID = (size_t) tC->threadID;
    //Map Phase:
    map(threadContext);
    //Sort Phase:
    std::sort(tC->context->intermediaryVecs->at(tID).begin(),
              tC->context->intermediaryVecs->at(tID).end(), compare);
    //Barrier:
    tC->context->barrier->barrier();
    //Shuffle:
    if (tC->threadID == 0)
    {
        //Update Total Number of keys to fit reduce stage:
        unsigned long totalKeys = 0;
        for (auto &vec : *tC->context->intermediaryVecs)
        {
            totalKeys += vec.size();
        }
        // Update state to Reduce stage
        tC->context->numOfProcessedKeys = 0;
        tC->context->numOfTotalKeys = totalKeys;
        tC->context->state.stage = REDUCE_STAGE;
        shuffle(tC->context);
    }
    //Reduce:
    reduce(threadContext);
    return nullptr;
}

/*
 * Starts all threads
 * */
void executeJob(JobContext *context)
{
    for (int i = 0; i < context->mTL; ++i)
    {
        auto *threadContext = new ThreadContext(i, context);
        context->threadContexts->push_back(threadContext);
        if (pthread_create(&context->threads[i], nullptr, runThread, threadContext))
        {
            std::cerr << "ERROR: Thread Creation Failed!!!!" << std::endl;
            exit(1);
        }
    }
}

/**
 * The function produces a (K2*, V2*) pair
 * */
void emit2(K2 *key, V2 *value, void *context)
{
    auto tC = (ThreadContext *) context;
    tC->context->intermediaryVecs->at((size_t) tC->threadID).emplace_back(key, value);
}

/**
 * The function produces a (K3*, V3*) pair/
 * */
void emit3(K3 *key, V3 *value, void *context)
{
    auto jc = (JobContext *) context;
    pthread_mutex_lock(jc->vecMutex);
    jc->outputVec.emplace_back(key, value);
    pthread_mutex_unlock(jc->vecMutex);
}

/**
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
    //Init the context:
    JobState state = {UNDEFINED_STAGE, 0.0};
    auto *threads = (pthread_t *) malloc(sizeof(pthread_t) * multiThreadLevel);
    auto *atomic_index = new std::atomic<int>(0);
    auto *intermediaryVecs = new std::vector<IntermediateVec>((unsigned long) multiThreadLevel);
    auto *reduceVecs = new std::vector<IntermediateVec>();
    auto *barrier = new Barrier(multiThreadLevel);
    //Mutexes:
    auto *vecMutex = new pthread_mutex_t();
    pthread_mutex_init(vecMutex, nullptr);
    auto *keyMutex = new pthread_mutex_t();
    pthread_mutex_init(keyMutex, nullptr);

    auto *sem = new sem_t;
    sem_init(sem, 0, 0);

    auto *threadContexts = new std::vector<ThreadContext *>((unsigned long) multiThreadLevel);
    auto totalKeys = inputVec.size();
    auto context = new JobContext(state, threads, threadContexts, inputVec, atomic_index, outputVec,
                                  multiThreadLevel,
                                  client, intermediaryVecs, reduceVecs, barrier, vecMutex, keyMutex,
                                  sem, totalKeys);
    executeJob(context);
    return (JobHandle) context;
}

/**
 * The function gets the job handle returned by startMapReduceJob and waits until its finished
 * */
void waitForJob(JobHandle job)
{
    auto *jc = (JobContext *) job;

    if (!jc->joiningDone)
    {
        for (int i = 0; i < jc->mTL; ++i)
        {
            if (pthread_join(jc->threads[i], nullptr))
            {
                std::cerr << "ERROR: Joining failed!!!" << std::endl;
                exit(1);
            }
        }
        jc->joiningDone = true;
    }
}


/**
 * The function gets a job handle and checks for its current state in a given JobState struct
 * */
void getJobState(JobHandle job, JobState *state)
{
    auto *jc = (JobContext *) job;
    float progress = 0;
    if (jc->numOfTotalKeys != 0)
    {
        progress = (float) jc->numOfProcessedKeys / jc->numOfTotalKeys;
    }
    else
    { progress = 0; }
    *state = {jc->state.stage, progress * 100};
}

/**
 * Releases all resources of a job. After calling, job will be invalid.
 * */
void closeJobHandle(JobHandle job)
{
    auto jc = (JobContext *) job;
    if (!jc->joiningDone)
    {
        waitForJob(job);
    }
    pthread_mutex_destroy(jc->keyMutex);
    pthread_mutex_destroy(jc->vecMutex);
    free(jc->threads);
    delete (jc->reduceVecs);
    delete (jc->intermediaryVecs);
    delete (jc->barrier);
    delete (jc->atomic_index);
    if (sem_destroy(jc->sem) == -1)
    {
        std::cerr << "ERROR: cannot close semaphore" << std::endl;
        exit(1);
    }
    for (auto item : *jc->threadContexts)
    {
        delete item;
    }
    delete (jc->threadContexts);
    delete (jc->sem);
    delete (jc->vecMutex);
    delete (jc->keyMutex);
    delete (jc);
}
#include <bits/stdc++.h>
#include <pthread.h>
#include <limits.h>

#define NUM_THREADS 32

using namespace std;

vector<vector<vector<int>>> partialLists;
// structura folosita pentru transmiterea parametrilot catre functia thread-urilor mapper
typedef struct mapperArgument{
    vector<string> inputList;
    vector<unordered_set<int>> powerList;
}mapperArgument;

pthread_barrier_t allBarrier;
pthread_mutex_t mapperReadMutex;
pthread_mutex_t mapperWriteMutex;

void *mapperFunction(void *arg) {
    // cast argument
    mapperArgument *mapperArg = (mapperArgument *)arg;
    
    // declarare variabile
    string inputFileName, inputFileName2;
    int n, i, j, auxInt, maxPower;
    vector<unordered_set<int>> powerList;
    vector<int> emptyVector;
    vector<vector<int>> partialList;
    
    // citire date din argument 
    pthread_mutex_lock(&mapperReadMutex);
    inputFileName = *mapperArg->inputList.begin();
    powerList = mapperArg->powerList;
    maxPower = powerList.size();
    mapperArg->inputList.erase(mapperArg->inputList.begin());
    pthread_mutex_unlock(&mapperReadMutex);

    // pregatire liste partiale
    for(i = 0; i < maxPower; i++)
        partialList.push_back(emptyVector);

    // procesare date
    // deschid fisierul de intrare asignat
    ifstream inputFile(inputFileName);
    inputFile >> n;    // citire numar de date
    // pentru fiecare numar verific daca este putere perfecta si in caz afirmativ, il salvez
    for (i = 0; i < n; i++){
        inputFile >> auxInt;
        for (j = 0; j < maxPower; j++){
            if (powerList[j].find(auxInt) != powerList[j].end())
                partialList[j].push_back(auxInt);
        }  
    }
    inputFile.close();

    // presupun ca mai sunt fisiere de intrare de procesat
    bool notEmpty = true;
    while (notEmpty){
        pthread_mutex_lock(&mapperReadMutex);
        if(mapperArg->inputList.empty()){
            // daca nu mai suntm trec mai departe
            notEmpty = false;
            pthread_mutex_unlock(&mapperReadMutex);
            break;
        }
        else{ // extrag un nou fisier de intrare
            inputFileName2 = *mapperArg->inputList.begin();
            mapperArg->inputList.erase(mapperArg->inputList.begin());
        }
        pthread_mutex_unlock(&mapperReadMutex);
        // procesare date
        // deschid fisierul de intrare asignat
        ifstream inputFile2(inputFileName2);
        inputFile2 >> n;    // citire numar de date
        // pentru fiecare numar verific daca este putere perfecta si in caz afirmativ, il salvez
        for (i = 0; i < n; i++){
            inputFile2 >> auxInt;
            for (j = 0; j < maxPower; j++){
                if (powerList[j].find(auxInt) != powerList[j].end())
                    partialList[j].push_back(auxInt);
            }  
        }
        inputFile2.close();
    }

    // folosesc un mutex pentru eviatea accesarii simultane de catre 2 thread-uri a aceleiasi zone 
    // de memorie la transmiterea listei partiale
    pthread_mutex_lock(&mapperWriteMutex);
    partialLists.push_back(partialList);
    pthread_mutex_unlock(&mapperWriteMutex);

    pthread_barrier_wait(&allBarrier);
    pthread_exit(NULL);
}

void *reducerFunction(void *arg) {
    // declarare variabile
    pthread_barrier_wait(&allBarrier);
    long threadID = *(long*)arg;
    int i, j;
    unordered_set<int> auxSet;
    string outputName = "out";

    // introduc valorile din listele partiale corespunzatoare puterii curente intr-un unordered_set 
    // pentru a elimina duplicatele
    for (i = 0; i < (int) partialLists.size(); i++)
        for(j = 0; j < (int) partialLists[i][threadID].size(); j++)
            auxSet.insert(partialLists[i][threadID][j]);
    // construiesc numele fisierului de iesiere si scriu rezultatul
    outputName += to_string(threadID + 2);
    outputName += ".txt";
    ofstream outputFile;
    outputFile.open(outputName);
    outputFile << auxSet.size();

    outputFile.close();
    pthread_exit(NULL);
}

int main(int argc, char *argv[]){
    // verific daca am primit suficiente agumente, altfel eroare
    if (argc < 4) {
        cout << "Numar insuficient de argumente\n";
        exit(-1);
    }

    // declarare variabile
    int n, i, j, r;
    int numberOfMappers = atoi(argv[1]);
    int numberOfRedducers = atoi(argv[2]);
    long id;
    void *status;
    string auxStr;
    vector<string> inputList;
    vector<unordered_set<int>> powerList;
    unordered_set<int> auxSet;
    pthread_t threads[NUM_THREADS];
    mapperArgument mapperArg;
    long ids[NUM_THREADS];

    // citire date din fisierul de intrare
    ifstream inputFile(argv[3]);
    inputFile >> n;  // numarul fisierelor ce trebuiesc procesate
    for( i = 0; i < n; i++) {
        // inroduc intr-un vector de stringuri fisierele ce trebuiesc procesate
        inputFile >> auxStr;
        inputList.push_back(auxStr);
    }

    // generare liste de puteri
    for (j = 2; j <= numberOfRedducers + 1; j++){
        i = 1;
        while (pow(i, j) <= INT_MAX) {
            auxSet.insert(pow(i, j));
            i++;
        }
        powerList.push_back(auxSet);
        auxSet.clear();
    }

    // completare argument pentru functia thread-urilor de tip mapper
    mapperArg.inputList = inputList;
    mapperArg.powerList = powerList;

    // initializare bariera si mutecsi
    pthread_barrier_init(&allBarrier, NULL, numberOfMappers + numberOfRedducers);
    pthread_mutex_init(&mapperReadMutex, NULL);
    pthread_mutex_init(&mapperWriteMutex, NULL);

    // pornire thread-uri
    for (id = 0; id < numberOfMappers + numberOfRedducers; id++) {
        if ( id < numberOfMappers)
            r = pthread_create(&threads[id], NULL, mapperFunction, (void*) &mapperArg);
        else { 
            ids[id - numberOfMappers] = id - numberOfMappers; 
            r = pthread_create(&threads[id], NULL, reducerFunction, &ids[id - numberOfMappers]);
        }
        if (r) {
            cout << "Eroare la pornirea thread-ului " << id << "\n";
            exit(-1);
        }
    }

    // asteptare thread-uri
    for (id = 0; id < numberOfMappers + numberOfRedducers; id++) {
        r = pthread_join(threads[id], &status);
        if (r) {
            cout << "Eroare la asteptarea thread-ului " << id << "\n";
            exit(-1);
        }
    }
    pthread_barrier_destroy(&allBarrier);
    pthread_mutex_destroy(&mapperReadMutex);
    pthread_mutex_destroy(&mapperWriteMutex);
    inputFile.close();
    return 0;
}
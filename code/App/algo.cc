#include<iostream>
#include<fstream>
#include<sstream>
#include<vector>
#include<unordered_map>
#include<unordered_set>
#include<queue>

const std::string kevin = "Diana Ross";

std::unordered_map<std::string, std::vector<std::string>> graph;

void readGraphFromTxt(std::string filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "err" << filename << std::endl;
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string actor, coactor;

        std::getline(iss, actor, ',');
        std::cout << actor << std::endl;
        while (std::getline(iss, coactor, ',')) {
            graph[actor].push_back(coactor);
            std::cout << coactor <<",";
        }
        std::cout << std::endl;
    }
    

    file.close();
};

int findDegree(std::string actor) {
    if (actor == kevin) {
        return 0;
    }

    std::queue<std::pair<std::string, int>> qActorWithDegree;
    std::unordered_set<std::string> visitedActors;

    qActorWithDegree.push({kevin, 0});

    while(!qActorWithDegree.empty()) {
        std::pair<std::string, int> current = qActorWithDegree.front();
        qActorWithDegree.pop();

        std::string currentActor = current.first;
        int currentDegree = current.second;

        for (std::string actorConnectedToCurrent : graph[currentActor]) {
            if (actorConnectedToCurrent == kevin) {
                return currentDegree;
            }

            if (visitedActors.find(actorConnectedToCurrent) == visitedActors.end()) {
                visitedActors.insert(actorConnectedToCurrent);
                qActorWithDegree.push({actorConnectedToCurrent, currentDegree + 1});
            }
        }
    }

    return -1;
};

int main() {
    readGraphFromTxt("o2.txt");
    //std::string userActor;

    //std::cout << "your actor: ";
    //std::cin >> userActor;
    auto userActor = "Michael Jackson";

    std::cout << "degree: " << findDegree(userActor) << std::endl;


    return 0;
};
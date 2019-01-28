#include <queue>
#include <cstdlib>
#include <vector>
#include <algorithm>
using namespace std;
int numCells(double endTime, double minSplit, double maxSplit)
{
    priority_queue<double, vector<double>, greater<double>> q;
    double one;
    double two;
    q.push(0.0);
    while (endTime >= q.top())
    {
        one = q.top() + minSplit + rand()*(maxSplit-minSplit)/RAND_MAX;
        two = q.top() + minSplit + rand()*(maxSplit-minSplit)/RAND_MAX;
        q.pop();
        q.push(one);
        q.push(two);
    }
    return q.size();
}

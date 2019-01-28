#include <iostream>
#include <vector>

using namespace std;

double max(double a, double b)
{
  if(a > b)
    return a;
  else
    return b;
}
pair<double,vector<int>> knapsack(int weightLimit, const vector<int> &weights, const vector<double> &values)
{
  int vecSize = weights.size();
  vector<vector<double>> cache(vecSize+1, vector<double>(weightLimit+1));
  for(int i = 0; i <= vecSize; i++)
  {
    for(int j = 0; j <= weightLimit; j++)
    {
      if(i == 0)
        cache[i][j] = 0;
      else if(weights[i-1] <= j)
        cache[i][j] = max(values[i-1] + cache[i-1][j-weights[i-1]], cache[i-1][j]);
      else
        cache[i][j] = cache[i-1][j];
    }
  }

  vector<int> solution;
  double points = cache[vecSize][weightLimit];
  for (int pos = vecSize; pos >= 1; pos--)
  {
    if (cache[pos - 1][weightLimit] < cache[pos][weightLimit])
    {
      solution.push_back (pos - 1);
      weightLimit -= weights[pos-1];
    }
  }
  return make_pair(points, solution);
}

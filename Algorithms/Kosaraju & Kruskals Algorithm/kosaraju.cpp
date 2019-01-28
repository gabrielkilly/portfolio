#include<iostream>
#include<vector>
#include<set>
#include<stack>
#include<algorithm>
using namespace std;

void indexByTiming(const vector<vector<int>> &roadTo, set<int> *visited, stack<int> *stack, int currNode)
{
  visited->insert(currNode);
  for(int pos = 0; pos < roadTo[currNode].size(); pos++)
  {
    if(visited->find(roadTo[currNode][pos]) == visited->end())
      indexByTiming(roadTo, visited, stack, roadTo[currNode][pos]);
  }
  stack->push(currNode);
}
vector<vector<int>> reverse(const vector<vector<int>> &roadTo)
{
    vector<vector<int>>reverseGraph(roadTo.size(), vector<int>(0));
    for(int x = 0; x < roadTo.size(); x++)
    {
      for(int y = 0; y < roadTo[x].size(); y++)
        reverseGraph[roadTo[x][y]].push_back(x);
    }
    return reverseGraph;
}
void findSet(const vector<vector<int>> &roadTo, set<int> *visited, set<int> *set, int currNode)
{
//  cout << currNode << " ";
  visited->insert(currNode);
  set->insert(currNode);
  for(int pos = 0; pos < roadTo[currNode].size(); pos++)
  {
    if(visited->find(roadTo[currNode][pos]) == visited->end())
      findSet(roadTo, visited, set, roadTo[currNode][pos]);
  }
}
vector<set<int>> drivingLegal(const vector<vector<int>> &roadTo)
{
  vector<set<int>> legalSets;
  set<int> visited;
  stack<int> stack;
  int currNode = 0;
  while(visited.size() < roadTo.size())
  {
    while(visited.find(currNode) != visited.end())
      currNode++;
    indexByTiming(roadTo, &visited, &stack, currNode);
  }

  visited.clear();
  vector<vector<int>>reverseRoadTo = reverse(roadTo);
  set<int> tempSet;

  while(!stack.empty())
  {
    currNode = stack.top();
    stack.pop();
    findSet(reverseRoadTo, &visited, &tempSet, currNode);
    while (!stack.empty() && visited.find(stack.top()) != visited.end())
      stack.pop();
    legalSets.push_back(tempSet);
    tempSet.clear();
  }
  return legalSets;

}

/*
int main()
{

  vector<vector<int> > roads1 = {{1},{2,4,5},{3,6},{2,7},{0,5},{6},{5,7},{7}};
  vector<set<int>> s = drivingLegal(roads1);
  for(int i = 0; i < s.size(); i++)
  {
    set<int>::iterator it;
    for (it = s[i].begin(); it != s[i].end(); ++it)
       cout << *it << " ";
   cout << "\n";
 }
  vector<vector<int> > roads2 = {{1},{2},{3},{0,5},
									{5,1,2,6,7},{6},{7},{5}};
  s = drivingLegal(roads2);
  for(int i = 0; i < s.size(); i++)
  {
    set<int>::iterator it;
    for (it = s[i].begin(); it != s[i].end(); ++it)
       cout << *it << " ";
   cout << "\n";
 }

  //reverseGraph Test
  vector<vector<int>> reverseRoads1 = reverse(roads1);
  for(int x = 0; x < reverseRoads1.size(); x++)
  {
    for(int y = 0; y < reverseRoads1[x].size(); y++)
      cout << reverseRoads1[x][y] << " ";
    cout << "\n";
  }
  return 0;
}*/

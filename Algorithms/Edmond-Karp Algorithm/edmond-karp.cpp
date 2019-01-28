#include <iostream>
#include <vector>
#include <tuple>
#include <set>
#include <queue>
#include <unordered_map>
#include <climits>
using namespace std;


bool bfs(int sinkIndex, const vector<vector<int>> &edges, unordered_map<int,int> *path)
{
  queue<int> vertexQueue;
  vector<int> visited(edges.size(), false);

  vertexQueue.push(0);
  while(!vertexQueue.empty()) {
    int curr = vertexQueue.front();
    vertexQueue.pop();
    for(int v = 0; v < sinkIndex + 1; v++) {
      int cap = edges[curr][v];
      if(curr == sinkIndex)
        return true;
      else if(cap > 0 and !visited[v]) {
        visited[v] = true;
        vertexQueue.push(v);
        path->insert({v, curr});
}  }  }
  return false;
}

vector<tuple<int,int,int>> courierLoads(const vector<int> &orders, const vector<tuple<int,int,int>> &capacities)
{
  int sinkIndex = orders.size();
  int sz = sinkIndex + 1;
  vector<vector<int>> edges(sz, vector<int>(sz, 0));
  //Make adjacency matric from input
  for(int cnt = 0; cnt < capacities.size(); cnt++) {
    int start, end, cap;
    tie(start, end, cap) = capacities[cnt];
    edges[start][end]= cap;
  }
  for(int c = 0; c < sz; c++)
    edges[c][sinkIndex] = orders[c];
  vector<vector<int>> edgesCopy{edges};
  vector<tuple<int,int,int>> flows;
  unordered_map<int, int> path;

  int maxFlow = 0;
  while(bfs(sinkIndex, edges, &path)) {
    int minFlow = INT_MAX;
    int rov = sinkIndex;
    while(rov != 0) {
      minFlow = min(edges[path[rov]][rov], minFlow);
      rov = path[rov];
    }
    rov = sinkIndex;
    maxFlow += minFlow;
    while(rov != 0) {
      edges[path[rov]][rov] = edges[path[rov]][rov] - minFlow;
      edges[rov][path[rov]] = edges[rov][path[rov]] + minFlow;
      rov = path[rov];
    }
    path.clear();
  }
  for(int j = 0; j < orders.size(); j++) {
    if(edges[j][sinkIndex] > 0)
      return {};
  }
  for(int i = 0; i < sz - 1; i++) {
    for(int j = 0; j < sz - 1; j++)
    {
      if(edgesCopy[i][j] > 0)
        flows.push_back(make_tuple(i, j, edges[j][i]));
    }
  }
  return flows;
}

// int main()
// {
//   vector<int> o1 = {0,1,2,3};
// 	vector<tuple<int,int,int>> c1 = {
// 		make_tuple(0,2,3),make_tuple(0,1,3),
// 		make_tuple(1,2,2),make_tuple(2,3,4)
// 	};
//
//   vector<int> o2 = {0,3,2,3};
// 	vector<tuple<int,int,int>> c2 = {
// 		make_tuple(0,2,5),make_tuple(0,1,2),
// 		make_tuple(1,2,2),make_tuple(2,3,4)
// 	};
//
//   vector<int> o3 = {0,1,2,3};
// 	vector<tuple<int,int,int>> c3 = {
// 		make_tuple(0,2,8),make_tuple(2,1,3),make_tuple(2,3,4)
// 	};
//   vector<tuple<int,int,int>> v = courierLoads(o3, c3);
//   cout << v.size() << "\n";
//   for(auto it : v)
//     cout << get<0>(it) << " " << get<1>(it) << " " << get<2>(it) << "\n";
//
//
// }

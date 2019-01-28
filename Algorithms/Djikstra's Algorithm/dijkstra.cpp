#include <iostream>
#include <vector>
#include <tuple>
#include <set>
#include <climits>
#include <queue>

using namespace std;

struct compare
 {
   bool operator()(const tuple<int, int> &l, const tuple<int, int> &r)
   {
       return get<0>(l) > get<0>(r);
   }
 };

tuple<vector<int>,vector<int>> distanceFromHome(const vector<vector<tuple<int,int>>> &nearbyDistances)
{
  int sz = nearbyDistances.size();
  vector<int> distances(sz, INT_MAX);
  vector<int> parents(sz, INT_MAX);
  priority_queue<tuple<int,int>, vector<tuple<int,int>>, compare> usableDistances;
  vector<bool> visited(sz, false);
  set<int> visits;
  int currVertex = 0;
  distances[0] = 0;
  usableDistances.push(make_tuple(0,0));
  parents[0] = -1;
  while(!usableDistances.empty())
  {
    usableDistances.pop();
    visited[currVertex] = true;
  //  visits.insert(currVertex);
    for(int j = 0; j < nearbyDistances[currVertex].size(); j++)
    {
        auto tup = nearbyDistances[currVertex][j];
        int v = get<0>(tup);
        int d = get<1>(tup);
        if(distances[currVertex] + d < distances[v])
        {
          distances[v] = distances[currVertex] + d;
          parents[v] = currVertex;
          usableDistances.push(make_tuple(distances[currVertex] + d, v));
        }

    }
    while(visited[get<1>(usableDistances.top())] && !usableDistances.empty()) {
      usableDistances.pop();
    }
    currVertex = get<1>(usableDistances.top());
  }
  return make_tuple(distances, parents);
}

int main()
{
  vector<vector<tuple<int,int>>> map2 =
		{{make_tuple(1,1)},
		 {make_tuple(2,1),make_tuple(3,2)},
		 {make_tuple(4,2),make_tuple(5,3)},
		 {make_tuple(4,3)},
		 {make_tuple(2,1),make_tuple(6,2)},
		 {make_tuple(0,1),make_tuple(6,5)},
		 {}};
	auto ret2 = distanceFromHome(map2);
	vector<int> d2 = {0,1,2,3,4,5,6};
	vector<int> p2 = {-1,0,1,1,2,2,4};
	if(d2!=get<0>(ret2)) {
		cout << "Distance error 2" << endl;
		return -1;
	}
	if(p2!=get<1>(ret2)) {
		cout << "Parent error 2" << endl;
		return -1;
	}
  cout << "\n";

  for(int x = 0; x < get<0>(ret2).size(); x++)
  {
    cout << "d: " << get<0>(ret2)[x] << "  p: " << get<1>(ret2)[x] << "\n";
  }

  vector<vector<tuple<int,int>>> map1 =
		{{make_tuple(1,1),make_tuple(2,3)},
		 {make_tuple(2,1),make_tuple(0,5)},
		 {make_tuple(0,3),make_tuple(1,3)}};
	auto ret1 = distanceFromHome(map1);
	vector<int> d1 = {0,1,2};
	vector<int> p1 = {-1,0,1};
	if(d1!=get<0>(ret1)) {
		cout << "Distance error 1" << endl;
		return -1;
	}
	if(p1!=get<1>(ret1)) {
		cout << "Parent error 1" << endl;
		return -1;
	}

  cout << "\n";
  for(int x = 0; x < get<0>(ret1).size(); x++)
  {
    cout << "d: " << get<0>(ret1)[x] << "  p: " << get<1>(ret1)[x] << "\n";
  }
}

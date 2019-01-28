#include <iostream>
#include <tuple>
#include <vector>
#include <cmath>
#include <math.h>
#include <algorithm>
#include <queue>
using namespace std;

template<typename T>
class DisjointSet
{
  private:
    class DisjointSet *findMarker(class DisjointSet *ds)
    {
      if(ds == ds->parent)
        return ds;
      class DisjointSet *par = findMarker(ds->parent);
      ds->parent = par;
      return par;
    }
  public:
    T dataInput;
    DisjointSet *parent;
    int rank;

    DisjointSet(const T &data)
    {
      dataInput = data;
      parent = this;
      rank = 0;
    }

    void unionSets(DisjointSet &ds)
    {
      class DisjointSet *thisMarker = findMarker(this);
      class DisjointSet *otherMarker = findMarker(&ds);

      if(thisMarker->rank >= otherMarker->rank)
      {
        if(thisMarker->rank == otherMarker->rank)
          thisMarker->rank++;
        otherMarker->parent = thisMarker->parent;
      }
      else
        thisMarker->parent = otherMarker->parent;
    }

    const T &getMarker()
    {
      class DisjointSet *marker = findMarker(this);
      return marker->dataInput;
    }
};

struct compare
 {
   bool operator()(const tuple<double, tuple<int, int>> &l, const tuple<double, tuple<int, int>> &r)
   {
       if(get<0>(l) == get<0>(r))
         return get<0>(get<1>(l)) > get<0>(get<1>(r));
       else
         return get<0>(l) > get<0>(r);
   }
 };
tuple<double,vector<tuple<int,int>>> sidewalkPlan(const vector<tuple<double, double> > &buildingLocations)
{
  int sz = buildingLocations.size();
  vector<DisjointSet<int> *> vertexes; //can maybe make faster
  priority_queue<tuple<double, tuple<int, int>>, vector<tuple<double,tuple<int, int>>>, compare> edges;
  for(int i = 0; i < sz; i++)
  {
    tuple<double, double> vertexPos = buildingLocations[i];
    vertexes.push_back(new DisjointSet<int>(i));
    for(int j = i; j < sz; j++)
    {
      if(i != j)
      {
        tuple<double, double> label = (i < j) ? make_tuple(i,j) : make_tuple(j, i);
        tuple<double, double> temp = buildingLocations[j];
        double points = sqrt(pow((get<0>(vertexPos) - get<0>(temp)), 2) + pow(get<1>(vertexPos) - get<1>(temp), 2));
        edges.push(make_tuple(points, label));
      }
    }
  }

  // while(!edges.empty())
  // {
  //   auto tup = edges.top();
  //   cout << get<0>(tup) << " (" << get<0>(get<1>(tup)) << "," << get<1>(get<1>(tup)) << ")" << "\n";
  //   edges.pop();
  // }

  int count = 0;
  vector<tuple<int, int>> ansIndices = {};
  double totalPoints = 0;
  while(count < sz - 1)
  {
      tuple<double, tuple<int, int>> edge = edges.top();
      edges.pop();
      tuple<int, int> indices = get<1>(edge);
      if(vertexes[get<0>(indices)]->getMarker() != vertexes[get<1>(indices)]->getMarker())
      {
        vertexes[get<0>(indices)]->unionSets(*vertexes[get<1>(indices)]);
        ansIndices.push_back(indices);
        totalPoints += get<0>(edge);
        count++;
      }
  }
  return make_tuple(totalPoints, ansIndices);
}

int main()
{
  vector<tuple<double,double> > locs1 = {make_tuple(0,0),make_tuple(1,0),make_tuple(-1,0),make_tuple(0,2),make_tuple(0,-2)};
  vector<tuple<double,double> > locs2 = {make_tuple(0,0),make_tuple(1,0),make_tuple(-1,0),make_tuple(0,2),make_tuple(0,-2),make_tuple(0.5,2),make_tuple(3,0),make_tuple(3,1)};

  auto tup = sidewalkPlan(locs1);
  auto tup1 = sidewalkPlan(locs2);
  cout << "Score = " << get<0>(tup) << "\n";
  auto indices = get<1>(tup);
  for(int i = 0; i < indices.size(); i++)
    cout << get<0>(indices[i]) << "," << get<1>(indices[i]) << " ";
  cout << "\n";

  cout << "Score = " << get<0>(tup1) << "\n";
  indices = get<1>(tup1);
  for(int i = 0; i < indices.size(); i++)
    cout << get<0>(indices[i]) << "," << get<1>(indices[i]) << " ";
  cout << "\n";

  for(int i=0; i<100; ++i) {
    vector<tuple<double,double> > locs;
    for(int j=0; j<200; ++j) {
      locs.push_back(make_tuple(rand()%1000,rand()%1000));
    }
    auto yourAns = sidewalkPlan(locs);
  }
}

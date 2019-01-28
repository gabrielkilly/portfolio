#include <iostream>
#include <vector>
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

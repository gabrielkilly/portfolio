#include <vector>
#include <tuple>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <functional>
#include <queue>
#include <string>
#include <numeric>
#include <cctype>
#include <iomanip>
#include <sstream>
#include <iomanip>
using namespace std;

tuple<vector<string>,vector<string>> breakBalance(const string &equ) {
  int k = equ.find("=");
  string s1 = equ.substr(0,k);
  string s2 = equ.substr(k+1,equ.length()-k);
  k = s1.find("+");
  vector<string> as1;
  while(k >= 0) {
    as1.push_back(s1.substr(0,k));
    s1 = s1.substr(k+1,s1.length()-k);
    k = s1.find("+");
  }
  as1.push_back(s1);
  k = s2.find("+");
  vector<string> as2;
  while(k >= 0) {
    as2.push_back(s2.substr(0,k));
    s2 = s2.substr(k+1,s2.length()-k);
    k = s2.find("+");
  }
  as2.push_back(s2);
  return make_tuple(as1,as2);
}

vector<vector<float>> createMatrix(tuple<vector<string>,vector<string>> &molecules) {
  vector<vector<float>> newvec;
  string checked = "";
  int col = 0;
  int nElems = get<0>(molecules).size()+get<1>(molecules).size();
  for(auto const m: get<0>(molecules)) {
    for(int i = 0; i < m.length(); i++) {
      if(!(isdigit(m[i]) || islower(m[i]))) {
        vector<float> tempvec(nElems,0);
        int row = 0;
        if(isupper(m[i])) {
          string elem(1,m[i]);
          int elemlength = 1;
          if(i+1 != m.length() && islower(m[i+1])) {
            elem += m[i+1];
            elemlength++;
          }
          cout << elem << "\n";
          if(checked.find(elem) >= 0) {
            for(auto const k: get<0>(molecules)) {
              int t = k.find(elem);
              if(t >= 0) {
                int n = 0;
                string num = "";
                while(islower(k[t+elemlength])){
                  t = k.find(elem,t+1);
                }
                while(isdigit(k[t+elemlength+n])) {
                  num += k[t+elemlength+n];
                  n++;
                }
                if(num.length()<1){
                  tempvec[row] = 1;
                } else {
                  tempvec[row] = stoi(num);
                }
              }
              row++;
            }
            for(auto const k: get<1>(molecules)) {
              int t = k.find(elem);
              if(t >= 0) {
                int n = 0;
                string num = "";
                while(isdigit(k[t+elemlength+n])) {
                  num += k[t+elemlength+n];
                  n++;
                }
                if(num.length()<1){
                  tempvec[row] = -1;
                } else {
                  tempvec[row] = stoi(num) * -1;
                }
              }
              row++;
            }
          }
          checked += elem;
        }
        newvec.push_back(tempvec);
      }
    }
  }
  return newvec;
}

void printmatrix(vector<vector<float>> &matrix) {
  int row = matrix.size();
  int col = matrix[0].size();

  for(int i=0; i < row; i++) {
    for(int k=0; k < col; k++) {
      cout << setw(7) << setprecision(4) << matrix[i][k] << " ";
    }
    cout << endl;
  }
  cout << endl;
}

vector<vector<float>> rowreduc(vector<vector<float>> &matrix) {
  int row = matrix.size();
  int col = matrix[0].size();

  int diag = 0;

  while(diag < row) {
    float diviser, multiplier;
    for(int i = 0; i < row; i++) {
      diviser = matrix[diag][diag];
      int n = 1;
      while(diviser == 0) {
        for(int k = 0; k < col; k++) {
          matrix[diag][k] += matrix[diag+n][k];
        }
        n++;
        diviser = matrix[diag][diag];
      }
      multiplier = matrix[i][diag] / diviser;
      for(int k = 0; k < col; k++) {
        if (i == diag) {
          matrix[i][k] /= diviser;
        } else {
          matrix[i][k] -= matrix[diag][k] * multiplier;
        }
      }
    }
    diag++;
    //printmatrix(matrix);
  }
  return matrix;
}

tuple<vector<int>, int> vecFormatter(vector<float> vec){
  int biggestNoAfterDecimal = 0;
  vector<int> sizes;
  vector<string> strs;
  for(auto elem : vec) {
    stringstream stream;
    stream << fixed << setprecision(4) << elem;
    string elemStr = stream.str();
    //string elemStr = to_string(elem);
    int decLoc = elemStr.find(".");
    if(decLoc != string::npos) {
      int sz = (elemStr.size() - 1) - decLoc;
      //cout << elem << " " << elemStr << " " << elemStr.size() << "\n";
      if(biggestNoAfterDecimal < sz)
        biggestNoAfterDecimal = sz;
      sizes.push_back(sz);
      strs.push_back(elemStr.erase(decLoc, 1));
    } else {
      sizes.push_back(0);
    }
  }
  vector<int> ints(sizes.size(), 0);
  for(int i = 0; i < sizes.size(); i++){
    ints[i] = stoi(strs[i]);
    cout << ints[i] << " " << sizes[i] << " " << "\n";
  }
  return make_tuple(ints, pow(10, biggestNoAfterDecimal));
}

int gcd(int a, int b) {
    if (a == 0 || b == 0)
        return 0;
    if (a == b)
        return a;
    if (a > b)
        return gcd(a-b, b);
    return gcd(a, b-a);
 }

int lcm(int a, int b) {
  return (a*b)/gcd(a, b);
}

vector<int> lowestMult(vector<int> vec, double divisor, vector<float> flts) {
  vector<int> vecCopy{vec};
  int l = vec[0];
  for(int i = 1; i < vec.size(); i++) {
    l = lcm(l, vec[i]);
  }
  for(int i = 0; i < vec.size(); i++){
    vecCopy[i] = (int)(flts[i] * (l / divisor) + 0.5);
  }
  return vecCopy;
}

tuple<vector<int>,vector<int>> chemBalance(const string &equ) {
  tuple<vector<string>,vector<string>> broke = breakBalance(equ);
  int nElems = get<0>(broke).size()+get<1>(broke).size();
  vector<vector<float>> mat = createMatrix(broke);
  vector<vector<float>> finalmat = rowreduc(mat);
  vector<float> vec(nElems,0);
  for(int i = 0; i < nElems-1; i++) {
    vec[i] = finalmat[i][nElems-1] * -1;
  }
  vec[nElems-1] = 1;
  auto tup = vecFormatter(vec);
  vector<int> newVec = lowestMult(get<0>(tup), get<1>(tup), vec);
  vector<int> vec1;
  vector<int> vec2;
  for(int i = 0; i < nElems; i++) {
    if(i < get<0>(broke).size()) {
      vec1.push_back(newVec[i]);
    } else {
      vec2.push_back(newVec[i]);
    }
  }
  return make_tuple(vec1,vec2);
}

int main() {
  vector<float> flts= {1.234, 2.3, 4, 234.23};
  auto tup1=  vecFormatter(flts);
  vector<int> ints = lowestMult(get<0>(tup1), get<1>(tup1), flts);
  cout << "\n";
  for(int i = 0; i < ints.size(); i++) {
    cout << ints[i] << "\n";
  }
  string f = "CH4+O2=H2O+CO";
  tuple<vector<string>,vector<string>> t = breakBalance(f);
  cout << get<0>(t).size()+get<1>(t).size() << "\n";
  cout << "first half of equation:\n";
  for(auto const c: get<0>(t)) {
    cout << c << "\n";
  }
  cout << "second half of equation:\n";
  for(auto const c: get<1>(t)) {
    cout << c << "\n";
  }
  auto newthing = createMatrix(t);
  auto newerthing = rowreduc(newthing);
  printmatrix(newerthing);
  tuple<vector<int>,vector<int>> tup = chemBalance(f);
  cout << "first half values: \n";
  for(auto const c: get<0>(tup)) {
    cout << c << "\n";
  }
  cout << "second half values: \n";
  for(auto const c: get<1>(tup)) {
    cout << c << "\n";
  }
  return 0;
}

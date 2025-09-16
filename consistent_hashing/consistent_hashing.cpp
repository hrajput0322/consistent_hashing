#include <iostream>
#include <vector>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <random>

using namespace std;

struct Bucket {
    int num_req = 0;
    bool server = false;
};

class RingSimulation {
    private:
        int n;
        vector<Bucket> ring;
        unordered_map<string, vector<int>> server_to_indexes;

        int hash_function(string& key) {
            const uint64_t OFFSET = 1469598103934665603ull;
            const uint64_t PRIME  = 1099511628211ull;
            const uint32_t RING   = this->n;

            string s0; s0.reserve(key.size()+2); s0.assign(key); s0 += "|0";
            string s1; s1.reserve(key.size()+2); s1.assign(key); s1 += "|1";
            string s2; s2.reserve(key.size()+2); s2.assign(key); s2 += "|2";

            uint64_t h = OFFSET;
            for (unsigned char c : key) { h ^= c; h *= PRIME; }
            int i = h % RING;

            return i;
        }

        int findReqOnServer(int index) {
            int store_index = index;
            int num_of_req = this->ring[index].num_req;
            index--;
            while (index>=0) {
                if (this->ring[index].server) {
                    return num_of_req;
                }
                num_of_req += this->ring[index].num_req;
                index--;
            }
            index = this->n - 1;
            while (index>=store_index) {
                if (this->ring[index].server) {
                    return num_of_req;
                }
                num_of_req += this->ring[index].num_req;
                index--;
            }
            return 0;
        }

    public:
        RingSimulation(int n) {
            this->n = n;
            this->ring.resize(n);
        }

        void addRequest(string& s) {
            int index = hash_function(s);
            this->ring[index].num_req++;
        }

        void addServer(string& key) {
            vector<int> indexes;
            indexes.reserve(200);

            for (int i = 0; i < 200; i++) {
                string salted = key + "|" + to_string(i);
                int idx = hash_function(salted);

                while (this->ring[idx].server) {
                    idx = (idx + 1) % n;
                }

                this->ring[idx].server = true;
                this->server_to_indexes[key].push_back(idx);
            }
        }

        void removeServer(string& key) {
            vector<int> indexes;
            
            for (int index: this->server_to_indexes[key]) {
                indexes.push_back(index);
            }

            server_to_indexes.erase(key);

            for (int index: indexes) {
                this->ring[index].server = false;
            }
        }

        unordered_map<string, int> getLoad() {
            // returns total count of requests on each server
            unordered_map<string, int> load;
            for (auto it: server_to_indexes) {
                string server = it.first;
                int num_of_req = 0;
                vector<int> indexes = it.second;
                for (int index: indexes) {
                    num_of_req += findReqOnServer(index);
                }
                load[server] = num_of_req;
            }

            return load;
        }

        void getLoadFactor() {
            // returns overall load factor
            unordered_map<string, int> load = getLoad();
            if (load.empty()) {
                cout << "Load factor: 0\n";
                return;
            }

            long long total = 0;
            long long mx = 0;
            for (auto& it : load) {
                total += it.second;
                mx = max(mx, (long long)it.second);
            }
            double avg = (double)total / load.size();

            cout << "Load factor: " << (avg > 0 ? (mx / avg) : 0.0) << "\n";
        }
};

int main() {
    RingSimulation rs = RingSimulation(100000);

    mt19937 rng(42);
    uniform_int_distribution<int> dist(0, 25);

    for (int i = 0; i < 10000; i++) {
        string req = "req_";
        for (int j = 0; j < 8; j++) {
            req += char('a' + dist(rng));
        }
        rs.addRequest(req);
    }

    cout<<"we have only 1 server right now\n";
    
    string s1 = "Indian server";
    rs.addServer(s1);

    auto load = rs.getLoad();
    for (auto& kv : load) {
        cout << kv.first << " " << kv.second << "\n";
    }
    rs.getLoadFactor();

    cout<<"================================================\n";
    cout<<"we have 2 servers now\n";

    string s2 = "server in America";
    rs.addServer(s2);

    load = rs.getLoad();
    for (auto& kv : load) {
        cout << kv.first << " " << kv.second << "\n";
    }
    rs.getLoadFactor();

    cout<<"================================================\n";
    cout<<"we have 3 servers now\n";

    string s3 = "Russia's server";
    rs.addServer(s3);

    load = rs.getLoad();
    for (auto& kv : load) {
        cout << kv.first << " " << kv.second << "\n";
    }
    rs.getLoadFactor();

    cout<<"================================================\n";
    cout<<"we have 2 servers now\n";

    string s4 = "Indian server";
    rs.removeServer(s4);

    load = rs.getLoad();
    for (auto& kv : load) {
        cout << kv.first << " " << kv.second << "\n";
    }
    rs.getLoadFactor();
}
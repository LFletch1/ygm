// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/disjoint_set.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/map.hpp>
#include <ygm/io/line_parser.hpp>
#include <fstream>
#include <random>
#include <algorithm>
#include <chrono>

namespace std {
template <>
struct hash<pair<int,int>> {
  auto operator()(const pair<int,int> &pair) const -> size_t {
    // return hash<XYZ>{}(xyz.value);
    return std::hash<int>{}(pair.first) ^ std::hash<int>{}(pair.second);
    // return std::hash<int>()
  }
};
}  // namespace std

int main(int argc, char **argv) {
    ygm::comm world(&argc, &argv);
    ygm::io::line_parser file_reader(world, {"graphs/facebook_combined.txt"}); // " "
    int num_of_nodes = 4039;

    // ygm::io::line_parser file_reader(world, {"graphs/enron-clean.csv"}); // ","
    // int num_of_nodes = 36625;

    // ygm::io::line_parser file_reader(world, {"graphs/US-Grid-Data.txt"}); // " "
    // int num_of_nodes = 4942;

    // ygm::io::line_parser file_reader(world, {"graphs/musae_ES_edges.csv"}); // ","
    // int num_of_nodes = 4648;
    
    // ygm::io::line_parser file_reader(world, {"graphs/musae_PTBR_edges.csv"}); // ","
    // int num_of_nodes = 1912;

    
    ygm::container::bag<std::pair<int,int>> graph_edges(world);
    std::vector<std::pair<int,int>> edges;
    file_reader.for_all([&graph_edges](const std::string& line) {
        // Line Parsing
        int start = 0;
        std::string delim = " ";
        int end = line.find(delim);
        std::vector<std::string> split_vec;
        while (end != std::string::npos) {
            split_vec.push_back(line.substr(start, end - start));
            start = end + delim.size();
            end = line.find(delim, start);
        }
        split_vec.push_back(line.substr(start, end - start));
        // std::cout << line.substr(start, end - start) << std::endl;
        graph_edges.async_insert(std::make_pair((std::stoi(split_vec[0])), (std::stoi(split_vec[1]))));
    }); 
    world.cout0() << graph_edges.size() << std::endl;

    static std::vector<int> true_labels; // Get true label from this vec by doing true_labels[fake_label]
    for (int i = 0; i < num_of_nodes; i++) {
        true_labels.push_back(i);
    }
    static std::vector<int> fake_labels; // Get fake label from this vec by doing fake_labels[true_label]
    for (int j = 0; j < num_of_nodes; j++) {
        fake_labels.push_back(j);
    }
    // world.cout0() << "Graph has " << graph_edges.size() << " edges" << std::endl;
    
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::disjoint_set<int> dset(world);

    ygm::container::counting_set<std::pair<int,int>> edge_frequency(world);
    
    world.barrier();
    // Shuffle label vec with same seed
    int seed = 42;
    std::default_random_engine rng1 = std::default_random_engine(50);

    // Local Shuffle RNG
    std::default_random_engine rng2 = std::default_random_engine(std::random_device()());
    
    // Global Shuffle RNG
    std::default_random_engine rng3 = std::default_random_engine(std::random_device()());


    auto start = std::chrono::high_resolution_clock::now();
    int trees = 1000;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 
        // world.barrier();
        world.cout0() << "Spanning Tree " << i << std::endl;

        local_spanning_tree_edges.clear();
        world.barrier();

        // Shuffle lables
        std::shuffle(true_labels.begin(), true_labels.end(), rng1);

        // Adjust fake_labels to match with true_labels
        for (int k = 0; k < true_labels.size(); k++) {
            int true_label = true_labels[k];
            fake_labels[true_label] = k;
        }
        world.barrier();
        
        graph_edges.local_shuffle(rng2);
        graph_edges.global_shuffle(rng3);

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        int edges_processed = 0;
        auto process_edge_lambda = [&world, &dset, &add_spanning_tree_edges_lambda, &edges_processed](const std::pair<int,int> edge) {
            int fake_label_a = fake_labels[edge.first];
            int fake_label_b = fake_labels[edge.second];
            dset.async_union_and_execute(fake_label_a, fake_label_b, add_spanning_tree_edges_lambda);
            // dset.async_union_and_execute(edge.first, edge.second, add_spanning_tree_edges_lambda);
            // dset.async_union(edge.first, edge.second);
            edges_processed += 1;
            if (edges_processed == 10000) {
                // world.cout0() << "Compressing" << std::endl;
                dset.all_compress();
                edges_processed = 0;
            }
        };

        // Generate tree
        graph_edges.for_all(process_edge_lambda);
        world.barrier();
        // int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());

        for (const auto edge : local_spanning_tree_edges) {
            int true_label_a = true_labels[edge.first];
            int true_label_b = true_labels[edge.second];
            // std::string edge_str = std::to_string(true_label_a) + "," + std::to_string(true_label_b);
            edge_frequency.async_insert(std::make_pair(true_label_a,true_label_b));
        }
        world.barrier();

        dset.clear();
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    if (world.rank0()) {
      std::cout << "Time: " << duration.count() << std::endl;
    }

    auto edge_count_lambda = [&world](const std::pair<int,int> edge_pair, const size_t count){
        world.cout() << "(" << edge_pair.first << "," << edge_pair.second << "): " <<  count << std::endl;
    };

    world.barrier();
    edge_frequency.for_all(edge_count_lambda);

    return 0;
}
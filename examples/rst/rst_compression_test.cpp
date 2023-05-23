// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <vector>
#include <ygm/comm.hpp>
#include <ygm/collective.hpp>
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
    // ygm::io::line_parser file_reader(world, {"graphs/facebook_combined.txt"}); // " "
    // int num_of_nodes = 4039;

    // ygm::io::line_parser file_reader(world, {"graphs/com-youtube.ungraph.txt"}); // "\t"
    // int num_of_nodes = 1134891;

    // ygm::io::line_parser file_reader(world, {"graphs/wiki-topcats.txt"}); // " "
    // int num_of_nodes = 1791489;  

    // ygm::io::line_parser file_reader(world, {"graphs/soc-LiveJournal1.txt"}); // "\t"
    // int num_of_nodes = 4847571;  

    // ygm::io::line_parser file_reader(world, {"graphs/com-orkut.ungraph.txt"}); // "\t"
    // int num_of_nodes = 3072442;

    ygm::io::line_parser file_reader(world, {"graphs/as-skitter.txt"}); // "\t"
    int num_of_nodes = 1696415;

    ygm::container::bag<std::pair<int,int>> graph_edges(world);
    std::vector<std::pair<int,int>> edges;
    file_reader.for_all([&graph_edges](const std::string& line) {
        // Line Parsing
        int start = 0;
        std::string delim = "\t";
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


    world.cout0() << "Processors: " << world.size() << std::endl;
    world.cout0() << "Nodes: " << num_of_nodes << std::endl;
    world.cout0() << "Edges: " << graph_edges.size() << std::endl;

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

    int trees = 5;
    // Start generating random spanning trees
    // Compressions per processor
    world.cout0() << "Edges Processed Before Compressing, Average Tree Generation Time (microseconds)" << std::endl;;
    std::vector<int> process_before_compressing = {10000, 100000, 1000000, 10000000, 100000000};
    for (int b_c : process_before_compressing) {
        int c_p = b_c / world.size();
        if (b_c > graph_edges.size()) {
            continue;
        }
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < trees; i++) { 

            local_spanning_tree_edges.clear();
            world.barrier();
            // world.cout0() << "Spanning Tree" << std::endl;

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
            world.barrier();

            size_t local_size = graph_edges.local_size();

            // size_t compressions_needed;
            // if (b_c == 0) {
            //     size_t compressions_needed = 0;
            // } else {
            //     size_t compressions_needed = local_size / c_p;
            // }
            size_t compressions_needed = 0;
            if (b_c != 0) {
                compressions_needed = local_size / c_p;
            }
            world.barrier();
            // world.cout0() << "Local Size " << local_size << std::endl;

            size_t least_compressions = ygm::min(compressions_needed, world);

            // world.cout0() << "Local Size " << local_size << std::endl;
            // world.cout0() << "c_p " << c_p << std::endl;
            // world.cout0() << "Least Compressions " << least_compressions << std::endl;

            auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
                local_spanning_tree_edges.push_back(std::make_pair(u, v));
            };

            int edges_processed = 0;
            int total_compressions = 0;
            auto process_edge_lambda = [&b_c, &least_compressions, &c_p, &world, 
                                        &total_compressions, &dset,
                                        &add_spanning_tree_edges_lambda, &edges_processed](const std::pair<int,int> edge) {
                int fake_label_a = fake_labels[edge.first];
                int fake_label_b = fake_labels[edge.second];
                // world.cout("Processing edge");
                dset.async_union_and_execute(fake_label_a, fake_label_b, add_spanning_tree_edges_lambda);
                edges_processed += 1;
                if (edges_processed == c_p) { 
                    if (b_c != 0) {
                        if (total_compressions < least_compressions) {
                            total_compressions += 1;
                            dset.all_compress();
                            edges_processed = 0;
                        }
                    }
                }
            };

            // Generate tree
            graph_edges.for_all(process_edge_lambda);
            world.barrier();

            for (const auto edge : local_spanning_tree_edges) {
                int true_label_a = true_labels[edge.first];
                int true_label_b = true_labels[edge.second];
                edge_frequency.async_insert(std::make_pair(true_label_a,true_label_b));
            }
            world.barrier();
            dset.clear();
        }
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        world.cout0() << b_c << "," << duration.count() / trees << std::endl;;
    }

    return 0;
}
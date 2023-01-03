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

// unsigned int hash(unsigned int x) {
//     x = ((x >> 16) ^ x) * 0x45d9f3b;
//     x = ((x >> 16) ^ x) * 0x45d9f3b;
//     x = (x >> 16) ^ x;
//     return x;
// }

int hash(int x, int r, int n) {
    x = (x + r) % n;
    return x;
}

int unhash(int x, int r, int n) {
    x = (x - r) % n;
    if (x < 0) {
        return (x + n);
    } else{
        return x;
    };
}

int main(int argc, char **argv) {
    ygm::comm world(&argc, &argv);

    ygm::io::line_parser file_reader(world, {"facebook_combined.txt"});
    int num_of_nodes = 4039;
    ygm::container::bag<std::pair<int,int>> graph_edges(world);
    // std::vector<std::pair<int,int>> edges;
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
        graph_edges.async_insert(std::make_pair(std::stoi(split_vec[0]), std::stoi(split_vec[1])));
    }); 
    world.cout0() << graph_edges.size() << std::endl;

    std::default_random_engine rand_eng = std::default_random_engine(32); 
 
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);
    world.barrier();

    int trees = 10000;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 
        // world.cout0() << "Here" << std::endl;
        world.barrier();
        // world.cout0() << "Spanning Tree" << std::endl;

        int shift = rand_eng() % num_of_nodes;
        // world.cout() << "Tree: " << i << ", Shift: " << shift << std::endl;
        world.barrier();
        local_spanning_tree_edges.clear();
        // graph_edges.clear();
        world.barrier();
        
        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&dset, &shift, &num_of_nodes, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            int fake_label_a = hash(edge.first, shift, num_of_nodes);
            int fake_label_b = hash(edge.second, shift, num_of_nodes);
            dset.async_union_and_execute(fake_label_a, fake_label_b, add_spanning_tree_edges_lambda);
        };

        // Generate tree
        world.barrier();
        graph_edges.for_all(process_edge_lambda);
        world.barrier();
        int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());
        world.barrier();

        // if (world.rank0()) {
        //     std::cout << "RST Size: " << spanning_tree_size << std::endl;
        // }
        // Now use counting set to count edge occurrences
        world.barrier();
        for (const auto edge : local_spanning_tree_edges) {
            int true_label_a = unhash(edge.first, shift, num_of_nodes);
            int true_label_b = unhash(edge.second, shift, num_of_nodes);
            std::string edge_str;
            if (true_label_a < true_label_b) {
                edge_str = std::to_string(true_label_a) + "," + std::to_string(true_label_b);
            } else {
                edge_str = std::to_string(true_label_b) + "," + std::to_string(true_label_a);
            }
            edge_frequency.async_insert(edge_str);
        }
        world.barrier();
        dset.clear();
    }

    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        // if (edge_count.second == 100) {
        world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
        // }
    };

    world.barrier();
    edge_frequency.for_all(count_lambda);

    return 0;
}
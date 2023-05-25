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


int main(int argc, char **argv) {
    ygm::comm world(&argc, &argv);

    // ygm::io::line_parser file_reader(world, {"fake_graph.txt"});
    ygm::io::line_parser file_reader(world, {"graphs/facebook_combined.txt"});
    int num_of_nodes = 4039;
    // int num_of_nodes = 21;
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
        graph_edges.async_insert(std::make_pair(std::stoi(split_vec[0]), std::stoi(split_vec[1])));
    }); 
    world.cout0() << graph_edges.size() << std::endl;
 
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    static std::vector<std::pair<int, int>> rst_cc_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);
    ygm::container::disjoint_set<int> cc_dset(world); 
    ygm::container::bag<std::pair<int,int>> rst_graph_edges(world);
    world.barrier();

    int bad_trees = 0;
    int trees = 1000;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 
        // world.cout0() << "Spanning Tree" << std::endl;

        local_spanning_tree_edges.clear();
        rst_cc_edges.clear();
        world.barrier();

        std::default_random_engine rand_eng = std::default_random_engine(32);
        world.barrier();
        
        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&world, &dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            dset.async_union_and_execute(edge.first, edge.second, add_spanning_tree_edges_lambda);
        };

        // Generate tree
        world.barrier();
        graph_edges.for_all(process_edge_lambda);

        world.barrier();
        int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());

        world.barrier();
        int count = 0;
        for (const auto edge : local_spanning_tree_edges) {
            rst_graph_edges.async_insert(std::make_pair(edge.first, edge.second));
            std::string edge_str = std::to_string(edge.first) + "," + std::to_string(edge.second);
            edge_frequency.async_insert(edge_str);
        }
        world.barrier();
        if (world.rank0()) {
            for (int v = 0; v < num_of_nodes; v++) {
                rst_graph_edges.async_insert(std::make_pair(v,v));
            }
        }

        world.barrier();
        size_t s = rst_graph_edges.size();
        if (world.rank0()) {
            std::cout << "Augmented RST Graph Size: " << s << std::endl;
        }
        auto add_to_cc = [](const int u, const int v) {
            rst_cc_edges.push_back(std::make_pair(u, v));
        };

        auto cc_check = [&world, &cc_dset, &add_to_cc](const std::pair<int,int> edge) {
            cc_dset.async_union_and_execute(edge.first, edge.second, add_to_cc);
        };

        rst_graph_edges.for_all(cc_check);
        world.barrier();
        
        int num_of_cc = cc_dset.num_sets();
        int rst_size = world.all_reduce_sum(rst_cc_edges.size());

        if (world.rank0()) {
            std::cout << "Original RST Size: " << spanning_tree_size << ", RST Size: " << rst_size << ", Number of connected components: " << num_of_cc << std::endl;
        }
        if (num_of_cc > 1) {
            bad_trees++;
        }

        rst_graph_edges.clear();

        world.barrier();

        dset.clear();
        cc_dset.clear();
    }

    auto count_lambda = [&world](const std::string edge_str, const auto count){
        // if (edge_count.second == 1000) {
            world.cout() << "(" << edge_str << ")" << ": " << count << std::endl;
        // }
    };

    world.barrier();
    edge_frequency.for_all(count_lambda);

    world.barrier();
    if (world.rank0()) {
        std::cout << "Total Bad Trees: " << bad_trees << std::endl;
    }

    return 0;
}
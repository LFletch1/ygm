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

    
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);

    int trees = 10000;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 

        local_spanning_tree_edges.clear();
 
        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            dset.async_union_and_execute(edge.first, edge.second, add_spanning_tree_edges_lambda);
        };

        // Generate tree
        world.barrier();
        graph_edges.for_all(process_edge_lambda);
        world.barrier();
        int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());
        world.barrier();

        for (const auto edge : local_spanning_tree_edges) {
            std:: string edge_str = std::to_string(edge.first) + "," + std::to_string(edge.second);
            edge_frequency.async_insert(edge_str);
        }
        world.barrier();

        dset.clear();
    }

    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
    };

    world.barrier();
    edge_frequency.for_all(count_lambda);

    return 0;
}
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

    ygm::container::bag<std::pair<int,int>> graph_edges(world);
    if (world.rank0()) {
        for (int i = 0; i <= 10; i++) {
            for (int j = i+1; j <= 10; j++) {
                graph_edges.async_insert(std::make_pair(i,j));
            }
        }
        for (int i = 11; i <= 20; i++) {
            for (int j = i+1; j <= 20; j++) {
                graph_edges.async_insert(std::make_pair(i,j));
            }
        }
        graph_edges.async_insert(std::make_pair(5,15));
    }
   
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);
    int m = graph_edges.size();
    world.barrier();
    if (world.rank0()) {
        std::cout << m << std::endl;
    }

    int trees = 10000;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 
        world.barrier();

        local_spanning_tree_edges.clear();

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

        // world.barrier();
        // int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());
        // if (world.rank0()) {
        //     std::cout << spanning_tree_size << std::endl;
        // }

        world.barrier();
        for (const auto edge : local_spanning_tree_edges) {
            std::string edge_str = std::to_string(edge.first) + "," + std::to_string(edge.second);
            edge_frequency.async_insert(edge_str);
        }
        dset.clear();
    }

    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
    };

    world.barrier();
    edge_frequency.for_all(count_lambda);
    return 0;
}
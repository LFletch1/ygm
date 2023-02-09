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
        for (int i = 0; i <= 2; i++) {
            for (int j = i+1; j <= 2; j++) {
                graph_edges.async_insert(std::make_pair(i,j));
            }
        }
        for (int i = 3; i <= 5; i++) {
            for (int j = i+1; j <= 5; j++) {
                graph_edges.async_insert(std::make_pair(i,j));
            }
        }
        graph_edges.async_insert(std::make_pair(2,5));
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
        // std::cout << "\nNew Spanning Tree" << std::endl;
        world.barrier();

        local_spanning_tree_edges.clear();

        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            std::cout << "Edge (" << u << "," << v << ") added" << std::endl;
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&world, &dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            std::cout << "Adding edge (" << edge.first << "," << edge.second << ")" << std::endl;
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
        world.cout() << std::endl;
        for (const auto edge : local_spanning_tree_edges) {
            std::string edge_str = std::to_string(edge.first) + "," + std::to_string(edge.second);
            edge_frequency.async_insert(edge_str);
        }
        std::vector<int> nodes = {0,1,2,3,4,5};
        std::map<int, int> reps = dset.all_find(nodes);
        for (int n = 0; n < 6; n++) {
            int parent = reps[n];
            std::cout << n << "'s parent is " << parent << std::endl;
        }
        dset.clear();
    }

    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
    };
    world.cout() << std::endl;

    world.barrier();
    edge_frequency.for_all(count_lambda);
    return 0;
}
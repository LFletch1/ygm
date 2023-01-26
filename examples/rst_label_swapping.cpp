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

    ygm::io::line_parser file_reader(world, {"musae_ES_edges.csv"});
    int num_of_nodes = 4648;
    ygm::container::bag<std::pair<int,int>> graph_edges(world);
    std::vector<std::pair<int,int>> edges;
    file_reader.for_all([&graph_edges](const std::string& line) {
        // Line Parsing
        int start = 0;
        std::string delim = ",";
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
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);
    
    world.barrier();
    int bad_trees = 0;

    int trees = 100000;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 
        world.barrier();

        local_spanning_tree_edges.clear();
        world.barrier();

        // Shuffle label vec with same seed
        std::default_random_engine rand_eng = std::default_random_engine(32);
        // Shuffle lables
        std::shuffle(true_labels.begin(), true_labels.end(), rand_eng);

        // Adjust fake_labels to correlate with true_labels
        for (int k = 0; k < true_labels.size(); k++) {
            int true_label = true_labels[k];
            fake_labels[true_label] = k;
        }
        world.barrier();
        
        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&world, &dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            int fake_label_a = fake_labels[edge.first];
            int fake_label_b = fake_labels[edge.second];
            dset.async_union_and_execute(fake_label_a, fake_label_b, add_spanning_tree_edges_lambda);
            // if (fake_label_a < fake_label_b) {
            //     dset.async_union_and_execute(fake_label_a, fake_label_b, add_spanning_tree_edges_lambda);
            // } else {
            //     dset.async_union_and_execute(fake_label_b, fake_label_a, add_spanning_tree_edges_lambda);
            // }
        };

        // Generate tree
        world.barrier();
        graph_edges.for_all(process_edge_lambda);
        world.barrier();
        int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());
        world.barrier();

        // Now use counting set to count edge occurrences
        world.barrier();
        int count = 0;
        for (const auto edge : local_spanning_tree_edges) {
            int true_label_a = true_labels[edge.first];
            int true_label_b = true_labels[edge.second];
            // std::string edge_str;
            std::string edge_str = std::to_string(true_label_a) + "," + std::to_string(true_label_b);
            // if (true_label_a < true_label_b) {
            // } else {
            //     edge_str = std::to_string(true_label_b) + "," + std::to_string(true_label_a);
            // }
            edge_frequency.async_insert(edge_str);
        }
        world.barrier();

        dset.clear();
    }


    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        // if (edge_count.second == 1000) {
            world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
        // }
    };

    world.barrier();
    edge_frequency.for_all(count_lambda);

    return 0;
}
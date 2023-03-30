// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/disjoint_set.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/array.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/map.hpp>
#include <ygm/io/line_parser.hpp>
#include <fstream>
#include <random>
#include <algorithm>


int main(int argc, char **argv) {
    ygm::comm world(&argc, &argv);
    // ygm::io::line_parser file_reader(world, {"musae_ES_edges.csv"});
    // ygm::io::line_parser file_reader(world, {"musae_PTBR_edges.csv"});
    // ygm::io::line_parser file_reader(world, {"enron-clean.csv"});
    // ygm::io::line_parser file_reader(world, {"US-Grid-Data.txt"});
    ygm::io::line_parser file_reader(world, {"facebook_combined.txt"});
    // int num_of_nodes = 4648;
    // int num_of_nodes = 36640;
    int num_of_nodes = 4039;
    // int num_of_nodes = 1912;
    // int num_of_nodes = 4942;
    
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

    // static std::vector<int> true_labels; // Get true label from this vec by doing true_labels[fake_label]
    // for (int i = 0; i < num_of_nodes; i++) {
    //     true_labels.push_back(i);
    // }
    // static std::vector<int> fake_labels; // Get fake label from this vec by doing fake_labels[true_label]
    // for (int j = 0; j < num_of_nodes; j++) {
    //     fake_labels.push_back(j);
    // }
    // world.cout0() << "Graph has " << graph_edges.size() << " edges" << std::endl;
    
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);
    ygm::container::array<int> fake_labels(world, num_of_nodes); // Get fake label from this vec by doing fake_labels[true_label]
    ygm::container::array<int> true_labels(world, num_of_nodes); // Get true label from this arr by true_labels[fake_label]
    if (world.rank0()) {
        for (int i = 0; i < num_of_nodes; i++) {
            fake_labels.async_set(i, i);
        }
    } 
    world.barrier();
    // Shuffle label vec with same seed
    int seed = 42;
    // std::default_random_engine rng1 = std::default_random_engine(50);
    ygm::default_random_engine<> rng1 = ygm::default_random_engine<>(world, seed);

    // Local Shuffle RNG
    ygm::default_random_engine<> rng2 = ygm::default_random_engine<>(world, seed);
    
    // Global Shuffle RNG
    ygm::default_random_engine<> rng3 = ygm::default_random_engine<>(world, seed);

    auto sync_true_labels = [&true_labels](const auto index, const auto value){
        true_labels.async_set(value, index);
    };

    auto print_fake_labels = [](const auto index, const auto value){
        std::cout << "Fake label of node " << index << " is " << value << std::endl;
    };

    auto print_true_labels = [](const auto index, const auto value){
        std::cout << "True label of fake labeled node " << index << " is " << value << std::endl;
    };

    int trees = 2;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 

        local_spanning_tree_edges.clear();
        world.barrier();

        // Shuffle lables
        fake_labels.global_shuffle(rng1);
        fake_labels.for_all(sync_true_labels);
        world.barrier();
        fake_labels.for_all(print_fake_labels);

        true_labels.for_all(print_true_labels);



        // Shuffle Edges
        world.barrier(); 
        graph_edges.local_shuffle(rng2);

        world.barrier();
        graph_edges.global_shuffle(rng3);

        // auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
        //     local_spanning_tree_edges.push_back(std::make_pair(u, v));
        // };

        // auto process_edge_lambda = [&world, &dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
        //     int fake_label_a = fake_labels[edge.first];
        //     int fake_label_b = fake_labels[edge.second];
        //     dset.async_union_and_execute(fake_label_a, fake_label_b, add_spanning_tree_edges_lambda);
        // };

        // // Generate tree
        // world.barrier();
        // graph_edges.for_all(process_edge_lambda);
        // world.barrier();
        // int spanning_tree_size = world.all_reduce_sum(local_spanning_tree_edges.size());
        // world.barrier();

        // for (const auto edge : local_spanning_tree_edges) {
        //     int true_label_a = true_labels[edge.first];
        //     int true_label_b = true_labels[edge.second];
        //     std::string edge_str = std::to_string(true_label_a) + "," + std::to_string(true_label_b);
        //     edge_frequency.async_insert(edge_str);
        // }
        // world.barrier();

        // dset.clear();
    }

    // auto edge_count_lambda = [&world](const std::string edge_str, const size_t count){
    //     world.cout() << "(" << edge_str << ")" << ": " <<  count << std::endl;
    // };

    // world.barrier();
    // edge_frequency.for_all(edge_count_lambda);

    return 0;
}
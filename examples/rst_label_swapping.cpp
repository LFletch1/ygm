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

    static std::vector<int> true_labels; // Get true label from this vec by doing true_labels[fake_label]
    for (int i = 0; i < num_of_nodes; i++) {
        true_labels.push_back(i);
    }
    static std::vector<int> fake_labels; // Get fake label from this vec by doing fake_labels[true_label]
    for (int j = 0; j < num_of_nodes; j++) {
        fake_labels.push_back(j);
    }
    std::default_random_engine rand_eng = std::default_random_engine(32);
    // world.cout0() << "Graph has " << graph_edges.size() << " edges" << std::endl;
    
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);
    world.barrier();

    int trees = 100;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 
        // world.cout0() << "Here" << std::endl;
        world.barrier();
        // world.cout0() << "Spanning Tree" << std::endl;

        local_spanning_tree_edges.clear();
        // graph_edges.clear();
        world.barrier();

        // Shuffle ranks on rank 0 then redistribute
        // std::shuffle(edges.begin(), edges.end(), std::default_random_engine(std::random_device()()));
        // Shuffle label vec with same seed

        // Shuffle lables
        std::shuffle(true_labels.begin(), true_labels.end(), rand_eng);

        // Adjust fake_labels to correlate with true_labels
        for (int k = 0; k < true_labels.size(); k++) {
            int true_label = true_labels[k];
            fake_labels[true_label] = k;
        }
        world.barrier();
        // world.cout() << "Edge (0,15) Fake Label" << std::endl;
        // int fake_label_a = fake_labels[0];
        // int fake_label_b = fake_labels[15];
        // if (true_labels[fake_label_a] == 0 && true_labels[fake_label_b] == 15) {
        //     world.cout() << "True" << std::endl;
        // } else {
        //     world.cout() << "False" << std::endl;
        // }
        // world.cout() << "(" << fake_labels[0] << "," << fake_labels[15] << ")" << std::endl;

        // if (world.rank0()) {
        //     for (auto edge : edges) {
        //         graph_edges.async_insert(edge);
        //     }
        // }
        
        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            if (true_labels[u] == 0 && true_labels[v] == 15) {
                std::cout << "Edge (0,15) successfully added" << std::endl;
            }
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&world, &dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            int fake_label_a = fake_labels[edge.first];
            int fake_label_b = fake_labels[edge.second];
            // if (fake_label_a == 3096 && fake_label_b == 26) {
            //     std::cout << "(3096,26) Trying to be added!" << std::endl;
            // }
            if (edge.first == 0 && edge.second == 15) {
                world.cout() << "Attempting to add edge (0,15)" << std::endl;
            }
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
        int count = 0;
        // world.cout() << local_spanning_tree_edges.size() << std::endl;
        for (const auto edge : local_spanning_tree_edges) {
            int true_label_a = true_labels[edge.first];
            int true_label_b = true_labels[edge.second];
            if (true_label_a == 0 && true_label_b == 15) {
                world.cout() << "Rank: " << world.rank() << " has edge (0,15), Tree: " << i << std::endl;
                // count += 1;
            }
            // if (edge.first == 3096 && edge.second == 26) {
            //     world.cout() << "Fake Labeled Edge (3096,26) is (" <<  true_label_a << "," << true_label_b << ")" << std::endl; 
            // }

            std:: string edge_str = std::to_string(true_label_a) + "," + std::to_string(true_label_b);
            edge_frequency.async_insert(edge_str);
        }
        world.barrier();

        // int count_sum = world.all_reduce_sum(count);
        // if (world.rank0()) {
        //     if (count_sum == 0) {
        //         // std::cout << "\nNode Labels" << std::endl;
        //         // Output Edge Label Vectors
        //         std::cout << "Edge (0,15) fake lable: (" << fake_labels[0] << "," << fake_labels[15] << ")" << std::endl;
        //         // for (int v = 0; v < num_of_nodes; v++) {
        //         //     int fake_lab = fake_labels[v];
        //         //     std::cout << "True Label: " << v << ", Fake Label: " << fake_lab << std::endl;
        //         //     std::cout << "Fake Label: " << fake_lab << ", True Label: " << true_labels[fake_lab] << std::endl;
        //         // }
        //     }
        // }

        // auto print_fake_labeled_edges = [&fake_labels](const std::pair<int,int> edge) {
        //     int fake_label_a = fake_labels[edge.first];
        //     int fake_label_b = fake_labels[edge.second];
        //     std::cout << "(" << fake_label_a << "," << fake_label_b << ")" << std::endl;
        // };

        // world.barrier();
        // if (count_sum == 0) {
        //     for (const auto edge : local_spanning_tree_edges) {
        //         world.cout() << "RST Edge: (" << true_labels[edge.first] << "," << true_labels[edge.second] << ")" << std::endl;
        //     }
        //     world.barrier();
        //     world.cout0() << "\nFake Edges: " << std::endl;
        //     graph_edges.for_all(print_fake_labeled_edges);
        // }

        world.barrier();

        dset.clear();
    }

    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        if (edge_count.second == 100) {
            world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
        }
    };

    world.barrier();
    edge_frequency.for_all(count_lambda);

    return 0;
}
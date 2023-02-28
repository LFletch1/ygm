// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/counting_set.hpp>
#include <algorithm>
#include <random>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);
  ygm::container::bag<int> bag(world);

  int total_items = 100;
  if (world.rank0()) {
    for (int i = 0; i < total_items; i++) {
      bag.async_insert(i);
    }
  }

  std::default_random_engine rand_eng = std::default_random_engine(std::random_device()());
  std::default_random_engine rand_eng2 = std::default_random_engine(std::random_device()());

  world.barrier();
  world.cout0() << "Locally Shuffling" << std::endl;

  bag.local_shuffle(rand_eng);

  world.barrier();
  world.cout0() << "Locally Shuffling Again" << std::endl;

  bag.local_shuffle(rand_eng);

  world.barrier();
  world.cout0() << "Globally Shuffling" << std::endl;
  bag.global_shuffle(rand_eng2);

  world.barrier();
  world.cout0() << "Globally Shuffling Again" << std::endl;
  bag.global_shuffle(rand_eng2);

  world.cout0() << bag.size() << std::endl;

  world.barrier();
  auto bag_content = bag.gather_to_vector(0);

  if (world.rank0()) {
    for (int i = 0; i < total_items; i++) {
      if (std::find(bag_content.begin(), bag_content.end(), i) == bag_content.end()) {
        world.cout0() << "Bag Missing Item " << i << std::endl;
      }
    }
  }
  auto print_bag = [&world](const int item){
    world.cout() << item << std::endl;
  };

  bag.for_all(print_bag);

  return 0;
}

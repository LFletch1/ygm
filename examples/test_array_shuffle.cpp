// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/comm.hpp>
#include <ygm/container/array.hpp>
#include <ygm/random.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);

  // Test async_set
  {
    int ranks = world.size();
    int elements_per_rank = 10000000;
    ygm::container::array<int> arr(world, ranks*elements_per_rank);

    // if (world.rank0()) {

    for (int i = 0; i < elements_per_rank; ++i) {
      int index = i + (world.rank() * elements_per_rank);
      arr.async_set(index, index);
    }
    // }

    int seed = 42;
    ygm::default_random_engine<> rng(world, seed);

    auto print_values = [&world](const auto index, const auto value) {
      world.cout() << "Index " << index << " contains value: " << value << std::endl;
    };

    world.cout0() << "First Global Shuffle" << std::endl;  
    arr.global_shuffle(rng);

    // arr.for_all( 
    //   [&world](const auto index, const auto value){
    //     if (world.rank0()) {
    //       std::cout << "Index " << index << " contains value: " << value << std::endl;
    //     }
    //   }
    // );
    // arr.for_all(print_values);

    // world.barrier();

    // world.cout0() << "Second Global Shuffle" << std::endl;  
    // arr.global_shuffle(rng);
    // arr.for_all(print_values);

  }

  return 0;
}

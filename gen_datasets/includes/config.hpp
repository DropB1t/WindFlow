/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of IntervalJoinBenchmarks.
 *  
 *  IntervalJoinBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/DropB1t/IntervalJoinBenchmarks/blob/main/LICENSE
 *  
 *  IntervalJoinBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#ifndef GEN_CONSTANTS_HPP
#define GEN_CONSTANTS_HPP

#include<string>

using namespace std;

typedef enum
{
    UNIFORM_SYNTHETIC,
    ZIPF_SYNTHETIC,
    SELFSIMILAR_SYNTHETIC
} gen_types;

const size_t default_data_size = 2000000;

const char separator = '|';

const uint rseed = 12345;
const uint lseed = 54321;

double skewness = 0.8;

// path of the datasets to be generated
const string r_synthetic_uniform_path ="../datasets/synt_u/r_uniform";
const string l_synthetic_uniform_path ="../datasets/synt_u/l_uniform";

const string r_synthetic_zipf_path ="../datasets/synt_z/r_zipf";
const string l_synthetic_zipf_path ="../datasets/synt_z/l_zipf";

const string r_synthetic_ss_path ="../datasets/synt_ss/r_ss";
const string l_synthetic_ss_path ="../datasets/synt_ss/l_ss";

#endif //GEN_CONSTANTS_HPP

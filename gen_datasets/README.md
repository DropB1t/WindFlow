# Compile and run Dataset Generator

## Compile
make all

## Configuration of tests

### Parameters
```
--num_key <value>
[--size <dataset_size>]
--type < su | sz >
[--zipf <zipf_exponent>]
```

### Test Types

- **su** = synthetic dataset with uniform distribution
- **sz** = synthetic dataset with zipf distribution

## Example
``./bin/gen --num_key 100 --size 10000 --type sz --zipf 0.8``

In the example above, we generate a synthetic dataset with:
- `--num_key 100`: This option specifies that 100 unique keys should be generated.

- `--size 10000`: This option specifies the total size of the data to be generated. In this case, it's 10K units.

- `--type sz`: This option specifies the type of data to be generated. In this case, 'sz' stands for synthetic dataset with zipf distribution.

- `--zipf 0.8`: This option specifies the distribution of the keys in the data. A Zipf distribution is a type of power law distribution that is often used in information systems. The parameter 0.8 controls the shape of the distribution.

## Contributions
- [The Zipfian distribution generator](https://github.com/llersch/cpp_random_distributions?tab=readme-ov-file#zipfian)

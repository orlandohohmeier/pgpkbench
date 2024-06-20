# Postgres Primary Key Performance Benchmarking

This project uses PyTest, PyTest Benchmark, and a few other Python libraries to run a Postgres database and perform benchmarks. The goal is to understand how different types of primary keys impact the performance of the database.

## Overview

There are many articles discussing the impact of various primary key types in Postgres, often debating the use of UUID primary keys as `varchar` vs `UUID1`. However, detailed information on the performance differences between k-sortable IDs and random ones (like UUID version 4) is sparse. On a theoretic level random IDs should perform much worse due to the underlying data structues but there are good arguemnts for using them anyways - like not leaking ID creation time or item count. This project aims to shed some light on the performance differences by running simple benchmarks on different primary key types.

## Benchmarks and Contributions

This project benchmarks different primary key types by inserting a number of elements into the database and performing various selects and inserts, both with and without relations. The results are posted on the wiki page. Contributions to improve these benchmarks or the benchmarking script are highly welcome!

Running the tests may take some time, but they are not resource-intensive, allowing you to run them in the background. There are likely other interesting benchmarks that can be conducted, so feel free to contribute new ideas and improvements.

## Why Not `pgbench`?

Initially, I considered using `pgbench` for this project. However, `pgbench` would require testing with and generating different types of IDs using Postgres extensions, which aren't available in all environments. To keep the setup clean and pure without relying on special extensions, I opted to generate the IDs within Python instead.

This approach does have some performance implications. Generating UUIDs (versions 4 or 7) or other IDs in Python is not a no-op; it takes time and affects the overall performance numbers. But there's also an inherent round trip when inserting data with parent-child relationships, retrieving the generated ID, and using it. Thus I believe this method provides a fair comparison of primary key performance under realistic conditions.

## Types Tested

The project benchmarks the following primary key types:

- Serial
- UUIDv4
- UUIDv7
- ULID


## Running the Benchmarks

To run the benchmarks, follow these steps:

1. Clone the repository.
2. Install the required dependencies using poetry.
3. Execute the benchmark tests.

```bash
git clone <repository_url>
cd <repository_directory>
poetry install
poetry run pytest
```

You can adjust the number of inserted elements usign the `INSERT_COUNT` env and
 the randomly sampled number of IDs to select with `SELECT_COUNT`. For `pytest-benchmark` options like `autosave`, `rounds`, or `histogram` please refer to https://pytest-benchmark.readthedocs.io/en/latest/

## Conclusion

This project is a tiny example designed to provide slightly different information on primary key performance in Postgres. Your contributions and feedback are greatly appreciated to make this resource more comprehensive and useful for the community.

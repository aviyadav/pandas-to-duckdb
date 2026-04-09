# Pandas vs Polars vs DuckDB: Data Scientists Guide

A comprehensive guide comparing **Pandas**, **Polars**, and **DuckDB** for data processing and analysis tasks. This project includes practical examples and performance benchmarks across common data operations.

## 📋 Overview

This guide demonstrates the key differences and advantages of three powerful data processing libraries:

- **Pandas**: The traditional Python data analysis library with a familiar DataFrame API
- **Polars**: A high-performance DataFrames library written in Rust, designed for speed and memory efficiency
- **DuckDB**: An in-process SQL database that provides fast SQL-based data querying

## 🎯 Purpose

The project provides side-by-side comparisons of common data operations including:
- Filtering rows
- Aggregations and groupby operations
- Joining datasets
- Window functions
- String operations
- Date/time operations
- Complex queries and transformations

Each operation is demonstrated with execution time measurements to help you understand performance characteristics.

## 🚀 Getting Started

### Installation

Ensure you have Python 3.14 or later, then install the required dependencies:

```bash
pip install -e .
```

Or install packages individually:

```bash
pip install pandas polars duckdb jupyter pyarrow
```

### Requirements

- Python >= 3.14
- pandas >= 3.0.2
- polars >= 1.39.3
- duckdb >= 1.5.1
- pyarrow >= 23.0.1
- jupyter >= 1.1.1

## 📁 Project Structure

```
.
├── README.md                                  # This file
├── pyproject.toml                            # Project configuration and dependencies
├── pandas_vs_polars_vs_duckdb.ipynb         # Main notebook with all comparisons
├── sales_data.csv                            # Sample dataset (5M+ rows)
```

## 📖 Usage

1. Launch Jupyter:
```bash
jupyter notebook
```

2. Open `pandas_vs_polars_vs_duckdb.ipynb`

3. Run cells to see:
   - Code examples for each library
   - Execution time comparisons
   - Output results and visualizations

## 📊 Topics Covered

The notebook includes comprehensive examples for:

1. **Data Generation** - Creating sample sales datasets
2. **Filtering Operations** - Selecting rows based on conditions
3. **Aggregations** - Group-by operations and summarizations
4. **Joins** - Combining datasets with various join types
5. **Window Functions** - Row numbering, ranking, and running totals
6. **String Operations** - Text manipulation and pattern matching
7. **Date/Time Operations** - Temporal data handling
8. **Advanced Queries** - Complex transformations and multi-step operations
9. **Performance Benchmarks** - Relative execution times across libraries

## 💡 Key Insights

Each library has strengths for different use cases:

- **Pandas**: Best for exploratory data analysis and when you have existing pandas knowledge
- **Polars**: Excellent for performance-critical work and large-scale data processing
- **DuckDB**: Ideal when working with SQL and need blazing-fast query execution

Performance and syntax patterns are highlighted throughout the notebook to help you choose the right tool for your task.

## 📝 Notes

- The `sales_data.csv` file is generated automatically when you run the first cell
- All examples use the same sample dataset for fair comparisons
- Execution times are printed for each operation to compare performance

## 🔧 Tips

- Run cells sequentially to avoid missing data dependencies
- Uncomment timing code to perform multiple iterations for more accurate benchmarks
- The sample dataset grows to 5 million rows for realistic performance measurement

---

**Created**: A comprehensive educational resource for data scientists and engineers evaluating Python data processing libraries.

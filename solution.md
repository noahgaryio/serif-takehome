## Explanation of Solution

The solution to process the large JSON data file involves multiple steps and tools to ensure efficiency, manageability, and effective processing. Here's a detailed explanation of the approach:

### Goals and Tools

1. **GoLang for Data Extraction**:
   - The choice to use GoLang for extracting data from the source demonstrates proficiency in a different programming language. GoLang is efficient and performs well for tasks involving data fetching and processing.

2. **Streaming and Subset Processing**:
   - Processing the entire file initially led to memory issues. By streaming the data and processing it in subsets, the solution handles large datasets more effectively. Starting with a small sample helped understand the data structure.

3. **PySpark for Data Processing**:
   - PySpark was chosen to process the data due to its ability to distribute tasks across multiple nodes, making it suitable for large-scale data processing. PySpark's capabilities in handling big data efficiently mitigated the memory issues encountered earlier.
   - A User Defined Function (UDF) in PySpark was used to strip query parameters and deduplicate URLs, ensuring that only unique URLs were processed.

4. **Managing Console Output**:
   - Handling massive console output was challenging. The solution involved using Py4J to manage logging, directing logs to `script_logs.txt` and `spark_logs.txt` files. This approach made the logs more readable and easier to manage.
   - Further extraction from the nested JSON structure was necessary to fetch the URLs accurately.

### Trade-offs and Time Taken

- **Language Choice**: Using GoLang for initial data extraction demonstrated versatility but added complexity. Switching to Python for handling the decompression step initially saved time but was ultimately not needed.
- **Memory Management**: Significant time was spent managing memory issues. The final solution used streaming and distributed processing with PySpark to handle large datasets effectively.
- **Logging**: Managing extensive logging output took considerable effort to ensure the process was trackable and manageable.

**Time Taken**: 
- Writing the initial code: ~5 hours
- Debugging and resolving memory issues: ~10 hours
- Finalizing and testing the solution: ~5 hours

**Total Time**: ~20 hours

## Getting Started

### Prerequisites
1. **Install Go**:
   - Follow the instructions [here](https://golang.org/doc/install) to install GoLang on your system.

2. **Install Python**:
   - Ensure Python 3 is installed. Install the necessary packages using:
   ```sh
   pip install -r requirements.txt
   ```

3. **Install Apache Spark**:
   - Follow the instructions [here](https://spark.apache.org/downloads.html) to download and install Apache Spark.

### Steps to Run

1. **Clone the Repository**:
   ```sh
   git clone https://github.com/noahgaryio/serif-takehome.git
   cd serif-takehome
   ```

2. **Run Suite of Scripts**:
   Execute the suite of scripts using the `make` command:
   ```sh
   make
   ```

3. **Clean Up**:
   To remove generated files and logs, use the `make clean` command:
   ```sh
   make clean
   ```

4. **Subsequent Runs**:
   Skip certain steps in subsequent runs by passing arguments to the `make` command:
   ```sh
   make ARGS="--skip-download"
   ```

### Important Notes

- **Log Files**:
  - Detailed logs are saved in `logs/spark_logs.txt` and `logs/script_logs.txt`.
  
- **Output**:
  - The extracted URLs are saved in the `data/output_urls.txt` file.
  
- **Troubleshooting**:
  - Ensure that all paths and dependencies are correctly set up. Check the error messages in the log files for guidance on resolving issues.

This approach ensures that the processing of large datasets is efficient, manageable, and can be executed on systems with limited memory. The use of multiple tools and languages demonstrates versatility and problem-solving skills in handling various stages of the data processing pipeline.

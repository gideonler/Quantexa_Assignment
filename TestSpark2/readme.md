# Flight Data Analysis Project

This project performs analysis on flight data to identify patterns and relationships among passengers. The main analysis is centered around finding passengers who have flown together multiple times. The project uses Apache Spark and Scala for data processing.

## Project Structure

The project has the following structure:


- **src/main/resources**: Contains the input CSV files `flightData.csv` and `passengers.csv`.
- **src/scala/example/main/Main.scala**: Main Scala file to be run for executing the analysis.
- **CSV_Output**: Directory where the analysis results will be stored.

## Instructions

Follow these steps to run the analysis:

1. **Data Setup**:
    - Place the `flightData.csv` and `passengers.csv` files in the `src/main/resources` directory.

2. **Run the Main File**:
    - Navigate to `src/scala/example/main/`.
    - Execute the `Main.scala` file, which contains the main analysis logic.

3. **View the Results**:
    - The analysis results will be stored in the `CSV_Output` directory.
    - For the primary analysis, check the `src/main/CSV_Output` directory.
    - For the bonus analysis, check the `src/main/CSV_Output/bonus` directory.

4. **CSV Output Format**:
    - Results will be written in CSV format.
    - Each CSV file will have a header row.

## Dependencies

- This project uses Apache Spark. Ensure that you have Spark configured and available in your environment.

## Notes

- Adjust the paths and configurations in the `Main.scala` file if needed.
- Feel free to explore the code for additional details on the analysis performed.



# AirNow Pollutant & Meteorology Analyzer

An R Shiny application for site-specific analysis of air quality and meteorological data. This tool downloads, merges, and visualizes data from official sources like AirNow, NOAA, and IEM, providing a comprehensive toolkit for environmental data exploration.

![App Screenshot](input_file_0.png)

## Key Features

-   **Dual Analysis Modes**: Supports both **Hourly** (AirNow + NOAA ISH) and **Daily** (AirNow + IEM ASOS) data analysis.
-   **Site-Specific Selection**: Easily select any AQS monitoring site in the United States by state and county.
-   **Automated Data Fetching**: Downloads the latest pollutant and meteorological data directly from AirNow, NOAA, and IEM servers.
-   **Intelligent Caching**: Caches downloaded data to speed up subsequent analyses for the same site and date range.
-   **Nearby Station Finder**: Automatically finds the closest meteorological stations to your selected air quality monitor.
-   **Comprehensive Visualizations**: Generates a wide array of publication-quality plots using the powerful `openair` package, including:
    -   Calendar Plots (with value and wind vector annotations)
    -   Polar Plots (concentration, CPF)
    -   Pollution and Wind Roses
    -   Time Series and Trend Plots (Theil-Sen, ggplot)
    -   Scatter Plots and Correlation Matrices
    -   And many more...
-   **Statistical Summaries**: Provides a detailed statistics tab with descriptive stats, AQI category breakdowns, and diurnal patterns.
-   **Data Export**: Allows you to download the final merged dataset as a CSV and all generated plots as a ZIP archive.

## How to Run the App

This application is built with the R Shiny framework. To run it locally, you will need R and RStudio.

### 1. Prerequisites

-   [R](https://cran.r-project.org/) (version 4.0 or higher recommended)
-   [RStudio Desktop](https://posit.co/download/rstudio-desktop/) (recommended IDE)

### 2. Installation

First, clone this repository to your local machine:

```bash
git clone https://github.com/YOUR_USERNAME/airnow-met-analyzer.git
cd airnow-met-analyzer
```

Next, open the project in RStudio and run the following command in the R console to install all the required packages:

```r
install.packages(c(
  "shiny", "bslib", "dplyr", "digest", "lubridate", "worldmet", "openair", 
  "readr", "ggplot2", "gridExtra", "grid", "viridis", "padr", "httr", 
  "purrr", "future", "furrr", "DT", "shinycssloaders", "shinyjs", "zip", 
  "glue", "tidyr", "mgcv", "RColorBrewer", "geosphere", "quantreg"
))
```

### 3. Launch the App

With all packages installed, open the `app.R` file in RStudio. Click the **"Run App"** button that appears at the top of the editor pane.

## Usage Workflow

1.  **Select Analysis Type**: Choose between "Hourly" or "Daily" analysis.
2.  **Select Pollutant & Site**:
    -   Choose the pollutant you wish to analyze.
    -   Select the State, County, and specific AQS monitoring site.
3.  **Select Meteorological Station**: The app will suggest nearby MET stations. Pick one from the list or enter a code manually.
4.  **Select Date Range**: Choose the start and end dates for your analysis.
5.  **Choose Plots**: Select the visualizations you want to generate.
6.  **Run Analysis**: Click the "Run Analysis" button and wait for the data to be downloaded and processed.
7.  **Explore Results**:
    -   **Merged Data Tab**: View and download the combined pollutant and meteorology dataset.
    -   **Selected Plots Tab**: View the plots you selected. You can download them all as a ZIP file.
    -   **Statistics Tab**: Review detailed statistical summaries of your data.

## Data Sources

This application relies on publicly available data from the following sources:

-   **Air Quality Data**: [EPA AirNow](https://www.airnow.gov/)
-   **Hourly Meteorological Data**: [NOAA Integrated Surface Database (ISH)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) via the `worldmet` R package.
-   **Daily Meteorological Data**: [Iowa Environmental Mesonet (IEM) ASOS](https://mesonet.agron.iastate.edu/request/daily.phtml)

## Author

*   **Rodney Cuevas**
*   For bug reports or questions, please contact [RCuevas@mdeq.ms.gov](mailto:RCuevas@mdeq.ms.gov).
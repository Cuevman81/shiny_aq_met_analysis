# AirNow Pollutant & Meteorology Analyzer

An R Shiny application for site-specific analysis of air quality and meteorological data. This tool downloads, merges, and visualizes data from official sources like AirNow, NOAA, and IEM, providing a comprehensive toolkit for environmental data exploration.

![App Screenshot](images/CalendarPlot.png)

## Key Features

-   **Categorized Visualization Suite**: Organized sidebar with grouped plot selections (each group has a **Select all / Clear** shortcut):
    -   **Temporal Trends (Both)**: Calendar Plots, Time Series, Interactive TrendLevel, Diurnal/Weekly Time Variation, Theil-Sen Trend, and Wind-Normalized Trend.
    -   **Meteorological & Source (Hourly Only)**: Wind Roses (standard + diurnal), Pollution Roses, Bivariate Polar Plots, Polar Annulus, Percentile Roses, Polar Cluster Analysis, and Stability Analysis.
    -   **Statistical & Diagnostic (Both)**: Data Summary Heatmaps, AQI Category Proportions (TimeProp), Scatter Plots (Pollutant vs Met), Correlation Matrices, and Kernel Density Exceedance.
-   **Interactive Site Map**: A Leaflet map showing the selected AQS site and nearby MET stations with their distances, to sanity-check the site-to-station pairing.
-   **Data Provenance**: The Statistics tab reports which MET source actually supplied the data (real-time IEM ASOS vs. quality-controlled NOAA ISH) and the station distance.
-   **Statistical Summaries**: Detailed metrics for both pollutants and weather, including AQI categories and diurnal patterns.
-   **Data Export**: Download the final merged dataset as a CSV and **all** generated plots as a ZIP archive in your chosen format (PNG, PDF, or SVG).

## How to Run the App

1.  **Clone the Repository**:
    ```bash
    git clone https://github.com/Cuevman81/shiny_aq_met_analysis.git
    cd shiny_aq_met_analysis
    ```
2.  **Install Dependencies**: Open R and run:
    ```r
    install.packages(c(
      "shiny", "bslib", "bsicons", "leaflet", "Hmisc", "dplyr", "digest",
      "lubridate", "worldmet", "openair", "readr", "ggplot2", "gridExtra",
      "viridis", "padr", "httr", "purrr", "future", "furrr", "DT",
      "shinycssloaders", "shinyjs", "zip", "glue", "tidyr", "mgcv",
      "RColorBrewer", "geosphere", "quantreg"
    ))
    ```
3.  **Run the App**: Open `Met_Pollutant_Analysis_Airnow_APP.R` and click **Run App**.

## Data Sources

-   **Air Quality**: [EPA AirNow](https://www.airnow.gov/)
-   **Meteorology (Hourly)**: Dual-source. [NOAA ISH](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) for long-term data and **[Iowa Environmental Mesonet (IEM)](https://mesonet.agron.iastate.edu/request/asos.py)** for real-time Hourly ASOS observations.
-   **Meteorology (Daily)**: [IEM ASOS Daily](https://mesonet.agron.iastate.edu/request/daily.phtml)

## Author

*   **Rodney Cuevas** ([RCuevas@mdeq.ms.gov](mailto:RCuevas@mdeq.ms.gov))
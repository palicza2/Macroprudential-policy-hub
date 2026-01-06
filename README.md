# Macroprudential Policy Hub

This repository provides a framework for tracking and analyzing Countercyclical Capital Buffer (CCyB) rates across the European Union and the European Economic Area (EEA).
It dynamically downloads the latest CCyB data from the European Systemic Risk Board (ESRB) and generates analytical reports and dashboards.

## Project Structure

The project is organized into a modular and rational structure to enhance maintainability, scalability, and ease of use:

```
Macroprudential_hub/
├── data/                 # Stores raw and intermediate data files (e.g., downloaded Excel files)
├── scripts/              # Contains R scripts for data loading, processing, and configuration
│   ├── config.R          # Centralized configuration variables (URLs, paths, filenames)
│   └── data_loader.R     # Handles data download, cleaning, and initial processing
├── reports/              # R Markdown files for generating reports and dashboards
│   ├── CCyB_Dashboard.Rmd # Flexdashboard for interactive CCyB data visualization
│   └── CCyB_Tracker.Rmd   # Detailed R Markdown report on CCyB evolution and policy
├── output/               # Stores generated output files (HTML reports, plots, Excel exports)
├── run_all.R             # Master script to execute the entire data pipeline and render reports
└── README.md             # This file
```

## Key Features

*   **Automated Data Acquisition**: Downloads the latest CCyB data directly from the ESRB website.
*   **Modular Design**: Separates data loading, processing, and reporting logic into distinct scripts for better organization.
*   **Centralized Configuration**: All external URLs, file paths, and key parameters are managed in a single `config.R` file.
*   **Dynamic Reports**: Generates detailed HTML reports and interactive dashboards using R Markdown and Flexdashboard.
*   **Data Export**: Automatically exports processed data into Excel files for further analysis.
*   **Version Controlled**: The entire project is under Git version control for tracking changes and collaborative development.

## Getting Started

To set up and run this project locally, follow these steps:

### 1. Prerequisites

Make sure you have [R](https://www.r-project.org/) and [RStudio](https://www.rstudio.com/products/rstudio/download/) installed.

### 2. Clone the Repository

```bash
git clone https://github.com/palicza2/Macroprudential-policy-hub.git
cd Macroprudential-policy-hub
```

### 3. Install R Packages

Open RStudio and install the necessary R packages. You can do this by running the following command in the R console:

```R
if (!require("pacman")) install.packages("pacman")
pacman::p_load(curl, readxl, dplyr, janitor, lubridate, countrycode, stringr, writexl, crosstalk, ggplot2, rmarkdown, flexdashboard, plotly, DT, reactable, tidyr, ggrepel)
```

### 4. Run the Project

Execute the `run_all.R` script to download the latest data, process it, and render all reports and plots. You can run this script directly from RStudio or your R console:

```R
source("run_all.R")
```

This script will perform the following actions:
*   Download the ESRB CCyB data to the `data/` directory (if not already present).
*   Process and clean the data.
*   Render `reports/CCyB_Tracker.Rmd` to `output/CCyB_Tracker.html`.
*   Render `reports/CCyB_Dashboard.Rmd` to `output/CCyB_Dashboard.html`.
*   Generate `output/ccyb_evolution.png`.
*   Export various data subsets to Excel files in the `output/` directory.

## Output Files

After running `run_all.R`, the `output/` directory will contain:

*   `CCyB_Tracker.html`: The comprehensive analytical report.
*   `CCyB_Dashboard.html`: The interactive dashboard.
*   `ccyb_evolution.png`: A plot showing the evolution of CCyB rates.
*   `data_diffusion.xlsx`: Excel export of aggregate trend data.
*   `data_latest_decisions.xlsx`: Excel export of recent policy decisions.
*   `data_snapshot.xlsx`: Excel export of the latest CCyB rates per country.
*   `data_pn_ccyb.xlsx`: Excel export of positive neutral CCyB frameworks data.
*   `data_historical.xlsx`: Excel export of full historical data.
*   `data_risk_analysis.xlsx`: Excel export of credit vs. CCyB setting analysis data.

## Customization and Further Development

*   **Configuration**: Modify `scripts/config.R` to change data sources, output directories, or other global parameters.
*   **Data Processing**: Adjust `scripts/data_loader.R` to modify data cleaning, transformation, or add new derived variables.
*   **Reports and Dashboards**: Edit the `.Rmd` files in the `reports/` directory to customize visualizations, add new analyses, or change the layout.
*   **New Analyses**: Add new R scripts to the `scripts/` directory for additional analytical modules.

## Contributing

Feel free to fork this repository, open issues, or submit pull requests.

## License

This project is open-source and available under the MIT License.
if (!require("pacman")) install.packages("pacman", repos = "https://cran.rstudio.com/")
pacman::p_load(arrow, dplyr)

# Path to the processed data
processed_data_path <- "data/processed_data.parquet"

# Read the parquet file
if (file.exists(processed_data_path)) {
  df_from_parquet <- read_parquet(processed_data_path)
  print("Successfully read processed_data.parquet:")
  print(head(df_from_parquet))
} else {
  print(paste("Error: File not found at", processed_data_path))
}

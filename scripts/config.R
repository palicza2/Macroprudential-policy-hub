# config.R

# --- Paths ---
data_dir <- "data/"
output_dir <- "output/"
scripts_dir <- "scripts/"
reports_dir <- "reports/"

# --- Data Source URLs ---
esrb_url <- "https://www.esrb.europa.eu/national_policy/ccb/shared/data/esrb.ccybd_CCyB_data.xlsx"

# --- File Names ---
esrb_data_filename <- "esrb.ccybd_CCyB_data.xlsx"

# Full path for downloaded data
destfile_path <- paste0(data_dir, esrb_data_filename)

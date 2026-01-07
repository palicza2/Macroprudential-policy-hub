if (!require("pacman")) install.packages("pacman")
pacman::p_load(curl, readxl, dplyr, janitor, lubridate, countrycode, stringr, writexl, crosstalk)

# --- DATA DOWNLOAD ---
h <- new_handle()
handle_setheaders(h, "User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")

message("Checking for data file...")
if (!file.exists(destfile_path)) {
  message("Downloading file...")
  tryCatch({
    curl_download(esrb_url, destfile_path, handle = h)
    message("Download successful!")
  }, error = function(e) {
    stop("Download failed. Check your internet connection or Proxy settings.")
  })
} else {
  message("Data file already exists. Skipping download.")
}

# --- DATA PROCESSING ---
if (file.exists(destfile_path)) {
  sheets <- excel_sheets(destfile_path)
  target_sheet <- if ("Data" %in% sheets) "Data" else if (length(sheets) >= 2) sheets[2] else sheets[1]
  
  preview <- read_excel(destfile_path, sheet = target_sheet, col_names = FALSE, n_max = 10)
  header_row_idx <- 1
  for(i in 1:nrow(preview)) {
    if (any(grepl("Country", as.character(preview[i, ]), ignore.case = TRUE))) {
      header_row_idx <- i
      break
    }
  }
  
  raw_df <- read_excel(destfile_path, sheet = target_sheet, skip = header_row_idx - 1, col_types = "text") %>%
    clean_names()
  
  rate_col <- if("c_cy_b_rate" %in% colnames(raw_df)) "c_cy_b_rate" else if("ccy_b_rate" %in% colnames(raw_df)) "ccy_b_rate" else NULL
  gap_col <- if("credit_gap" %in% colnames(raw_df)) "credit_gap" else NULL
  gdp_col <- if("credit_to_gdp" %in% colnames(raw_df)) "credit_to_gdp" else NULL
  
  summarize_reasoning <- function(text) {
    if (is.na(text) || text == "N/A") return("N/A")
    
    keywords <- c()
    if (grepl("credit|lending|loan", text, ignore.case = TRUE)) keywords <- c(keywords, "Credit Growth")
    if (grepl("house|property|real estate|mortgage", text, ignore.case = TRUE)) keywords <- c(keywords, "Real Estate")
    if (grepl("debt|leverage|indebted", text, ignore.case = TRUE)) keywords <- c(keywords, "Indebtedness")
    if (grepl("resilience|buffer|shock|loss", text, ignore.case = TRUE)) keywords <- c(keywords, "Systemic Resilience")
    if (grepl("neutral|standard|cycle|baseline", text, ignore.case = TRUE)) keywords <- c(keywords, "Positive Neutral Framework")
    if (grepl("GDP|growth|economy", text, ignore.case = TRUE)) keywords <- c(keywords, "Macro Trends")
    
    if (length(keywords) == 0) return("General Macro-Financial Monitoring")
    return(paste(unique(keywords), collapse = " | "))
  }
  
  df <- raw_df %>%
    mutate(
      date = as.Date(as.numeric(application_since), origin = "1899-12-30"),
      rate = if(!is.null(rate_col)) as.numeric(.[[rate_col]]) else 0,
      credit_gap = if(!is.null(gap_col)) as.numeric(.[[gap_col]]) else NA,
      credit_to_gdp = if(!is.null(gdp_col)) as.numeric(.[[gdp_col]]) else NA,
      full_reasoning = if("justification" %in% colnames(raw_df)) justification else "N/A"
    ) %>%
    mutate(
      reasoning = sapply(full_reasoning, summarize_reasoning),
      iso2 = countrycode(country, "country.name", "iso2c", custom_match = c("Greece" = "GR"))
    ) %>%
    filter(!is.na(date), !is.na(rate)) %>%
    arrange(country, date)
  
  # Calculate aggregate trend
  unique_dates <- sort(unique(df$date))
  agg_trend <- lapply(unique_dates, function(d) {
    df %>%
      filter(date <= d) %>%
      group_by(country) %>%
      filter(date == max(date)) %>%
      ungroup() %>%
      summarise(date = d, n_positive = sum(rate > 0, na.rm = TRUE))
  }) %>% bind_rows()
  
  # Latest state per country
  latest_df <- df %>%
    group_by(country) %>%
    filter(date == max(date)) %>%
    slice(1) %>%
    ungroup()
  
  num_countries <- n_distinct(df$country)
  active_buffers <- sum(latest_df$rate > 0, na.rm = TRUE)
  
  # Positive Neutral Detection - High Precision List
  pn_list <- c("Belgium", "Bulgaria", "Cyprus", "Denmark", "Estonia", "Germany", 
               "Ireland", "Lithuania", "Luxembourg", "Netherlands", "Norway", 
               "Portugal", "Slovenia", "Sweden")
  
  neutral_df <- latest_df %>%
    filter(country %in% pn_list) %>%
    select(country, date, rate) %>%
    arrange(country)
  
  # Shared Data for Historical Filtering
  shared_df <- SharedData$new(df, key = ~country, group = "Select Countries")

} else {
  stop("Data file not found after attempted download.")
}

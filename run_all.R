library(rmarkdown)
library(ggplot2)
library(knitr)
if (!require("here")) {
  options(repos = c(CRAN = "https://cran.rstudio.com"))
  install.packages("here")
}

Sys.setenv(PATH = paste("C:/Users/alex_/anaconda3/envs/rstudio/Scripts", Sys.getenv("PATH"), sep = ";"))

# Source the configuration file
source("scripts/config.R")

# Source the data loader script to get all necessary dataframes
source("scripts/data_loader.R")
save(df, agg_trend, latest_df, num_countries, active_buffers, neutral_df, shared_df, file = "data/processed_data.RData")

message("Rendering CCyB Tracker Report...")
rmarkdown::render(paste0(reports_dir, "CCyB_Tracker.Rmd"), 
                  output_file = "index.html", 
                  output_dir = ".")

# Re-create ccyb_evolution.png using the data from data_loader.R
# This was originally in CCyB_refresher.R, but is now a separate output.
p <- df %>%
  ggplot(aes(x = date, y = rate, color = country)) +
  geom_step(linewidth = 1.2) + 
  labs(title = "Evolution of Countercyclical Capital Buffer (CCyB) Rates",
       subtitle = "Post-COVID tightening cycle is visible in most economies",
       y = "CCyB Rate (%)",
       x = "Date") +
  theme_minimal()

ggsave("ccyb_evolution.png", p, width = 10, height = 6)
message("Plot saved as ccyb_evolution.png")

message("All reports and plots rendered successfully!")
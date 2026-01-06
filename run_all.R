library(rmarkdown)
library(ggplot2)

# Source the configuration file
source("scripts/config.R")

# Source the data loader script to get all necessary dataframes
source("scripts/data_loader.R")

message("Rendering CCyB Tracker Report...")
rmarkdown::render(paste0(reports_dir, "CCyB_Tracker.Rmd"), 
                  output_file = "CCyB_Tracker.html", 
                  output_dir = output_dir)

message("Rendering CCyB Dashboard Report...")
rmarkdown::render(paste0(reports_dir, "CCyB_Dashboard.Rmd"), 
                  output_file = "CCyB_Dashboard.html", 
                  output_dir = output_dir)

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

ggsave(paste0(output_dir, "ccyb_evolution.png"), p, width = 10, height = 6)
message("Plot saved as output/ccyb_evolution.png")

message("All reports and plots rendered successfully!")
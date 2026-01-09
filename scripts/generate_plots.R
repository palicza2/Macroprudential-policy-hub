if (!require("pacman")) install.packages("pacman", repos = "https://cran.rstudio.com/")
pacman::p_load(arrow, dplyr, ggplot2, plotly, ggrepel) # Added ggrepel as it's used in the Rmd for risk analysis plot

# --- Configuration ---
DATA_DIR <- "data/"
OUTPUT_DIR <- "output/"

# Ensure output directory exists
if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR)
}

# --- Load Data ---
processed_data_path <- file.path(DATA_DIR, "processed_data.parquet")
agg_trend_path <- file.path(DATA_DIR, "agg_trend.parquet")
latest_country_path <- file.path(DATA_DIR, "latest_country.parquet")

if (file.exists(processed_data_path)) {
  df <- read_parquet(processed_data_path)
} else {
  stop(paste("Error: File not found at", processed_data_path))
}

if (file.exists(agg_trend_path)) {
  agg_trend <- read_parquet(agg_trend_path)
} else {
  stop(paste("Error: File not found at", agg_trend_path))
}

if (file.exists(latest_country_path)) {
  latest_df <- read_parquet(latest_country_path)
} else {
  stop(paste("Error: File not found at", latest_country_path))
}

# --- Generate Plots ---

# Macroprudential Diffusion Plot (p_agg)
p_agg <- agg_trend %>%
  ggplot(aes(x = date, y = n_positive)) +
  geom_step(color = "#0984e3", linewidth = 1.2) +
  geom_area(fill = "#0984e3", alpha = 0.05) +
  labs(y = "Number of Countries", x = "Date") +
  theme_minimal(base_family = "Inter") +
  theme(panel.grid.minor = element_blank(),
        axis.title = element_text(size = 11, face = "bold"))

# Save p_agg as PNG
png(file.path(OUTPUT_DIR, "macroprudential_diffusion.png"), width = 1000, height = 600, res = 100)
print(p_agg)
dev.off()

# Current Landscape (Cross-sectional Snapshot) Bar Plot (p_bar)
p_bar <- latest_df %>%
  arrange(desc(rate)) %>%
  mutate(country = factor(country, levels = country)) %>%
  ggplot(aes(x = country, y = rate, fill = rate, text = paste("Country:", country, "<br>Rate:", rate, "%"))) +
  geom_bar(stat = "identity", width = 0.7) +
  scale_fill_gradient(low = "#74b9ff", high = "#0984e3") +
  labs(y = "CCyB Rate (%)", x = "") +
  theme_minimal(base_family = "Inter") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1, size = 10),
        panel.grid.major.x = element_blank(),
        axis.title.y = element_text(size = 11, face = "bold"))

# Save p_bar as PNG
png(file.path(OUTPUT_DIR, "cross_sectional_snapshot.png"), width = 1000, height = 600, res = 100)
print(p_bar)
dev.off()

# Historical Evolution Plot (p_hist)
p_hist <- ggplot(df, aes(x = date, y = rate, color = country, group = country,
                                 text = paste("Country:", country, "<br>Date:", date, "<br>Rate:", rate, "%"))) +
  geom_step(linewidth = 1) +
  labs(y = "CCyB Rate (%)", x = "Effective Date") +
  theme_minimal(base_family = "Inter") +
  theme(legend.position = "none",
        axis.title = element_text(size = 11, face = "bold"))

# Save p_hist as PNG
png(file.path(OUTPUT_DIR, "historical_evolution.png"), width = 1000, height = 600, res = 100)
print(p_hist)
dev.off()

# Risk Analysis (Credit vs. CCyB Setting) Scatter Plot
p_risk <- latest_df %>%
  filter(!is.na(credit_gap), !is.na(rate)) %>%
  ggplot(aes(x = credit_gap, y = rate)) +
  geom_vline(xintercept = 2, linetype = "dashed", color = "#dfe6e9") +
  geom_hline(yintercept = 0, color = "#2d3436", alpha = 0.3) +
  geom_point(aes(size = credit_to_gdp, fill = rate), shape = 21, color = "white", alpha = 0.8) +
  geom_text_repel(aes(label = iso2), 
                  box.padding = 0.5, 
                  point.padding = 0.3,
                  segment.color = '#636e72',
                  size = 4.5,
                  fontface = "bold") +
  scale_fill_gradient(low = "#0984e3", high = "#d63031") +
  scale_size_continuous(range = c(5, 18)) +
  labs(
    x = "Credit-to-GDP Gap (percentage points)",
    y = "CCyB Rate (%)",
    size = "Credit/GDP Ratio",
    fill = "Rate"
  ) +
  theme_minimal(base_family = "Inter") +
  theme(axis.title = element_text(size = 11, face = "bold"),
        panel.grid.minor = element_blank())

# Save p_risk as PNG
png(file.path(OUTPUT_DIR, "risk_analysis.png"), width = 1000, height = 700, res = 100)
print(p_risk)
dev.off()


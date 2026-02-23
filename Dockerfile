# Macroprudential Hub - Pipeline Runner
# Builds the AI-powered dashboard from ESRB data.

FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Ensure output directories exist
RUN mkdir -p data figures reports/partials reports/plots reports/downloads

# Run the pipeline (output: index.html, reports/, figures/, data/)
CMD ["python", "main.py"]

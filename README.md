# Movie Data Enrichment Scraper

## 🚀 Project Overview

This Python script is part of a larger Data Engineering project (MovieLens ELT Pipeline). Its primary function is to enrich the MovieLens dataset by scraping the web to collect additional details for over **27,000 movies**.

The extracted data (movie poster, budget, revenue, and original language) is crucial for providing valuable context and enhancing the overall dataset before loading into the data warehouse.

## 🛠️ Technologies Used

*   **Python:** The core scripting language.
*   **Requests:** HTTP library for making web requests.
*   **BeautifulSoup (bs4):** Library for parsing HTML and XML documents.
*   **Zyte API:** Used for proxy management to handle large-scale scraping and ensure reliable data collection from over 27,000 pages.

## 📋 Prerequisites

Before running the script, ensure you have the following:

1.  **Python:** Installed on your system.
2.  **Zyte API Key:** A valid API key is required for proxy handling.
3.  **Required Python Libraries:** You will need to install the following packages:
    ```bash
    pip install requests beautifulsoup4
    ```

## ⚙️ Setup and Execution

1.  **Set Environment Variables:**
    The script requires your Zyte API key. It is recommended to set this as an environment variable for security:
    ```bash
    export ZYTE_API_KEY="YOUR_ZYTE_API_KEY"
    ```

2.  **Run the Scraper:**
    Execute the main Python script. The script will:
    *   Iterate through the list of movies.
    *   Use the `requests` library with the Zyte API to fetch the required web pages.
    *   Use `BeautifulSoup` to parse the HTML and extract the enrichment data (poster, budget, revenue, original language).
    *   Save the collected data into a local CSV file.

## 💾 Data Output

The script generates an enriched dataset which is saved to a CSV file. This file is then used as a source for the subsequent steps in the ELT pipeline.

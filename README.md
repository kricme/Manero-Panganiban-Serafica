# NLP-Based Sentiment Analysis of Bangko Sentral ng Pilipinas Speeches as Signals and Predictors of the Philippine Interest Rate Movement

This repository contains the relevant code, datasets, and replication package for our undergraduate thesis. This study analyzes the governor speeches of the Bangko Sentral ng Pilipinas (BSP) using natural language processing (NLP) to explore if central bank communication tone can serve as signals and predictors of Philippine interest rate movements.

**Thesis Group:**
*   Chrislyn Mañero
*   Emrick Panganiban
*   Katrina Serafica

> **Important Note on Replication:** The scripts and Jupyter notebooks in this repository rely heavily on the established directory structure to read datasets and save outputs. If you intend to review the code or replicate our findings, please clone or download the **entire repository** to ensure all relative file paths function correctly out-of-the-box.

## Open Data Contribution

In alignment with open science principles, we are making the scraped BSP speeches and our human-annotated sentiment labels publicly available. This allows other researchers to carry out natural language processing (NLP) and macroeconomic studies on Philippine central bank communications without needing to replicate the data collection efforts.

For detailed information on the dataset columns, provenance, and caveats, please refer to the [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## Repository Structure

The repository is structured sequentially to follow the data pipeline from collection to forecasting:

*   **`00 Cleaned Dataset/`**: The centralized "Data Lake" containing the final analytical datasets, including the cleaned speech corpus, the economic variables, and the merged datasets with EconoBERT sentiment scores.
*   **`00 Original Dataset/`**: The raw, untouched datasets downloaded from FactSet, CEIC, and the BSP website, as well as the raw output from the scraper.
*   **`01 Speech Scraper/`**: Contains `main.py`, a custom Python web scraper built using `BeautifulSoup` to extract speeches directly from the BSP website.
*   **`02 Data Pre-Processing and EDA/`**: Jupyter notebooks dedicated to cleaning the raw data, aligning daily variables to monthly frequencies, applying YoY transformations, and generating Exploratory Data Analysis (EDA) visualizations.
*   **`03 Dictionary-based Sentiment Analysis/`**: Baseline sentiment scoring using lexical dictionaries (AFINN, Bing, Syuzhet, NRC).
*   **`04 Transformer-based Sentiment Analysis/`**: Advanced contextual sentiment scoring using pre-trained and fine-tuned BERT models (FinBERT, EconoBERT, RoBERTa).
*   **`05 Speech and Econ Dataset Merging/`**: Scripts aligning the monthly economic data with the aggregated speech sentiment scores.
*   **`06 Regression.ipynb`**: The baseline OLS regression models testing the explanatory power of sentiment variables against the Interbank Call Loan Rate (ICLR).
*   **`07 Forecasting.ipynb`**: Out-of-sample forecasting models (ARIMA, VAR) evaluating the predictive accuracy of adding tone scores to standard macroeconomic models.

## How to Use the Scraper

To fetch the latest speeches from the BSP website:

1. Navigate to the `01 Speech Scraper` directory.
2. Install the required dependencies: `pip install -r requirements.txt`
3. Run the scraper: `python main.py`
4. Follow the prompt to enter a date range. The scraper will automatically clean the HTML transcription and save the output as both JSON and CSV in the `00 Original Dataset/speech dataset/bsp_speeches/` folder.

## License

This dataset and code are released under an open-source license to facilitate further research in macroeconomic NLP. (Please insert specific license type here, e.g., MIT License or CC-BY-4.0).

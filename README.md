# NLP-Based Sentiment Analysis of Bangko Sentral ng Pilipinas Speeches as Signals and Predictors of the Philippine Interest Rate Movement

This repository contains the relevant code, datasets, and replication package for our undergraduate thesis. This study analyzes the governor speeches of the Bangko Sentral ng Pilipinas (BSP) using natural language processing (NLP) to explore if central bank communication tone can serve as signals and predictors of Philippine interest rate movements.

**Thesis Group:**
*   Chrislyn Mañero
*   Emrick Panganiban
*   Katrina Serafica

**Thesis Advisers:**
*   Scott Chua
*   Richell Flores

> **Important Note on Replication:** The scripts and Jupyter notebooks in this repository rely heavily on the established directory structure to read datasets and save outputs. If you intend to review the code or replicate our findings, please clone or download the **entire repository** to ensure all relative file paths function correctly out-of-the-box.

## Replication Guide

To ensure replicability and avoid local operating system, hardware, or dependency conflicts (such as R-Python bridges or GPU setup for Transformers), we have divided the execution pipeline into two phases. 

> [!NOTE]
> The final datasets generated in Phase 1 are already provided in the repository, so reviewers can skip directly to **Phase 2** if they only wish to evaluate the models and results.

### Phase 1: Data Collection & Pre-Processing (Local Execution)
*Used for Folders `01` and `02`.*

#### How to Use the Scraper

To fetch the latest speeches from the BSP website:

1. Navigate to the `01 Speech Scraper` directory on your local machine.
2. Install the required dependencies: `pip install -r requirements.txt`
3. Run the scraper: `python main.py`
4. Follow the prompt to enter a date range. The scraper will automatically clean the HTML transcription and save the output as both JSON and CSV in the `00 Original Dataset/speech dataset/bsp_speeches/` folder.

### Phase 2: Sentiment Analysis & Modeling (Google Colab Recommended)
*Used for Folders `03` through `07`.*

Because Folder `03` (R-Python bridging via `rpy2`) and Folder `04` (contextual Transformer models like FinBERT and RoBERTa) require specific dependencies and heavy GPU computation, we highly recommend running the rest of the pipeline in Google Colab.

#### How to Run Phase 2 in Colab:
1. Open [Google Colab](https://colab.research.google.com/).
2. Open a blank notebook.
3. Clone this repository into the Colab environment by running:
   ```bash
   !git clone https://github.com/kricme/manero-panganiban-serafica.git
4. Change the working directory to the repo root:
   ```bash
   %cd manero-panganiban-serafica
5. Open Notebooks `03` through `07` directly in Colab and execute them in order. All notebooks contain setup blocks that will automatically pull updates and configure relative paths out-of-the-box.

> [!IMPORTANT]
> Please ensure your Colab runtime is set to use a **T4 GPU** (via *Runtime > Change runtime type*) before executing the notebooks in **`04 Transformer-based Sentiment Analysis/`**.

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
*   **`06 Regression/`**: Contains the regression notebook and its output tables/figures evaluating the explanatory power of central bank tone on interest rate adjustments.
*   **`07 Forecasting/`**: Contains the forecasting notebook and its output predictive tables/plots assessing the forecasting accuracy of adding tone scores to macroeconomic models.



## License

This dataset and code are released under an open-source license to facilitate further research in macroeconomic NLP. (Please insert specific license type here, e.g., MIT License or CC-BY-4.0).

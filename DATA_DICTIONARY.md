# Data Dictionary

This document explains the structure, provenance, and meaning of the data variables included in this repository.

## 1. Cleaned BSP Speech Dataset
**File:** `00 Cleaned Dataset/01 speech dataset/bsp_speech_dataset.csv`

This dataset contains the governor speeches from the Bangko Sentral ng Pilipinas (BSP), scraped directly from their official website and cleaned of HTML and encoding artifacts. Duplicate speeches (e.g., website upload errors) have been algorithmically removed via Cosine Similarity analysis (>98.5% threshold).

| Column Name | Data Type | Provenance | Description |
| :--- | :--- | :--- | :--- |
| `Title` | String | Scraped | The official title of the speech as listed on the BSP website. |
| `Date` | Date | Scraped | The date the speech was delivered (Format: DD-MM-YYYY). |
| `Location` | String | Scraped | The geographic location or venue where the speech was delivered. |
| `Occasion` | String | Scraped | The event or conference at which the speech was given. |
| `Speaker` | String | Derived | The standardized name of the BSP official (titles removed). |
| `Role` | String | Derived | The official capacity of the speaker (`Governor`, `Deputy Governor`, `Officer-in-Charge`). |
| `Text` | String | Scraped | The full, cleaned text transcription of the speech. |
| `Len` | Integer | Derived | The total word count of the `Text` column. |
| `Link` | String | Derived | The direct URL to the original speech on the BSP website. |
| `last_name` | String | Derived | The extracted surname of the speaker. |
| `year` | Integer | Derived | The extracted year of the speech date. |


## 2. Human-Annotated Sentiment Dataset
**File:** `00 Original Dataset/speech dataset/labeled_sentences_1.csv`

This file contains randomly sampled sentences from the BSP speech corpus that have been manually annotated for sentiment. This dataset was used to fine-tune the EconoBERT, RoBERTa, and FinBERT models.

| Column Name | Data Type | Provenance | Description |
| :--- | :--- | :--- | :--- |
| `sentence` | String | Scraped | A single sentence extracted from the BSP corpus. |
| `label` | String | Manual | The human-annotated sentiment class (`positive`, `negative`, or `neutral`). |

## 3. Economic Dataset
**File:** `00 Cleaned Dataset/03 economic dataset/withGDP_Econ_Dataset.csv`

This is the analytical dataset representing the macroeconomic "environment." Daily data (such as the ICLR) have been aggregated into monthly averages to ensure consistency with other macro indicators. The independent variables have been subjected to Year-over-Year (YoY) transformations to ensure stationarity for modeling.

| Column Name | Data Type | Provenance | Description |
| :--- | :--- | :--- | :--- |
| `Date` | Date | Derived | The first day of the month for the observation period (Format: YYYY-MM-DD). |
| `Interbank Call Loan Rate` | Float | CEIC | The monthly average of the daily overnight lending rate among banks (Target Variable). |
| `Real GDP` | Float | FactSet | The YoY percentage change in the monthly inflation-adjusted value of total economic output. |
| `CPI` | Float | FactSet | The YoY percentage change in the monthly Consumer Price Index (Inflation). |
| `Wholesale Price` | Float | FactSet | The YoY percentage change in the monthly Wholesale Price Index. |
| `Industrial Production`| Float | FactSet | The YoY percentage change in the monthly manufacturing and mining output. |
| `Intl Trade Merch Exports`| Float | FactSet | The YoY percentage change in the monthly total value of goods sold abroad. |
| `Intl Trade Merch Imports`| Float | FactSet | The YoY percentage change in the monthly total value of goods purchased from abroad. |
| `FX Rate` | Float | BSP | The YoY percentage change in the monthly Peso-Dollar exchange rate. |

*Note: A parallel dataset `withoutGDP_Econ_Dataset.csv` is also provided, offering a longer historical time series by excluding Real GDP (which is only available from April 2000).*

## 4. Economic Data with EconoBERT Results Dataset
**File:** `00 Cleaned Dataset/04 econ data with econobert results/withGDP_IRD_with_tone_dataset_jittered.csv`

This is the final analytical dataset containing both the economic data and the tone values obtained from the EconoBERT model. Because speeches are not delivered every month, the monthly sentiment metrics were forward-filled from the last available month to create a continuous time series. To prevent artificial zero-variance sequences (plateaus) introduced by forward-filling, mean-zero Gaussian noise (jitter) was applied strictly to the forward-filled runs, retaining natural variance for time-series modeling.

| Column Name | Data Type | Provenance | Description |
| :--- | :--- | :--- | :--- |
| `Date` | Date | Derived | The first day of the month for the observation period (Format: YYYY-MM-DD). |
| `Interbank Call Loan Rate` | Float | CEIC | The monthly average of the daily overnight lending rate among banks (Target Variable). |
| `Real GDP` | Float | FactSet | The YoY percentage change in the monthly inflation-adjusted value of total economic output. |
| `CPI` | Float | FactSet | The YoY percentage change in the monthly Consumer Price Index (Inflation). |
| `Wholesale Price` | Float | FactSet | The YoY percentage change in the monthly Wholesale Price Index. |
| `Industrial Production`| Float | FactSet | The YoY percentage change in the monthly manufacturing and mining output. |
| `Intl Trade Merch Exports`| Float | FactSet | The YoY percentage change in the monthly total value of goods sold abroad. |
| `Intl Trade Merch Imports`| Float | FactSet | The YoY percentage change in the monthly total value of goods purchased from abroad. |
| `FX Rate` | Float | BSP | The YoY percentage change in the monthly Peso-Dollar exchange rate. |
| `tone_mean` | Float | Derived | The average tone of speeches delivered in a given month. |
| `pos_mean` | Float | Derived | The average share of positive sentences across speeches in a given month. |
| `neg_mean` | Float | Derived | The average share of negative sentences across speeches in a given month. |
| `neu_mean` | Float | Derived | The average share of neutral sentences across speeches in a given month. |
| `speech_count` | Integer | Derived | The total number of speeches delivered during that month. |
| `tone_mean_filled` | Float | Derived | The `tone_mean` values forward-filled to cover months with zero speeches. |
| `pos_mean_filled` | Float | Derived | The `pos_mean` values forward-filled to cover months with zero speeches. |
| `neg_mean_filled` | Float | Derived | The `neg_mean` values forward-filled to cover months with zero speeches. |
| `neu_mean_filled` | Float | Derived | The `neu_mean` values forward-filled to cover months with zero speeches. |
| `tone_mean_jittered` | Float | Derived | The forward-filled `tone_mean` with mean-zero Gaussian jitter added to plateau runs. |
| `pos_mean_jittered` | Float | Derived | The forward-filled `pos_mean` with mean-zero Gaussian jitter added to plateau runs. |
| `neg_mean_jittered` | Float | Derived | The forward-filled `neg_mean` with mean-zero Gaussian jitter added to plateau runs. |
| `neu_mean_jittered` | Float | Derived | The forward-filled `neu_mean` with mean-zero Gaussian jitter added to plateau runs. |


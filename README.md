# Social Media Content Analytics Pipeline

A beginner data engineering project that builds a complete pipeline
for a fictional social media platform.

## Project Overview

This pipeline ingests raw social media data, cleans it, transforms it,
analyses it, and visualises the results using Python and MySQL.

## Tools & Technologies

| Tool | Purpose |
|---|---|
| MySQL | Database to store raw data |
| MySQL Workbench | Visual database management |
| Python 3 | Pipeline scripting |
| Pandas | Data cleaning and transformation |
| Matplotlib | Data visualisation |
| Faker | Synthetic data generation |
| VS Code | Code editor |

## Project Structure

    social_media_pipeline/
    │
    ├── data/
    │   ├── users.csv
    │   ├── posts.csv
    │   ├── engagement.csv
    │   ├── users_cleaned.csv
    │   ├── posts_cleaned.csv
    │   ├── engagement_cleaned.csv
    │   └── final_combined.csv
    │
    ├── charts/
    │   ├── chart1_posts_per_platform.png
    │   ├── chart2_likes_over_time.png
    │   └── chart3_top_hashtags.png
    │
    ├── generate_data.py
    ├── pipeline.py
    └── README.md

## Pipeline Steps

**Step 1 — Generate:** Created a MySQL database with 3 tables (users,
posts, engagement) and populated each with 200+ rows of synthetic data
using Python's Faker library.

**Step 2 — Ingest:** Exported all 3 tables to CSV files and loaded them
into Pandas DataFrames for processing.

**Step 3 — Clean:** Identified and resolved 5 data quality problems
(see Data Problems section below).

**Step 4 — Transform:** Joined all 3 tables into one combined DataFrame,
calculated engagement rates, and ran aggregations.

**Step 5 — Visualise:** Created 3 charts saved as PNG files.

## Data Problems Found & Fixed

| # | Problem | How It Was Introduced | How It Was Fixed |
|---|---|---|---|
| 1 | NULL usernames | ~10% of users had no username | Filled with 'unknown_user' |
| 2 | Duplicate posts | 5 posts re-inserted with same data | Removed with drop_duplicates() |
| 3 | Negative likes | Every 30th engagement row had likes < 0 | Filtered out rows where likes < 0 |
| 4 | Future post dates | Every 40th post had a future posted_at | Filtered out posts where posted_at > today |
| 5 | Inconsistent platform names | Used 'Insta', 'tiktok', 'YOUTUBE' | Mapped all values to correct Title Case |

## Key Findings

- **TikTok** had the highest number of posts across all platforms
- **Story** posts achieved the highest average engagement rate (0.60)
- **#tech** was the top performing hashtag by total engagement
- After cleaning, the dataset reduced from 225 posts to 208 usable records

## How to Run This Project

1. Clone the repository
2. Install dependencies:

        pip install mysql-connector-python faker pandas matplotlib

3. Create the database in MySQL Workbench using the schema in `generate_data.py`
4. Update the MySQL password in both scripts
5. Run data generation:

        python generate_data.py

6. Run the full pipeline:

        python pipeline.py
## Author

**Bafana Zwane**

Built as part of the Witle Academy Data Engineering Bootcamp — Week 1
Mini Project.

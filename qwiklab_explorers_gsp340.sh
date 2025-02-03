clear

#!/bin/bash
# Define color variables

BLACK=`tput setaf 0`
RED=`tput setaf 1`
GREEN=`tput setaf 2`
YELLOW=`tput setaf 3`
BLUE=`tput setaf 4`
MAGENTA=`tput setaf 5`
CYAN=`tput setaf 6`
WHITE=`tput setaf 7`

BG_BLACK=`tput setab 0`
BG_RED=`tput setab 1`
BG_GREEN=`tput setab 2`
BG_YELLOW=`tput setab 3`
BG_BLUE=`tput setab 4`
BG_MAGENTA=`tput setab 5`
BG_CYAN=`tput setab 6`
BG_WHITE=`tput setab 7`

BOLD=`tput bold`
RESET=`tput sgr0`

# Array of color codes excluding black and white
TEXT_COLORS=($RED $GREEN $YELLOW $BLUE $MAGENTA $CYAN)
BG_COLORS=($BG_RED $BG_GREEN $BG_YELLOW $BG_BLUE $BG_MAGENTA $BG_CYAN)

# Pick random colors
RANDOM_TEXT_COLOR=${TEXT_COLORS[$RANDOM % ${#TEXT_COLORS[@]}]}
RANDOM_BG_COLOR=${BG_COLORS[$RANDOM % ${#BG_COLORS[@]}]}

#----------------------------------------------------start--------------------------------------------------#

echo "${RANDOM_BG_COLOR}${RANDOM_TEXT_COLOR}${BOLD}Starting Execution${RESET}"

# Step 2: Creating BigQuery Schema and Tables
echo "${BLUE}${BOLD}Creating BigQuery Schema and Tables...${RESET}"
bq query --use_legacy_sql=false \
"
-- Create the dataset if it does not exist
CREATE SCHEMA IF NOT EXISTS covid
OPTIONS(
    description='Dataset for COVID-19 Government Response data'
);

-- Create the table with schema from the source table
CREATE OR REPLACE TABLE covid.oxford_policy_tracker
PARTITION BY date
OPTIONS(
    partition_expiration_days=1445,
    description='Oxford Policy Tracker table in the COVID-19 dataset with an expiry time of 1445 days.'
) AS
SELECT
    *
FROM
    \`bigquery-public-data.covid19_govt_response.oxford_policy_tracker\`
WHERE
    alpha_3_code NOT IN ('GBR', 'BRA', 'CAN', 'USA');
"

bq query --use_legacy_sql=false \
"
-- Create the dataset if it does not exist
CREATE SCHEMA IF NOT EXISTS covid_data
OPTIONS(
    description='Dataset for country area data from Census Bureau International public dataset'
);

-- Create the table with the schema from the source table
CREATE OR REPLACE TABLE covid_data.country_area_data
AS
SELECT
    *
FROM
    \`bigquery-public-data.census_bureau_international.country_names_area\`;
"

bq query --use_legacy_sql=false \
"
-- Create the dataset if it does not exist
CREATE SCHEMA IF NOT EXISTS covid_data
OPTIONS(
    description='Dataset for COVID-19 related mobility data'
);

-- Create the table with the schema from the source table
CREATE OR REPLACE TABLE covid_data.mobility_data
AS
SELECT
    *
FROM
    \`bigquery-public-data.covid19_google_mobility.mobility_report\`;
"

# Step 3: Cleaning Data
echo "${MAGENTA}${BOLD}Cleaning Data in BigQuery Tables...${RESET}"
bq query --use_legacy_sql=false \
"
DELETE FROM covid_data.oxford_policy_tracker_by_countries
WHERE population IS NULL;
"

bq query --use_legacy_sql=false \
"
DELETE FROM covid_data.oxford_policy_tracker_by_countries
WHERE country_area IS NULL;
"

echo

# Function to display a random congratulatory message
function random_congrats() {
    MESSAGES=(
        "${GREEN}Congratulations For Completing The Lab! Keep up the great work!${RESET}"
        "${CYAN}Well done! Your hard work and effort have paid off!${RESET}"
        "${YELLOW}Amazing job! You’ve successfully completed the lab!${RESET}"
        "${BLUE}Outstanding! Your dedication has brought you success!${RESET}"
        "${MAGENTA}Great work! You’re one step closer to mastering this!${RESET}"
        "${RED}Fantastic effort! You’ve earned this achievement!${RESET}"
        "${CYAN}Congratulations! Your persistence has paid off brilliantly!${RESET}"
        "${GREEN}Bravo! You’ve completed the lab with flying colors!${RESET}"
        "${YELLOW}Excellent job! Your commitment is inspiring!${RESET}"
        "${BLUE}You did it! Keep striving for more successes like this!${RESET}"
        "${MAGENTA}Kudos! Your hard work has turned into a great accomplishment!${RESET}"
        "${RED}You’ve smashed it! Completing this lab shows your dedication!${RESET}"
        "${CYAN}Impressive work! You’re making great strides!${RESET}"
        "${GREEN}Well done! This is a big step towards mastering the topic!${RESET}"
        "${YELLOW}You nailed it! Every step you took led you to success!${RESET}"
        "${BLUE}Exceptional work! Keep this momentum going!${RESET}"
        "${MAGENTA}Fantastic! You’ve achieved something great today!${RESET}"
        "${RED}Incredible job! Your determination is truly inspiring!${RESET}"
        "${CYAN}Well deserved! Your effort has truly paid off!${RESET}"
        "${GREEN}You’ve got this! Every step was a success!${RESET}"
        "${YELLOW}Nice work! Your focus and effort are shining through!${RESET}"
        "${BLUE}Superb performance! You’re truly making progress!${RESET}"
        "${MAGENTA}Top-notch! Your skill and dedication are paying off!${RESET}"
        "${RED}Mission accomplished! This success is a reflection of your hard work!${RESET}"
        "${CYAN}You crushed it! Keep pushing towards your goals!${RESET}"
        "${GREEN}You did a great job! Stay motivated and keep learning!${RESET}"
        "${YELLOW}Well executed! You’ve made excellent progress today!${RESET}"
        "${BLUE}Remarkable! You’re on your way to becoming an expert!${RESET}"
        "${MAGENTA}Keep it up! Your persistence is showing impressive results!${RESET}"
        "${RED}This is just the beginning! Your hard work will take you far!${RESET}"
        "${CYAN}Terrific work! Your efforts are paying off in a big way!${RESET}"
        "${GREEN}You’ve made it! This achievement is a testament to your effort!${RESET}"
        "${YELLOW}Excellent execution! You’re well on your way to mastering the subject!${RESET}"
        "${BLUE}Wonderful job! Your hard work has definitely paid off!${RESET}"
        "${MAGENTA}You’re amazing! Keep up the awesome work!${RESET}"
        "${RED}What an achievement! Your perseverance is truly admirable!${RESET}"
        "${CYAN}Incredible effort! This is a huge milestone for you!${RESET}"
        "${GREEN}Awesome! You’ve done something incredible today!${RESET}"
        "${YELLOW}Great job! Keep up the excellent work and aim higher!${RESET}"
        "${BLUE}You’ve succeeded! Your dedication is your superpower!${RESET}"
        "${MAGENTA}Congratulations! Your hard work has brought great results!${RESET}"
        "${RED}Fantastic work! You’ve taken a huge leap forward today!${RESET}"
        "${CYAN}You’re on fire! Keep up the great work!${RESET}"
        "${GREEN}Well deserved! Your efforts have led to success!${RESET}"
        "${YELLOW}Incredible! You’ve achieved something special!${RESET}"
        "${BLUE}Outstanding performance! You’re truly excelling!${RESET}"
        "${MAGENTA}Terrific achievement! Keep building on this success!${RESET}"
        "${RED}Bravo! You’ve completed the lab with excellence!${RESET}"
        "${CYAN}Superb job! You’ve shown remarkable focus and effort!${RESET}"
        "${GREEN}Amazing work! You’re making impressive progress!${RESET}"
        "${YELLOW}You nailed it again! Your consistency is paying off!${RESET}"
        "${BLUE}Incredible dedication! Keep pushing forward!${RESET}"
        "${MAGENTA}Excellent work! Your success today is well earned!${RESET}"
        "${RED}You’ve made it! This is a well-deserved victory!${RESET}"
        "${CYAN}Wonderful job! Your passion and hard work are shining through!${RESET}"
        "${GREEN}You’ve done it! Keep up the hard work and success will follow!${RESET}"
        "${YELLOW}Great execution! You’re truly mastering this!${RESET}"
        "${BLUE}Impressive! This is just the beginning of your journey!${RESET}"
        "${MAGENTA}You’ve achieved something great today! Keep it up!${RESET}"
        "${RED}You’ve made remarkable progress! This is just the start!${RESET}"
    )

    RANDOM_INDEX=$((RANDOM % ${#MESSAGES[@]}))
    echo -e "${BOLD}${MESSAGES[$RANDOM_INDEX]}"
}

# Display a random congratulatory message
random_congrats

echo -e "\n"  # Adding one blank line

cd

remove_files() {
    # Loop through all files in the current directory
    for file in *; do
        # Check if the file name starts with "gsp", "arc", or "shell"
        if [[ "$file" == gsp* || "$file" == arc* || "$file" == shell* ]]; then
            # Check if it's a regular file (not a directory)
            if [[ -f "$file" ]]; then
                # Remove the file and echo the file name
                rm "$file"
                echo "File removed: $file"
            fi
        fi
    done
}

remove_files




export DATASET_CP1=covid
export DATASET_CP2=covid_data


gcloud auth list

export PROJECT_ID=$(gcloud config get-value project)

export PROJECT_ID=$DEVSHELL_PROJECT_ID

bq mk $DEVSHELL_PROJECT_ID:covid

sleep 15

bq query --use_legacy_sql=false \
"
CREATE OR REPLACE TABLE $DATASET_CP1.oxford_policy_tracker
PARTITION BY date
OPTIONS(
partition_expiration_days=1445,
description='oxford_policy_tracker table in the COVID 19 Government Response public dataset with  an expiry time set to 90 days.'
) AS
SELECT
   *
FROM
   \`bigquery-public-data.covid19_govt_response.oxford_policy_tracker\`
WHERE
   alpha_3_code NOT IN ('GBR', 'BRA', 'CAN','USA')
"




bq query --use_legacy_sql=false \
"
ALTER TABLE $DATASET_CP2.global_mobility_tracker_data
ADD COLUMN population INT64,
ADD COLUMN country_area FLOAT64,
ADD COLUMN mobility STRUCT<
   avg_retail      FLOAT64,
   avg_grocery     FLOAT64,
   avg_parks       FLOAT64,
   avg_transit     FLOAT64,
   avg_workplace   FLOAT64,
   avg_residential FLOAT64
>

"




bq query --use_legacy_sql=false \
"
CREATE OR REPLACE TABLE $DATASET_CP2.pop_data_2019 AS
SELECT
  country_territory_code,
  pop_data_2019
FROM 
  \`bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide\`
GROUP BY
  country_territory_code,
  pop_data_2019
ORDER BY
  country_territory_code
"  



bq query --use_legacy_sql=false \
"
UPDATE
   \`$DATASET_CP2.consolidate_covid_tracker_data\` t0
SET
   population = t1.pop_data_2019
FROM
   \`$DATASET_CP2.pop_data_2019\` t1
WHERE
   CONCAT(t0.alpha_3_code) = CONCAT(t1.country_territory_code);
"   


bq query --use_legacy_sql=false \
"
UPDATE
   \`$DATASET_CP2.consolidate_covid_tracker_data\` t0
SET
   t0.country_area = t1.country_area
FROM
   \`bigquery-public-data.census_bureau_international.country_names_area\` t1
WHERE
   t0.country_name = t1.country_name
"




bq query --use_legacy_sql=false \
"
 UPDATE
   \`$DATASET_CP2.consolidate_covid_tracker_data\` t0
SET
   t0.mobility.avg_retail      = t1.avg_retail,
   t0.mobility.avg_grocery     = t1.avg_grocery,
   t0.mobility.avg_parks       = t1.avg_parks,
   t0.mobility.avg_transit     = t1.avg_transit,
   t0.mobility.avg_workplace   = t1.avg_workplace,
   t0.mobility.avg_residential = t1.avg_residential
FROM
   ( SELECT country_region, date,
      AVG(retail_and_recreation_percent_change_from_baseline) as avg_retail,
      AVG(grocery_and_pharmacy_percent_change_from_baseline)  as avg_grocery,
      AVG(parks_percent_change_from_baseline) as avg_parks,
      AVG(transit_stations_percent_change_from_baseline) as avg_transit,
      AVG(workplaces_percent_change_from_baseline) as avg_workplace,
      AVG(residential_percent_change_from_baseline)  as avg_residential
      FROM \`bigquery-public-data.covid19_google_mobility.mobility_report\`
      GROUP BY country_region, date
   ) AS t1
WHERE
   CONCAT(t0.country_name, t0.date) = CONCAT(t1.country_region, t1.date)
"



bq query --use_legacy_sql=false \
"
SELECT DISTINCT country_name
FROM \`$DATASET_CP2.oxford_policy_tracker_worldwide\`
WHERE population is NULL
UNION ALL
SELECT DISTINCT country_name
FROM \`$DATASET_CP2.oxford_policy_tracker_worldwide\`
WHERE country_area IS NULL
ORDER BY country_name ASC
"





bq query --use_legacy_sql=false \
"
CREATE TABLE $DATASET_CP2.country_area_data AS
SELECT *
FROM \`bigquery-public-data.census_bureau_international.country_names_area\`;
"


# Create a new table 'mobility_data' in the 'covid_data' dataset
bq query --use_legacy_sql=false \
"CREATE TABLE $DATASET_CP2.mobility_data AS
SELECT *
FROM \`bigquery-public-data.covid19_google_mobility.mobility_report\`"




bq query --use_legacy_sql=false \
"DELETE FROM covid_data.oxford_policy_tracker_by_countries
WHERE population IS NULL AND country_area IS NULL"

bq query --use_legacy_sql=false \
"
DELETE FROM \`covid_data.oxford_policy_tracker_by_countries\`
WHERE 
    \`population\` IS NULL AND 
    \`country_area\` IS NULL
"





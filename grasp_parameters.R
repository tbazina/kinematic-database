# Launch Apache Spark locally and analyze data

library(R.matlab)
library(tidyverse)
# Pasting values from web as tribble
library(datapasta)
# Apache Spark interface
library(sparklyr)
# Database Interface
library(DBI)
# Plotting inside a database
library(dbplot)

# Install Apache Spark
# spark_install()
# spark_installed_versions()

# Configuration for local use
## Initialize configuration with defaults
local_config <- spark_config()
## Memory
local_config$`sparklyr.shell.driver-memory` <- "6G"
## Memory fraction (default 60 %)
local_config$`spark.memory.fraction` <- 0.8
## Cores
local_config$`sparklyr.cores.local` <- 8

# Connect to Spark locally (use version 3.0+ with Java 11)
sc <- spark_connect(master = 'local', version = '3.1', config = local_config)

# Launch web interface
spark_web(sc)

# Call SQL inside Spark
sqlfunction <- function(sc, block) {
  spark_session(sc) %>% invoke("sql", block)
  }

# # Load data
# loadDBSpark <- function(sc, subjects=1:27, folder='clean_data') {
#   # Loop over subjects and exercise and read parquet files in Spark
#   for (subject in subjects) {
#     for (exercise in 1:2) {
#       path = paste0(folder, '/s', subject, '_E', exercise, '.parquet')
#       print(paste('Loading:', path))
#       spark_read_parquet(
#         sc,
#         name = paste0('s', subject, '_e', exercise),
#         path = path,
#         memory = F,
#         overwrite = T
#         )
#     }
#   }
#   # sc %>% spark_session() %>% invoke("catalog") %>% invoke("dropTempView", "iris")
# }
# loadDBSpark(sc, subjects = 1:77)
# 
# # Names of all tables loaded in Spark
# table_names <- src_tbls(sc)
# # List of views of all spark tables 
# dat_list <- lapply(table_names, function(name) {tbl(sc, name)})
# # nrows of all tables and total nrows
# sapply(dat_list, sdf_nrow)
# sum(sapply(dat_list, sdf_nrow))
# # Bind rows of all tables and save to clean_data folder
# sdf_bind_rows(dat_list) %>% spark_write_parquet(
#   path = 'clean_data/s1-77_e1-2.parquet',
#   mode = 'overwrite'
# )

dat_full <- spark_read_parquet(
  sc,
  name = 'dat_full',
  path = 'clean_data/s1-77_e1-2.parquet',
  memory = F,
  overwrite = T
  )

# Dataset dimesion
dat_full %>% sdf_nrow()
dat_full %>% dim()

# Count NA or NaN values over entire dataset
# is.nan is not working but isnan is
dat_na <- dat_full %>% mutate_all(is.na) %>% mutate_all(as.numeric) %>% summarise_all(sum) %>% collect()
dat_na %>% View()
dat_nan <- dat_full %>% mutate_all(~isnan(.)) %>% mutate_all(as.numeric) %>% summarise_all(sum) %>% collect()
dat_nan %>% View()
# MCP2_f contains only NaN values -> drop for further investigation
dat_full %>% select(-MCP2_a)

# Check data types (schema) and unique values
dat_spec <- dat_full %>% select(-MCP2_a) %>% head() %>% collect() %>% sapply(class)
dat_spec %>% View()
dat_full %>% select(exercise, restimulus, rerepetition) %>% sdf_distinct() %>%
  collect() %>% sapply(unique)
# Exercise and rerepetition can be integers (not numeric)
dat_spec <- dat_full %>% select(-MCP2_a) %>% mutate(
  exercise = as.integer(exercise),
  rerepetition = as.integer(rerepetition)
) %>% head() %>% collect() %>% sapply(class)
dat_spec %>% View()

# Group by subject, exercise and restimulus and get descriptive statistics
dat_summary <- dat_full %>% select(-MCP2_a) %>% mutate(
  exercise = as.integer(exercise),
  rerepetition = as.integer(rerepetition)
) %>% group_by(exercise, restimulus) %>% 
  summarise_if(
    .predicate = is.numeric,
    .funs = list(
      'min' = ~min(., na.rm=T), 
      'max' = ~max(., na.rm=T), 
      'mean' = ~mean(., na.rm=T),
      # 'median' = ~percentile_approx(., 0.5),
      'sd' = ~sd(., na.rm=T)
      # 'IQR' = ~(percentile_approx(., 0.75) - percentile_approx(., 0.25))
      )
  ) %>% collect()
dat_summary %>% arrange(exercise, restimulus) %>% View()

# Remove duplicate values to shorten dataset -> distinct only
dat_full %>% select(-MCP2_a) %>% mutate(
  exercise = as.integer(exercise),
  rerepetition = as.integer(rerepetition)
  ) %>% sdf_distinct() %>%
  arrange(subject, exercise, restimulus) %>% 
  spark_write_parquet(
  path = 'clean_data/s1-77_e1-2_distinct.parquet',
  mode = 'overwrite'
)

# Load only distinct database
dat_full <- spark_read_parquet(
  sc,
  name = 'dat_full',
  path = 'clean_data/s1-77_e1-2_distinct.parquet',
  memory = F,
  overwrite = T
  )

# Extract only MCP, PIP and DIP flexion joint angles for fingers 2-5 and save
dat_full %>%
  select(
    subject, laterality, gender, age, height, weight, exercise, restimulus, rerepetition,
    starts_with(c('MCP', 'PIP', 'DIP'))
    ) %>% 
  select(-MCP1, -MCP4_a, -MCP5_a) %>% 
  spark_write_parquet(
    path = 'clean_data/s1-77_e1-2_distinct_mcp_pip_dip.parquet',
    mode = 'overwrite'
    )

# Load only MCP, PIP and DIP dataset in Spark
dat_mcp_pip_dip <- spark_read_parquet(
  sc,
  name = 'dat_mcp_pip_dip',
  path = 'clean_data/s1-77_e1-2_distinct_mcp_pip_dip.parquet',
  memory = F,
  overwrite = T
  )

# Descriptive statistics on data
dat_summary <- dat_mcp_pip_dip %>%
  select(!c(subject, laterality, gender, age, height, weight, rerepetition)) %>% 
  # Filter out rest position
  filter(restimulus != 0) %>%
  group_by(exercise, restimulus) %>% 
  summarise_if(
    .predicate = is.numeric,
    .funs = list(
      'min' = ~min(., na.rm=T), 
      'max' = ~max(., na.rm=T), 
      'mean' = ~mean(., na.rm=T),
      # 'median' = ~percentile_approx(., 0.5),
      'sd' = ~sd(., na.rm=T)
      # 'IQR' = ~(percentile_approx(., 0.75) - percentile_approx(., 0.25))
      )
  ) %>% collect()
dat_summary %>% arrange(exercise, restimulus) %>% View()

# Boxplot of MCP, PIP, DIP
dat_mcp_pip_dip %>% 
  select(!c(subject, laterality, gender, age, height, weight, rerepetition)) %>% 
  # Filter out rest position
  filter(restimulus != 0) %>%
  mutate(
    exercise.restimulus = paste0(exercise, '-', restimulus)
  ) %>%
  select(-exercise, -restimulus) %>% 
  db_compute_boxplot(x = exercise.restimulus, var = MCP2_f)

# Disconnect from Spark
spark_disconnect(sc)
spark_disconnect_all()

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
# Plotting
library(ggplot2)
library(ggsci)
library(ggmosaic)
library(RColorBrewer)
# Correlation
library(corrr)

# Install Apache Spark
# spark_install()
# spark_installed_versions()

# Configuration for local use
## Initialize configuration with defaults
local_config <- spark_config()
## Memory
local_config$`sparklyr.shell.driver-memory` <- "6G"
## Memory fraction (default 60 %)
local_config$`spark.memory.fraction` <- 0.4
## Cores
local_config$`sparklyr.cores.local` <- 12

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
      'median' = ~percentile_approx(., 0.5),
      'sd' = ~sd(., na.rm=T)
      # 'IQR' = ~(percentile_approx(., 0.75) - percentile_approx(., 0.25))
      )
  ) %>% collect()
dat_summary %>% arrange(exercise, restimulus) %>% View()

# Boxplot data for MCP, PIP, DIP
dat_boxplot <- dat_mcp_pip_dip %>% 
  select(!c(subject, laterality, gender, age, height, weight, rerepetition)) %>% 
  # Filter out rest position
  filter(restimulus != 0) %>%
  # mutate(
  #   exercise.restimulus = paste0(exercise, '-', restimulus)
  # ) %>%
  # select(-exercise, -restimulus) %>% 
  # Pivot all angles
  pivot_longer(cols = !c(exercise, restimulus), names_to = "angles", values_to = 'values') %>% 
  group_by(exercise, restimulus) %>%
  db_compute_boxplot(x = angles, var = values, coef = 1.5) %>% collect()

# IQR already multiplied by 1.5. ymin, ymax = lower, upper +- IQR
dat_boxplot %>% View()

# Boxplot for exercise 1 only
dat_boxplot %>% 
  mutate(
    exercise = as.factor(exercise),
    restimulus = as.factor(restimulus)
  ) %>% 
  filter(exercise == 1) %>% 
  ggplot(aes(fill=angles)) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_errorbar(
    aes(
      x = restimulus,
      ymax = max_raw,
      ymin = min_raw,
      ),
    # colour = '#E69F00', size = 0.7, alpha = 0.4
    colour = 'slateblue4', size = 0.7, alpha = 0.4
  ) +
  geom_boxplot(
    aes(
      x = restimulus,
      ymin = ymin,
      lower = lower,
      middle = middle,
      upper = upper,
      ymax = ymax,
      group = restimulus
    ),
    stat = 'identity',
    width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) + 
  scale_y_continuous(
    name = expression('Angle [°]'),
    breaks = seq(-400, 400, 50),
    minor_breaks = seq(-400, 400, 25)
    # limits=c(0, NA)
  ) +
  scale_x_discrete(
    name = 'Movement',
    expand = c(0.055, 0.055)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_futurama(name = 'Joint') +
  ggtitle("Exercise 1 - Boxplot of angles and movements") +
  guides(
    fill = guide_legend(nrow = 1)
  ) +
  theme_bw() + theme(
    # panel.border = element_rect(),
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/boxplot_exercise_1_angles_movements_outliers.png',
  width = 30, height = 13, units = 'cm', dpi = 320, pointsize = 12)

# Boxplot for exercise 2 only
dat_boxplot %>% 
  mutate(
    exercise = as.factor(exercise),
    restimulus = as.factor(restimulus)
  ) %>% 
  filter(exercise == 2) %>% 
  ggplot(aes(fill=angles)) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_errorbar(
    aes(
      x = restimulus,
      ymax = max_raw,
      ymin = min_raw,
      ),
    # colour = '#E69F00', size = 0.7, alpha = 0.4
    colour = 'slateblue4', size = 0.7, alpha = 0.4
  ) +
  geom_boxplot(
    aes(
      x = restimulus,
      ymin = ymin,
      lower = lower,
      middle = middle,
      upper = upper,
      ymax = ymax,
      group = restimulus
    ),
    stat = 'identity',
    width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) + 
  scale_y_continuous(
    name = expression('Angle [°]'),
    breaks = seq(-400, 400, 50),
    minor_breaks = seq(-400, 400, 25)
    # limits=c(0, NA)
  ) +
  scale_x_discrete(
    name = 'Movement',
    expand = c(0.055, 0.055)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_futurama(name = 'Joint') +
  ggtitle("Exercise 2 - Boxplot of angles and movements") +
  guides(
    fill = guide_legend(nrow = 1)
  ) +
  theme_bw() + theme(
    # panel.border = element_rect(),
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/boxplot_exercise_2_angles_movements_outliers.png',
  width = 36, height = 13, units = 'cm', dpi = 320, pointsize = 12)

# Boxplot of IQR for exercise 1 only
dat_boxplot %>% 
  mutate(
    exercise = as.factor(exercise),
    restimulus = as.factor(restimulus)
  ) %>% 
  filter(exercise == 1) %>% 
  ggplot(aes(fill=angles)) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_boxplot(
    aes(
      x = restimulus,
      ymin = lower,
      lower = lower,
      middle = middle,
      upper = upper,
      ymax = upper,
      group = restimulus
    ),
    stat = 'identity',
    width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) + 
  scale_y_continuous(
    name = expression('Angle [°]'),
    breaks = seq(-400, 400, 50),
    minor_breaks = seq(-400, 400, 25)
    # limits=c(0, NA)
  ) +
  scale_x_discrete(
    name = 'Movement',
    expand = c(0.055, 0.055)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_futurama(name = 'Joint') +
  ggtitle("Exercise 1 - IQR of angles and movements") +
  guides(
    fill = guide_legend(nrow = 1)
  ) +
  theme_bw() + theme(
    # panel.border = element_rect(),
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/boxplot_exercise_1_angles_movements_iqr.png',
  width = 30, height = 13, units = 'cm', dpi = 320, pointsize = 12)

# Boxplot of IQR for exercise 2 only
dat_boxplot %>% 
  mutate(
    exercise = as.factor(exercise),
    restimulus = as.factor(restimulus)
  ) %>% 
  filter(exercise == 2) %>% 
  ggplot(aes(fill=angles)) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_boxplot(
    aes(
      x = restimulus,
      ymin = lower,
      lower = lower,
      middle = middle,
      upper = upper,
      ymax = upper,
      group = restimulus
    ),
    stat = 'identity',
    width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) + 
  scale_y_continuous(
    name = expression('Angle [°]'),
    breaks = seq(-400, 400, 50),
    minor_breaks = seq(-400, 400, 25)
    # limits=c(0, NA)
  ) +
  scale_x_discrete(
    name = 'Movement',
    expand = c(0.055, 0.055)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_futurama(name = 'Joint') +
  ggtitle("Exercise 2 - IQR of angles and movements") +
  guides(
    fill = guide_legend(nrow = 1)
  ) +
  theme_bw() + theme(
    # panel.border = element_rect(),
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/boxplot_exercise_2_angles_movements_iqr.png',
  width = 36, height = 13, units = 'cm', dpi = 320, pointsize = 12)

# Conclusions from outliers:
## MCP - lot of outliers outside anatomical range: -100° - 200°
## PIP - lot of outliers outside anatomical range: -150° - 300°
## DIP - lot of outliers outside anatomical range: -350° - 200°
# Conclusions from IQR:
## MCP - 50% of most values in range -25° - 100, MCP5_f - lot of values > 100°
## PIP - 50% of almost all values in range -25° - 135°
## DIP - median values for DIP3, DIP4 and DIP5 are mostly between 0° and -100°
## DIP - median values for DIP2 are mostly between 0° and 100°
## DIP3, DIP4 and DIP5 should maybe be negated before fitting


# Filter data to anatomical ranges, negate DIP3, DIP4 and DIP5 and save
## MCP_f -30° - 90°
## PIP -7.5° - 135°
## DIP -5° - 90°
dat_mcp_pip_dip %>% 
  # Filter out rest position
  filter(restimulus != 0) %>%
  # Negate DIP3, DIP4 and DIP5
  mutate(
    DIP3 = -DIP3,
    DIP4 = -DIP4,
    DIP5 = -DIP5
  ) %>% 
  # Filter to anatomical angles
  filter(
    between(MCP2_f, -30, 90) & between(MCP3_f, -30, 90) & 
      between(MCP4_f, -30, 90) & between(MCP5_f, -30, 90) &
      between(PIP2, -7.5, 135) & between(PIP3, -7.5, 135) &
      between(PIP4, -7.5, 135) & between(PIP5, -7.5, 135) &
      between(DIP2, -5, 90) & between(DIP3, -5, 90) &
      between(DIP4, -5, 90) & between(DIP5, -5, 90)
  ) %>% 
  spark_write_parquet(
    path = 'clean_data/s1-77_e1-2_distinct_mcp_pip_dip_filter.parquet',
    mode = 'overwrite'
    )

# Load only MCP, PIP and DIP filtered dataset in Spark
dat_mcp_pip_dip_filt <- spark_read_parquet(
  sc,
  name = 'dat_mcp_pip_dip_filt',
  path = 'clean_data/s1-77_e1-2_distinct_mcp_pip_dip_filter.parquet',
  memory = F,
  overwrite = T
  )

# Number of rows
dat_mcp_pip_dip_filt %>% sdf_nrow()
dat_mcp_pip_dip_filt %>% glimpse()

# Boxplot data for MCP, PIP, DIP filtered
dat_boxplot_filt <- dat_mcp_pip_dip_filt %>% 
  select(!c(subject, laterality, gender, age, height, weight, rerepetition)) %>% 
  pivot_longer(cols = !c(exercise, restimulus), names_to = "angles", values_to = 'values') %>% 
  group_by(exercise, restimulus) %>%
  db_compute_boxplot(x = angles, var = values, coef = 1.5) %>% collect()

# Boxplot for exercise 1 only filtered
dat_boxplot_filt %>% 
  mutate(
    exercise = as.factor(exercise),
    restimulus = as.factor(restimulus)
  ) %>% 
  filter(exercise == 1) %>% 
  ggplot(aes(fill=angles)) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_errorbar(
    aes(
      x = restimulus,
      ymax = max_raw,
      ymin = min_raw,
      ),
    colour = 'slateblue4', size = 0.7, alpha = 0.4
  ) +
  geom_boxplot(
    aes(
      x = restimulus,
      ymin = ymin,
      lower = lower,
      middle = middle,
      upper = upper,
      ymax = ymax,
      group = restimulus
    ),
    stat = 'identity',
    width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) + 
  scale_y_continuous(
    name = expression('Angle [°]'),
    breaks = seq(-100, 150, 20),
    minor_breaks = seq(-400, 400, 10)
  ) +
  scale_x_discrete(
    name = 'Movement',
    expand = c(0.055, 0.055)
    ) +
  scale_fill_futurama(name = 'Joint') +
  ggtitle("Exercise 1 - Boxplot of anatomical angles and movements") +
  guides(
    fill = guide_legend(nrow = 1)
  ) +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/boxplot_filtered_exercise_1_angles_movements.png',
  width = 30, height = 13, units = 'cm', dpi = 320, pointsize = 12)

# Boxplot for exercise 2 only
dat_boxplot_filt %>% 
  mutate(
    exercise = as.factor(exercise),
    restimulus = as.factor(restimulus)
  ) %>% 
  filter(exercise == 2) %>% 
  ggplot(aes(fill=angles)) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_errorbar(
    aes(
      x = restimulus,
      ymax = max_raw,
      ymin = min_raw,
      ),
    colour = 'slateblue4', size = 0.7, alpha = 0.4
  ) +
  geom_boxplot(
    aes(
      x = restimulus,
      ymin = ymin,
      lower = lower,
      middle = middle,
      upper = upper,
      ymax = ymax,
      group = restimulus
    ),
    stat = 'identity',
    width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) + 
  scale_y_continuous(
    name = expression('Angle [°]'),
    breaks = seq(-100, 150, 20),
    minor_breaks = seq(-400, 400, 10)
  ) +
  scale_x_discrete(
    name = 'Movement',
    expand = c(0.055, 0.055)
    ) +
  scale_fill_futurama(name = 'Joint') +
  ggtitle("Exercise 2 - Boxplot of anatomical angles and movements") +
  guides(
    fill = guide_legend(nrow = 1)
  ) +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/boxplot_filtered_exercise_2_angles_movements.png',
  width = 36, height = 13, units = 'cm', dpi = 320, pointsize = 12)

# Describe data
dat_mcp_pip_dip_filt %>% sdf_describe()
dat_mcp_pip_dip_filt %>% glimpse()

## Contingency table on exercise 2 (movement - subject)
dat_contingency <- dat_mcp_pip_dip_filt %>%
  filter(exercise == 2) %>% 
  group_by(restimulus, subject, rerepetition) %>% 
  # Filter out all groups with less than 100 observations per same repetition
  filter(n() >= 100) %>% ungroup() %>% 
  sdf_crosstab('restimulus', 'subject') %>% collect()
dat_contingency %>% 
  rename(
    movement = restimulus_subject
    ) %>% 
  pivot_longer(cols = -movement, names_to = 'subject', values_to = 'count') %>%
  mutate(
    movement = as.numeric(movement),
    subject = as.numeric(subject)
    ) %>%
  arrange(movement, subject) %>% 
  filter(count > 0) %>%
  mutate(
    movement = as.ordered(movement),
    subject = as.ordered(subject)
    ) %>%
  ggplot() +
  geom_count(
    aes(
      x = subject, y = movement, fill = count, size = count
      ),
    shape = 21, stroke = 0.2
    ) +
  guides(
    fill = guide_colorbar(title = 'Observations', nrow = 1),
    size = guide_legend(title = 'Observations', nrow = 1)
  ) +
  scale_x_discrete(name = 'Subject') +
  scale_y_discrete(name = 'Movement') +
  scale_size_continuous(
    range = c(0.5, 7),
    breaks = c(1e2, 1e3, 1e4, 2e4, 3e4, 4e4, 5e4, 6e4)
  ) +
  scale_fill_viridis_c(direction = 1) +
  ggtitle('Contingency table - observations - subject vs movement') +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/contingency_table_exercise_2_subject_movement.png',
  width = 36, height = 15, units = 'cm', dpi = 320, pointsize = 12)
# Conclusion from contingency table
## 35 - 40, 43, 51, 53, 58, 60, 61, 64 will dominate the fit
## sample 100 points from each subject, movement and repetition and perform fitting
dat_mcp_pip_dip_filt %>%
  filter(exercise == 2) %>% group_by(subject, restimulus, rerepetition) %>%
  filter(n() >= 100) %>%
  summarise(
    n = n()
  ) %>% collect() %>% group_by(subject, restimulus) %>%
  summarise(repetitions = n()) %>% View()

dat_mcp_pip_dip_filt %>%
  filter(exercise == 2) %>% 
  group_by(restimulus, subject, rerepetition) %>% 
  # Filter out all groups with less than 100 observations per same repetition
  filter(n() >= 100) %>% ungroup() %>%
  sdf_repartition(12, partition_by = c('subject', 'restimulus', 'rerepetition')) %>%
  filter(
    restimulus == 5 & subject %in% c(33, 34, 35)
    ) %>% 
  sdf_repartition(12, partition_by = c('subject', 'restimulus', 'rerepetition')) %>% 
  # group_by(subject, restimulus, rerepetition) %>% 
  # nest(data = MCP2_f:DIP5) %>% select(data) %>% 
  # do(ml_linear_regression(., .$DIP2 ~ .$MCP2_f)) %>% 
  # summarise(
  #   mod = list(ml_linear_regression(., DIP2 ~ MCP2_f))
  # )
  # summarise(rsq = summary(mod)$r.squared)
  # do(head(., 2))
  # ml_linear_regression(DIP2 ~ MCP2_f) %>% summary()
  spark_apply(
    function(e) summary(lm(DIP2 ~ MCP2_f, data = e))$r.squared,
    names = 'r_squared',
    memory = F,
    group_by = c('subject', 'restimulus', 'rerepetition')
  )

# Correlation matrix
corr_mat_f <- function(dat, exercise_filt = 2, min_obs = 100) {
  # Obtain entire correlation matrices for groups (restimulus, subject, rerepetition)
  dat_filt <- dat %>% 
    select(subject, exercise, restimulus, rerepetition, MCP2_f:DIP5) %>% 
    filter(exercise == exercise_filt) %>% 
    select(-exercise) %>% 
    group_by(restimulus, subject, rerepetition) %>% 
    # Filter out all groups with less than min_obs observations per same repetition
    filter(n() >= min_obs) %>% ungroup()
  # TODO: Add number of observations to use as weight factor later
  cart_prod <- dat_filt %>% 
    select(restimulus, subject, rerepetition) %>%
    sdf_distinct() %>%
    arrange(restimulus, subject, rerepetition) %>% 
    collect()
  num_groups <- cart_prod %>% nrow()
  cart_prod %>% as.list()
  groups_corr <- tibble(
    restimulus = integer(),
    subject = integer(),
    rerepetition = integer(),
    corrs = list()
  )
  for (i in 1:num_groups) {
  # for (i in 1:5) {
    # print(paste(cart_prod$restimulus[i], cart_prod$subject[i], cart_prod$rerepetition[i]))
    groups_filt <- dat_filt %>%
      filter(
        restimulus == !!cart_prod$restimulus[i] &
          subject == !!cart_prod$subject[i] &
          rerepetition == !!cart_prod$rerepetition[i]
          )
    groups_corr <- groups_corr %>% add_row(
      restimulus = cart_prod$restimulus[i],
      subject = cart_prod$subject[i],
      rerepetition = cart_prod$rerepetition[i],
      corrs = list(correlate(
        groups_filt %>% select(-restimulus, -subject, -rerepetition))
        )
    )
  }
  groups_corr
    # ml_corr(columns = c('MCP2_f', 'DIP2', 'PIP2'))
    # summarise(
    #   corr = ml_corr()
    #   )
}

corr_mat <- corr_mat_f(dat_mcp_pip_dip_filt)
corr_mat %>% glimpse()

# Plot correlation matrices
corr_mat_long <- corr_mat %>% 
  mutate(
    corrs = map(corrs, shave),
    corrs = map(corrs, stretch),
    ) %>% unnest(corrs) %>% 
  rename(corr = r)

# Number of distinct correlation coefficients (66 per repetition)
corr_mat_long %>% 
  drop_na() %>% 
  group_by(restimulus, subject, rerepetition) %>% 
  summarise(
    count = n()
  )

# Boxplot of correlation coefficients
## Group similar grasp types
## WEight coefficients for linear regression
corr_mat_long %>% 
  # Keep only correlation values >= 0.5
  # mutate(
  #   corr = case_when(
  #     abs(corr) >= 0.9 ~ corr,
  #     T ~ NA_real_
  #     )
  # ) %>%
  drop_na() %>%
  mutate(
    x_y = paste(x, y, sep = ' - '),
    subject_rerepetition = paste0(subject, rerepetition)
  ) %>%
  select(-subject, -rerepetition) %>% 
  filter(
    # !restimulus %in% c(2, 3, 10, 20),
    # !restimulus %in% c(8, 9, 17, 18, 23),
    # !restimulus %in% c(6, 14, 15, 16, 19),
    # !restimulus %in% c(5, 13, 21, 22)
    # restimulus %in% c(1, 7)
    # restimulus %in% c(4, 11, 12)
    restimulus %in% c(11, 12, 13, 18)
  ) %>% 
  group_by(restimulus, subject_rerepetition, x_y) %>%
  summarise(
    corr = median(corr)
  ) %>% 
  group_by(restimulus, x_y) %>% 
  mutate(
    corr_median_abs = abs(median(corr)),
    restimulus = paste('Movement:', restimulus)
  ) %>% 
  ungroup() %>% 
  # Keep only median correlation values >= 0.8 (very strong relationship)
  filter(
    corr_median_abs >= 0.8
  ) %>% 
  ggplot() +
  facet_wrap(. ~ restimulus, scales = 'fixed', nrow = 1) +
  geom_boxplot(
    aes(
      x = corr,
      y = x_y,
      fill = corr_median_abs,
      # group = subject_rerepetition,
      # color = subject_rerepetition
    ),
    width = 0.6, alpha = 1, outlier.size = 1, outlier.stroke = 0.1
  ) + 
  # scale_y_continuous(
    # name = expression('Angle [°]'),
    # breaks = seq(-400, 400, 50),
    # minor_breaks = seq(-400, 400, 25)
    # limits=c(0, NA)
  # ) +
  scale_x_continuous(
  name = 'Correlation',
  # expand = c(0.055, 0.055),
  breaks = seq(-1, 1, 0.25)
  ) +
  scale_fill_viridis_c(
    name = 'Absolute median correlation',
    option = 'viridis',
    begin = 0.4,
    limits = c(0.8, 1),
    breaks = seq(0, 1, 0.05)
    ) +
  ggtitle("Boxplot of correlations - Exercise 2") +
  guides(
    fill = guide_colorbar(
      nrow = 1,
      barheight = 0.5
      )
  ) +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    legend.text = element_text(color="black", size = 8),
    legend.title = element_text(color = 'black', size = 8, vjust = 1),
    axis.title.x = element_text(face="bold", size = 9),
    axis.title.y = element_blank(),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", size = 7),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  # filename = 'plots/correlation_boxplot_exercise_2_movement_2_3_10_20.png',
  # filename = 'plots/correlation_boxplot_exercise_2_movement_8_9_17_18_23.png',
  # filename = 'plots/correlation_boxplot_exercise_2_movement_6_14_15_16_19.png',
  # filename = 'plots/correlation_boxplot_exercise_2_movement_5_13_21_22.png',
  # filename = 'plots/correlation_boxplot_exercise_2_movement_1_7.png',
  filename = 'plots/correlation_boxplot_exercise_2_movement_4_11_12.png',
  # width = 13, height = 9, units = 'cm', dpi = 320, pointsize = 12)
  # width = 13, height = 11, units = 'cm', dpi = 320, pointsize = 12)
  width = 19, height = 11, units = 'cm', dpi = 320, pointsize = 12)
  # width = 25, height = 15, units = 'cm', dpi = 320, pointsize = 12)

corr_mat_long %>% 
  # Keep only correlation values >= 0.5
  # mutate(
  #   corr = case_when(
  #     abs(corr) >= 0.9 ~ corr,
  #     T ~ NA_real_
  #     )
  # ) %>%
  drop_na() %>%
  mutate(
    x_y = paste(x, y, sep = ' - '),
    subject_rerepetition = paste0(subject, rerepetition)
  ) %>%
  select(-subject, -rerepetition) %>% 
  filter(
    restimulus == 1 & str_detect(x, 'MCP')
  ) %>% 
  group_by(restimulus, subject_rerepetition, x_y) %>%
  summarise(
    corr = median(corr)
  ) %>% ungroup() %>% 
  group_by(restimulus, x_y) %>%
  summarise(
    corr = median(corr)
  ) %>% ungroup() %>%
  mutate(
    corr_sign = case_when(
      corr > 0 ~ 'Positive',
      corr < 0 ~ 'Negative',
      T ~ NA_character_
    )
  ) %>% 
  ggplot() + 
  facet_wrap(. ~ corr_sign, scales = 'free_x', nrow = 1) +
  geom_point(
    aes(
      x = corr, y = x_y,
      # size = abs(corr),
      fill = abs(corr)
      # group = subject
      ),
    shape = 21, stroke = 0.2
    ) +
   guides(
    # fill = guide_colorbar(title = 'Observations', nrow = 1),
    # size = guide_legend(title = 'Observations', nrow = 1)
  ) +
  scale_x_continuous(
    name = 'Correlation'
  ) +
  scale_y_discrete(name = NA) +
  # scale_size_continuous(
  #   range = c(0.5, 7),
  #   breaks = seq(0.5, 1, 0.1)
  # ) +
  scale_fill_viridis_c(direction = 1) +
  ggtitle('Correlation matrix') +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    panel.background = element_blank(),
    panel.spacing = unit(2, 'mm'),
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    # axis.title = element_text(face="bold", size = 10),
    # axis.title.y = element_blank(),
    axis.text.x = element_text(colour="black", size = 8),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = 'plots/correlation_matrix_exercise_2_subject_movement.png',
  width = 36, height = 15, units = 'cm', dpi = 320, pointsize = 12)

# Linear regression on data
test_reg <- dat_mcp_pip_dip_filt %>% 
  select(subject, exercise, restimulus, rerepetition, MCP2_f:DIP5) %>% 
  filter(exercise == 2) %>% 
  select(-exercise) %>% 
  group_by(restimulus, subject, rerepetition) %>% 
  # Filter out all groups with less than min_obs observations per same repetition
  filter(n() >= 100) %>% ungroup() %>% 
  filter(
    restimulus == 17
    ) %>%
  group_by(subject, rerepetition) %>% collect()

test_reg %>% group_by(subject, rerepetition) %>% 
  nest(data = MCP2_f:DIP5) %>% 
  mutate(
    fit = map(data, ~lm(DIP2 ~ MCP3_f+0, data = .)),
    glanced = map(fit, glance),
    tidied = map(fit, tidy)
  ) %>% unnest(glanced) %>% select(-statistic, -p.value) %>% 
  unnest(tidied) %>% select(
    subject, restimulus, rerepetition, nobs, r.squared, adj.r.squared, term, estimate, p.value
  ) %>% ungroup() %>%
  filter(adj.r.squared >= 0.8) %>%
  summarise(
    coeff = weighted.mean(estimate)
  )
  
# Disconnect from Spark
spark_disconnect(sc)
spark_disconnect_all()

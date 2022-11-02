# Launch Apache Spark locally and analyze intra-finger joint dependancies

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
# Generalized linear models and visualisation in ggplot
library(glmnet)
library(ggfortify)
# Linear mixed-effects models
library(nlme)
library(lme4)
library(lmerTest)
# Diagnostic panel plot of residuals
library(ggResidpanel)
# Ramer-Douglas-Peucker Algorithm reducing the number of points on a 2D curve
library(RDP)


# Install Apache Spark
# spark_install()
# spark_installed_versions()

# Configuration for local use
## Initialize configuration with defaults
local_config <- spark_config()
## Memory
# local_config$`sparklyr.shell.driver-memory` <- "10G"
local_config$`sparklyr.shell.driver-memory` <- "18G"
## Memory fraction (default 60 %)
# local_config$`spark.memory.fraction` <- 0.3
local_config$`spark.memory.fraction` <- 0.4
## Cores
# local_config$`sparklyr.cores.local` <- 8
local_config$`sparklyr.cores.local` <- 12

# Connect to Spark locally (use version 3.0+ with Java 11)
sc <- spark_connect(master = 'local', version = '3.1', config = local_config)

# Launch web interface
spark_web(sc)

# # Call SQL inside Spark
# sqlfunction <- function(sc, block) {
#   spark_session(sc) %>% invoke("sql", block)
# }
# 
# # Load only distinct database
# dat_dist <- spark_read_parquet(
#   sc,
#   name = 'dat_dist',
#   path = 'clean_data/s1-77_e1-2_distinct.parquet',
#   memory = F,
#   overwrite = T
#   )
# 
# dat_dist %>% glimpse()
# 
# # Extract only CMC, MCP, PIP and DIP and IP flexion joint angles for fingers 1-5
# # and exercise 2. Save dataset.
# dat_dist %>%
#   select(
#     subject, laterality, gender, age, height, weight, exercise, restimulus, rerepetition,
#     CMC1_f, MCP1, IP1, MCP2_f, PIP2, DIP2, MCP3_f, PIP3, DIP3, MCP4_f, PIP4, DIP4,
#     CMC5, MCP5_f, PIP5, DIP5
#     ) %>% 
#   filter(exercise == 2) %>% 
#   spark_write_parquet(
#     path = 'clean_data/s1-77_e2_distinct_flex.parquet',
#     mode = 'overwrite'
#     )

# Load only 1-5 finger flexions/extensions dataset in Spark
dat_flex <- spark_read_parquet(
  sc,
  name = 'dat_flex',
  path = 'clean_data/s1-77_e2_distinct_flex.parquet',
  memory = F,
  overwrite = T
  )
dat_flex %>% glimpse()
dat_flex %>% sdf_nrow()


# Boxplot of IQR - several angles, one motion (restimulus) and 
# subjects (rerepetitions grouped)
## IQR already multiplied by 1.5. ymin, ymax = lower, upper +- IQR
## Takes ~40 min to generate 
dat_boxplot <- dat_flex %>%
  # Filter out rest position
  filter(restimulus != 0) %>%
  select(!c(laterality, gender, age, height, weight, exercise, rerepetition)) %>% 
  pivot_longer(
    cols = !c(subject, restimulus), names_to = "angles", values_to = 'values'
    ) %>% 
  group_by(subject, restimulus) %>% 
  # group_by(exercise, restimulus) %>%
  db_compute_boxplot(x = angles, var = values, coef = 1.5) %>% collect()
  
## Function for automatic boxplot figure save
save_iqr_boxplot <- function(
    dat_boxplot, # Collected db_compute_boxplot
    n_min = 100, # Minimum number of observations in group
    iqr_share_min = 0.5, # Minimum share of iqr/iqr_mean per motion
    restimulus_filt = 1, # Motion/restimulus
    angles_filt = c('CMC1_f', 'MCP1', 'IP1') # angles for plot (always vector)
    ){
  iqr_boxplot <- dat_boxplot %>%
    group_by(restimulus, angles) %>% 
    mutate(
      iqr = iqr / 1.5,
      iqr_mean = mean(iqr),
      iqr_share = iqr / iqr_mean,
      median_of_medians = median(middle)
    ) %>% 
    ungroup() %>% 
    # Filter out too small samples 
    # and ones with <50% of motion IQR (too small part of entire motion captured)
    filter(
      n >= n_min,
      iqr_share >= iqr_share_min
    ) %>% 
    filter(restimulus == restimulus_filt & angles %in% angles_filt) %>%
    arrange(restimulus, subject) %>% 
    mutate(
      restimulus = as.ordered(restimulus),
      subject = as.ordered(subject)
    ) %>% 
    ggplot() +
    facet_grid(angles ~ . , scales = 'free_y') +
    # Zero line for easier outlier detection
    geom_hline(
      yintercept = 0,
      color = 'black',
      lty = 'dotdash'
    ) +
    # Median of medians line for easier outlier detection
    geom_hline(
      aes(
        yintercept = median_of_medians
        ),
      color = 'orangered3',
      lty = 'solid'
    ) +
    geom_boxplot(
      aes(
        x = subject,
        ymin = ymin,
        lower = lower,
        middle = middle,
        upper = upper,
        ymax = ymax,
        group = subject,
        fill = subject
      ),
      stat = 'identity',
      width = 0.5, alpha = 1, outlier.size = 1, outlier.stroke = 0.1) +
    scale_y_continuous(
      name = expression('Angle [°]'),
      breaks = seq(-400, 400, 25),
      minor_breaks = seq(-400, 400, 12.5)
      # limits=c(0, NA)
    ) +
    scale_x_discrete(
      name = 'Subject',
      # expand = c(0.055, 0.055)
      # breaks = seq(0, 30, 1)
      ) +
    scale_fill_viridis_d(
      name = 'Subject',
      begin = 0.3
      ) +
    ggtitle(
      paste(
        'Motion', restimulus_filt, '-', 'IQR per subject and angles', 
        paste(angles_filt, collapse = ', ')
        )
      ) +
    theme_bw() + theme(
      legend.position = 'none',
      panel.background = element_blank(),
      panel.spacing = unit(1, 'mm'),
      axis.title = element_text(face="bold", size = 10),
      axis.text.x = element_text(colour="black", size = 7),
      axis.text.y = element_text(colour="black", size = 8),
      axis.line = element_line(size=0.5, colour = "black"),
      plot.title = element_text(hjust = 0.5),
      panel.grid = element_line(colour = 'grey', size = 0.2)
    )
  ggsave(
    filename = paste0(
      'plots/boxplot_IQR_e2_movement_', restimulus_filt, '_',
      paste(angles_filt, collapse = '_'), '.png'
      ),
    plot = iqr_boxplot,
    width = 30, height = 19, units = 'cm', dpi = 320, pointsize = 12)
}

## Loop and save
for (restimulus in 1:23) {
  for (angles in list(
    c('CMC1_f', 'MCP1', 'IP1'),
    c('MCP2_f', 'PIP2', 'DIP2'),
    c('MCP3_f', 'PIP3', 'DIP3'),
    c('MCP4_f', 'PIP4', 'DIP4'),
    c('CMC5', 'MCP5_f', 'PIP5', 'DIP5')
    )) {
    # print(paste(movement, angles))
    save_iqr_boxplot(
      dat_boxplot = dat_boxplot,
      restimulus_filt = restimulus,
      angles_filt = angles
    )
  }
}

# Filtered boxplot data
## Group by subject and restimulus (motion)
dat_boxplot <- dat_boxplot %>% 
  group_by(restimulus, angles) %>% 
  mutate(
    # Calculated iqr already multiplied by 1.5
    iqr = iqr / 1.5,
    iqr_mean = mean(iqr),
    iqr_share = iqr / iqr_mean,
    median_of_medians = median(middle)
  ) %>% 
  ungroup() %>% 
  # Filter out too small samples (use NA values)
  # and ones with <50% of motion IQR (too small part of entire motion captured)
  mutate(
    n_small = case_when(
      n < 100 ~ NA_real_,
      T ~ n
    ),
    range_small = case_when(
      iqr_share < 0.5 ~ NA_real_,
      T ~ iqr_share
    )
  ) %>% 
  arrange(restimulus, angles, subject)
dat_boxplot %>% View()

## Export plot for median of medians
dat_boxplot %>% 
  select(restimulus, angles, subject, middle, median_of_medians) %>% 
  group_by(restimulus, angles) %>% 
  distinct(median_of_medians) %>% 
  ungroup() %>% 
  mutate(
    restimulus = as.ordered(restimulus)
  ) %>% 
  ggplot(
    aes(
      x = restimulus,
      y = median_of_medians
    )
  ) +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_rug(
    size = 0.8, outside = F, sides = 'l'
  ) +
  geom_col(
    aes(
      fill = restimulus
      ),
    width = 0.7
  ) +
  # Zero line for easier outlier detection
  geom_hline(
    yintercept = 0,
    color = 'black',
    lty = 'dotdash'
  ) +
  scale_y_continuous(
      name = expression('Median of medians - Angle [°]'),
      # breaks = seq(-400, 400, 25),
      # minor_breaks = seq(-400, 400, 12.5)
      # limits=c(0, NA)
    ) +
  scale_x_discrete(
    name = 'Motion',
    expand = c(0.0, 1.6, 0.0, 0.2)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_viridis_d(
    name = 'Motion',
    end = 0.8
    ) +
  scale_color_viridis_d(
    name = 'Motion',
    end = 0.8
    ) +
  ggtitle('Median of medians - motion vs angle') +
  theme_bw() + theme(
    legend.position = 'none',
    legend.box = 'horizontal',
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    panel.background = element_blank(),
    panel.spacing = unit(1, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
  ggsave(
    filename = 'plots/median_of_medians_motion_angle.png',
    width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
  

# Boxplot of medians for outlier detection - before filtering
dat_boxplot %>% 
  mutate(
    restimulus = as.ordered(restimulus)
  ) %>% 
  ggplot() +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_boxplot(
    aes(
      x = restimulus,
      y = middle,
      fill = restimulus
      )
  ) +
  # Zero line for easier outlier detection
  geom_hline(
    yintercept = 0,
    color = 'black',
    lty = 'dotdash'
  ) +
  scale_y_continuous(
      name = expression('Boxplot of medians - Angle [°]'),
      # breaks = seq(-400, 400, 25),
      # minor_breaks = seq(-400, 400, 12.5)
      # limits=c(0, NA)
    ) +
  scale_x_discrete(
    name = 'Motion',
    # expand = c(0.0, 1.6, 0.0, 0.2)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_viridis_d(
    name = 'Motion',
    begin = 0.3,
    end = 0.9
    ) +
  scale_color_viridis_d(
    name = 'Motion',
    end = 0.8
    ) +
  ggtitle('Boxplot of medians - motion vs angle - before filtering') +
  theme_bw() + theme(
    legend.position = 'none',
    legend.box = 'horizontal',
    legend.box.spacing = unit(2, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    panel.background = element_blank(),
    panel.spacing = unit(1, 'mm'),
    axis.title = element_text(face="bold", size = 10),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", size = 8),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
  ggsave(
    filename = 'plots/boxplot_of_medians_motion_angle_no_filt.png',
    width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)

# Boxplot of medians for outlier detection - before filtering (MDPI template)
dat_boxplot %>% 
  mutate(
    restimulus = as.ordered(restimulus)
  ) %>% 
  # filter(angles %in% c('DIP3')) %>%
  # mutate(angles = 'DIP3 - raw') %>% 
  filter(angles %in% c('MCP2_f')) %>%
  mutate(angles = 'MCP2_f - raw') %>%
  ggplot() +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_boxplot(
    aes(
      x = restimulus,
      y = middle,
      fill = restimulus
      ),
    outlier.size = 0.1,
    size = 0.1
  ) +
  # Zero line for easier outlier detection
  geom_hline(
    yintercept = 0,
    color = 'black',
    lty = 'dotdash',
    size = 0.1
  ) +
  geom_hline(
    yintercept = -30,
    color = 'black',
    lty = 'longdash',
    size = 0.1
  ) +
  geom_hline(
    yintercept = 90,
    color = 'black',
    lty = 'longdash',
    size = 0.1
  ) +
  scale_y_continuous(
      # name = expression('Median joint angle [°]'),
      name = '',
      breaks = seq(-400, 400, 25),
      # minor_breaks = seq(-400, 400, 12.5)
      # limits=c(0, NA)
    ) +
  scale_x_discrete(
    name = 'Motion',
    # expand = c(0.0, 1.6, 0.0, 0.2)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_viridis_d(
    name = 'Motion',
    begin = 0.3,
    end = 0.9
    ) +
  scale_color_viridis_d(
    name = 'Motion',
    end = 0.8
    ) +
  # ggtitle('Boxplot of angle medians - 2 samples - before filtering') +
  theme_bw() + theme(
    legend.position = 'none',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    panel.background = element_blank(),
    panel.spacing = unit(0, 'mm'),
    axis.title = element_text(face="bold", size = 7),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", size = 7),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    strip.text.x = element_text(size = 7, colour = "black"),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.1),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
  )
  # ggsave(
  #   filename = 'plots/boxplot_of_medians_motion_angle_no_filt_dip3.png',
  #   width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)
  ggsave(
    filename = 'plots/boxplot_of_medians_motion_angle_no_filt_mcp2_f.png',
    width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)

# Filter data to anatomical ranges.
# CMC5, DIP3, DIP4 and DIP5 should also be negated 
# (negative instead of positive sign for flexion).
## Thumb (finger 1):
### CMC1_f (TMC) (-15° - 50°)
### MCP1 (-40° - 45°)
### IP1 (-5° - 75°)
## Fingers 2 - 5 (except PIP)
### MCP_f (-30° - 90°)
### PIP (-5° - 120°)
### DIP (-5° - 90°)
## Finger 5
### CMC5 (0° - 15°)
### PIP5 (-5° - 135°)

# Replace values outside anatomical ROM with NA for later filtering
# If some angles are measured inaccurately, not entire observation should be removed
dat_flex_anat <- dat_flex %>% 
  # Filter out rest position
  filter(restimulus != 0) %>%
  # Negate CMC5, DIP3, DIP4 and DIP5
  mutate(
    CMC5 = -CMC5,
    DIP3 = -DIP3,
    DIP4 = -DIP4,
    DIP5 = -DIP5
  ) %>% 
  # Replace all values outside anatomical angles with NA.
  mutate(
    CMC1_f = case_when(
      between(CMC1_f, -15, 50) ~ CMC1_f,
      T ~ NA_real_
    ),
    MCP1 = case_when(
      between(MCP1, -40, 45) ~ MCP1,
      T ~ NA_real_
    ),
    IP1 = case_when(
      between(IP1, -5, 75) ~ IP1,
      T ~ NA_real_
    ),
    MCP2_f = case_when(
      between(MCP2_f, -30, 90) ~ MCP2_f,
      T ~ NA_real_
    ),
    PIP2 = case_when(
      between(PIP2, -5, 120) ~ PIP2,
      T ~ NA_real_
    ),
    DIP2 = case_when(
      between(DIP2, -5, 90) ~ DIP2,
      T ~ NA_real_
    ),
    MCP3_f = case_when(
      between(MCP3_f, -30, 90) ~ MCP3_f,
      T ~ NA_real_
    ),
    PIP3 = case_when(
      between(PIP3, -5, 120) ~ PIP3,
      T ~ NA_real_
    ),
    DIP3 = case_when(
      between(DIP3, -5, 90) ~ DIP3,
      T ~ NA_real_
    ),
    MCP4_f = case_when(
      between(MCP4_f, -30, 90) ~ MCP4_f,
      T ~ NA_real_
    ),
    PIP4 = case_when(
      between(PIP4, -5, 120) ~ PIP4,
      T ~ NA_real_
    ),
    DIP4 = case_when(
      between(DIP4, -5, 90) ~ DIP4,
      T ~ NA_real_
    ),
    CMC5 = case_when(
      between(CMC5, 0, 15) ~ CMC5,
      T ~ NA_real_
    ),
    MCP5_f = case_when(
      between(MCP5_f, -30, 90) ~ MCP5_f,
      T ~ NA_real_
    ),
    PIP5 = case_when(
      between(PIP5, -5, 135) ~ PIP5,
      T ~ NA_real_
    ),
    DIP5 = case_when(
      between(DIP5, -5, 90) ~ DIP5,
      T ~ NA_real_
    )
  ) %>% sdf_register('dat_flex_anat')

# Save dataset filtered to anatomical ROM
dat_flex_anat %>%
  spark_write_parquet(
    path = 'clean_data/s1-77_e2_distinct_flex_anatomical.parquet',
    mode = 'overwrite'
    )

# Load dataset filtered to anatomical ROM
dat_flex_anat <- spark_read_parquet(
  sc,
  name = 'dat_flex_anat',
  path = 'clean_data/s1-77_e2_distinct_flex_anatomical.parquet',
  memory = F,
  overwrite = T
  )
dat_flex_anat %>% glimpse()
dat_flex_anat %>% sdf_nrow()

# Boxplot of IQR - several angles, one motion (restimulus) and 
# subjects (rerepetitions grouped)
## IQR already multiplied by 1.5. ymin, ymax = lower, upper +- IQR
## Takes several minutes to generate 
dat_boxplot_anat <- dat_flex_anat %>%
  # Filter out rest position
  filter(restimulus != 0) %>%
  select(!c(laterality, gender, age, height, weight, exercise, rerepetition)) %>% 
  pivot_longer(
    cols = !c(subject, restimulus), names_to = "angles", values_to = 'values'
    ) %>% 
  # Drop all NA values
  filter(!is.na(values)) %>% 
  group_by(subject, restimulus) %>% 
  # group_by(exercise, restimulus) %>%
  db_compute_boxplot(x = angles, var = values, coef = 1.5) %>% collect()

# # Fix IQR calculation and filter out slamm samples with too small ROM
# dat_boxplot_anat <- dat_boxplot_anat %>% 
#   group_by(restimulus, angles) %>% 
#   mutate(
#     iqr = iqr / 1.5,
#     iqr_mean = mean(iqr),
#     iqr_share = iqr / iqr_mean,
#     median_of_medians = median(middle)
#   ) %>% 
#   ungroup() %>% 
#   # Filter out too small samples (use NA values)
#   # and ones with <50% of motion IQR (too small part of entire motion captured)
#   mutate(
#     n_small = case_when(
#       n < 100 ~ NA_real_,
#       T ~ n
#     ),
#     range_small = case_when(
#       iqr_share < 0.5 ~ NA_real_,
#       T ~ iqr_share
#     )
#   ) %>% 
#   arrange(restimulus, angles, subject)

## Filtered boxplot data with new iqr and median calculations on data
## Group by subject and restimulus (motion)
dat_boxplot_anat_range_small <- dat_boxplot_anat %>% 
  drop_na() %>% 
  arrange(restimulus, angles, subject) %>% 
  # Recalculate IQR mean and median of medians without filtered data
  group_by(restimulus, angles) %>% 
  mutate(
    iqr_mean = mean(iqr),
    iqr_share = iqr / iqr_mean,
    median_of_medians = median(middle)
  ) %>% ungroup() %>% 
  arrange(restimulus, angles, subject)

## Filter using 1.5 IQR rule on anatomical data without small samples and range
## Create pipeline for filtering
iqr_filter_pipeline <- . %>% 
  group_by(restimulus, angles) %>%
  # Calculate quantiles and IQR for medians (all subjects grouped per motion and angle)
  mutate(
    q1_group = quantile(middle, 0.25),
    q3_group = quantile(middle, 0.75),
    iqr_group = IQR(middle),
    lbound = q1_group - 1.5 * iqr_group,
    ubound = q3_group + 1.5 * iqr_group,
  ) %>% 
  ungroup() %>% 
  mutate(
    median_filt = case_when(
      middle <= lbound ~ NA_real_,
      middle >= ubound ~ NA_real_,
      T ~ middle
    )
  ) %>% 
  drop_na() %>% 
  group_by(restimulus, angles) %>% 
  mutate(
    iqr_mean = mean(iqr),
    iqr_share = iqr / iqr_mean,
    median_of_medians = median(middle)
  ) %>% ungroup() %>% 
  arrange(restimulus, angles, subject)

## Filter dat with 1.5 IQR rule iteratively 6 times
dat_boxplot_anat_range_small_iqr_filt <- 
  iqr_filter_pipeline(
    iqr_filter_pipeline(
      iqr_filter_pipeline(
        iqr_filter_pipeline(
          iqr_filter_pipeline(
            iqr_filter_pipeline(
              dat_boxplot_anat_range_small
              )
            )
          )
        )
      )
    )

# Function for plotting boxplot of medians in faceted motion vs angle diagram
boxplot_motion_angle <- function(
    boxplot_data, 
    title = 'Boxplot of medians - motion vs angle - anatomical ROM') {
  boxplot_data %>% 
    mutate(
      restimulus = as.ordered(restimulus)
    ) %>% 
    ggplot() +
    facet_wrap(. ~ angles, scales = 'free_y') +
    # Zero line for easier outlier detection
    geom_hline(
      yintercept = 0,
      color = 'black',
      lty = 'dotdash'
    ) +
    geom_boxplot(
      aes(
        x = restimulus,
        y = middle,
        fill = restimulus
        )
    ) +
    scale_y_continuous(
        name = expression('Boxplot of medians - Angle [°]'),
        # breaks = seq(-400, 400, 25),
        # minor_breaks = seq(-400, 400, 12.5)
        # limits=c(0, NA)
      ) +
    scale_x_discrete(
      name = 'Motion',
      # expand = c(0.0, 1.6, 0.0, 0.2)
      # breaks = seq(0, 30, 1)
      ) +
    scale_fill_viridis_d(
      name = 'Motion',
      begin = 0.3,
      end = 0.9
      ) +
    scale_color_viridis_d(
      name = 'Motion',
      end = 0.8
      ) +
    ggtitle(title) +
    theme_bw() + theme(
      legend.position = 'none',
      legend.box = 'horizontal',
      legend.box.spacing = unit(2, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(1, 'mm'),
      axis.title = element_text(face="bold", size = 10),
      axis.text.x = element_text(colour="black", size = 7),
      axis.text.y = element_text(colour="black", size = 8),
      axis.line = element_line(size=0.5, colour = "black"),
      plot.title = element_text(hjust = 0.5),
      panel.grid = element_line(colour = 'grey', size = 0.2)
    )
}

# Function for plotting medians of medians in faceted motion vs angle diagram
median_plot_motion_angle <- function(
    boxplot_data,
    title = 'Median of medians (anatomical ROM) - motion vs angle') {
  boxplot_data %>% 
    select(restimulus, angles, subject, middle, median_of_medians) %>% 
    group_by(restimulus, angles) %>% 
    distinct(median_of_medians) %>% 
    ungroup() %>% 
    mutate(
      restimulus = as.ordered(restimulus)
    ) %>% 
    ggplot(
      aes(
        x = restimulus,
        y = median_of_medians
      )
    ) +
    facet_wrap(. ~ angles, scales = 'free_y') +
    geom_rug(
      size = 0.8, outside = F, sides = 'l'
    ) +
    geom_col(
      aes(
        fill = restimulus
        ),
      width = 0.7
    ) +
    # Zero line for easier outlier detection
    geom_hline(
      yintercept = 0,
      color = 'black',
      lty = 'dotdash'
    ) +
    scale_y_continuous(
        name = expression('Median of medians - Angle [°]'),
        # breaks = seq(-400, 400, 25),
        # minor_breaks = seq(-400, 400, 12.5)
        # limits=c(0, NA)
      ) +
    scale_x_discrete(
      name = 'Motion',
      expand = c(0.0, 1.6, 0.0, 0.2)
      # breaks = seq(0, 30, 1)
      ) +
    scale_fill_viridis_d(
      name = 'Motion',
      end = 0.8
      ) +
    scale_color_viridis_d(
      name = 'Motion',
      end = 0.8
      ) +
    ggtitle(title) +
    theme_bw() + theme(
      legend.position = 'none',
      legend.box = 'horizontal',
      legend.box.spacing = unit(2, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(1, 'mm'),
      axis.title = element_text(face="bold", size = 10),
      axis.text.x = element_text(colour="black", size = 7),
      axis.text.y = element_text(colour="black", size = 8),
      axis.line = element_line(size=0.5, colour = "black"),
      plot.title = element_text(hjust = 0.5),
      panel.grid = element_line(colour = 'grey', size = 0.2)
    )
}

## Plot for median of medians after filtering to anatomical angles
median_plot_motion_angle(
  dat_boxplot_anat, 
  title = 'Median of medians (anatomical ROM) - motion vs angle'
  )
ggsave(
  filename = 'plots/median_of_medians_anatomical_motion_angle.png', 
  width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
## Plot for median of medians after filtering to anatomical angles and removal of
## small samples (n > 100, IQR share < 0.5)
median_plot_motion_angle(
  dat_boxplot_anat_range_small, 
  title = 'Median of medians (anatomical ROM, small samples removed) - motion vs angle'
  )
ggsave(
  filename = 'plots/median_of_medians_anatomical_small_range_motion_angle.png', 
  width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
## Plot for median of medians after filtering to anatomical angles, removal of
## small samples (n > 100, IQR share < 0.5) and filtering with 1.5 IQR rule
median_plot_motion_angle(
  dat_boxplot_anat_range_small_iqr_filt, 
  title = 'Median of medians (anatomical ROM, small samples removed, 1.5 IQR filter) - motion vs angle'
  )
ggsave(
  filename = 'plots/median_of_medians_anatomical_small_range_irq_filt_motion_angle.png', 
  width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
  

## Boxplot of medians for outlier detection after filtering to anatomical angles
## and removal of small samples (n > 100, IQR share < 0.5)
## MDPI template
# dat_boxplot_anat_range_small %>% 
## Filtering with 1.5 IQR rule
dat_boxplot_anat_range_small_iqr_filt %>% 
  mutate(
    restimulus = as.ordered(restimulus)
  ) %>% 
  # filter(angles %in% c('DIP3')) %>%
  # mutate(angles = 'DIP3 - anatomical, no small samples') %>%
  # mutate(angles = 'DIP3 - 1.5 IQR filtered') %>%
  filter(angles %in% c('MCP2_f')) %>%
  # mutate(angles = 'MCP2_f - anatomical, no small samples') %>%
  mutate(angles = 'MCP2_f - 1.5 IQR filtered') %>%
  ggplot() +
  facet_wrap(. ~ angles, scales = 'free_y') +
  geom_boxplot(
    aes(
      x = restimulus,
      y = middle,
      fill = restimulus
      ),
    outlier.size = 0.1,
    size = 0.1
  ) +
  # Zero line for easier outlier detection
  geom_hline(
    yintercept = 0,
    color = 'black',
    lty = 'dotdash',
    size = 0.1
  ) +
  geom_hline(
    yintercept = -30,
    # yintercept = -30,
    color = 'black',
    lty = 'longdash',
    size = 0.1
  ) +
  geom_hline(
    yintercept = 90,
    color = 'black',
    lty = 'longdash',
    size = 0.1
  ) +
  scale_y_continuous(
      # name = expression('Median joint angle [°]'),
      name = '',
      breaks = seq(-400, 400, 20),
      # minor_breaks = seq(-400, 400, 12.5)
      # limits=c(0, NA)
    ) +
  scale_x_discrete(
    name = 'Motion',
    # expand = c(0.0, 1.6, 0.0, 0.2)
    # breaks = seq(0, 30, 1)
    ) +
  scale_fill_viridis_d(
    name = 'Motion',
    begin = 0.3,
    end = 0.9
    ) +
  scale_color_viridis_d(
    name = 'Motion',
    end = 0.8
    ) +
  # ggtitle('Boxplot of angle medians - 2 samples - before filtering') +
  theme_bw() + theme(
    legend.position = 'none',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    panel.background = element_blank(),
    panel.spacing = unit(0, 'mm'),
    axis.title = element_text(face="bold", size = 7),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", size = 7),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    strip.text.x = element_text(size = 7, colour = "black"),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.1),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
  )
  # ggsave(
  #   filename = 'plots/boxplot_of_medians_motion_angle_anatomical_small_range_dip3.png',
  #   width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)
  # ggsave(
  #   filename = 'plots/boxplot_of_medians_motion_angle_anatomical_small_range_iqr_filt_dip3.png',
  #   width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)
  # ggsave(
  #   filename = 'plots/boxplot_of_medians_motion_angle_anatomical_small_range_mcp2_f.png',
  #   width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)
  ggsave(
    filename = 'plots/boxplot_of_medians_motion_angle_anatomical_small_range_iqr_filt_mcp2_f.png',
    width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)
  
## Boxplot of medians for outlier detection - after filtering to anatomical angles
boxplot_motion_angle(
  dat_boxplot_anat,
  title = 'Boxplot of medians - motion vs angle - anatomical ROM'
)
ggsave(
  filename = 'plots/boxplot_of_medians_motion_angle_anatomical.png',
  width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
## Boxplot of medians for outlier detection after filtering to anatomical angles
## and removal of small samples (n > 100, IQR share < 0.5)
boxplot_motion_angle(
  dat_boxplot_anat_range_small,
  title = 'Boxplot of medians - motion vs angle - anatomical ROM, small samples removed'
)
ggsave(
  filename = 'plots/boxplot_of_medians_motion_angle_anatomical_small_range.png',
  width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
## Boxplot of medians for outlier detection after filtering to anatomical angles,
## removal of small samples (n > 100, IQR share < 0.5) and filtering with 1.5 IQR rule
boxplot_motion_angle(
  dat_boxplot_anat_range_small_iqr_filt,
  title = 'Boxplot of medians - motion vs angle - anatomical ROM, small samples removed, 1.5 IQR filter'
)
ggsave(
  filename = 'plots/boxplot_of_medians_motion_angle_anatomical_small_range_iqr_filt.png',
  width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)

## Extract remaining data after all filtering for selection over entire dataset
## filtered_selection[[finger_index]] to extract list of motion-subject combinations
filtered_selection <- list()
for (finger in 1:5) {
  filtered_selection[[finger]] <- 
    dat_boxplot_anat_range_small_iqr_filt %>% 
      select(restimulus, subject, angles, n) %>% 
      mutate(
        angles = case_when(
          str_detect(angles, as.character(finger)) ~ angles,
          T ~ NA_character_
        )
      ) %>% 
      drop_na() %>% 
      pivot_wider(names_from = angles, values_from = n) %>%
      drop_na() %>% 
      mutate(
        restimulus_subject = paste(restimulus, subject, sep = '-')
      ) %>% 
      pull(restimulus_subject)
}

# Contingency tables for all fingers
contingency_tables <- list()
for (finger in 1:5) {
  contingency_tables[[finger]] <- dat_flex_anat %>% 
    select(restimulus, subject, contains(as.character(!!finger))) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Create column with restimulus - subject combination for filtering
    mutate(
      restimulus_subject = paste(restimulus, subject, sep = '-')
    ) %>%
    # Filter entire dataset using filtered_selection
    filter(
      restimulus_subject %in% !!filtered_selection[[finger]]
    ) %>% 
    # Extract contingency table
    sdf_crosstab('restimulus', 'subject') %>% collect()
}
  
contingency_tables[[1]] %>% View()
filtered_selection[[5]] %>% length()
  
plot_contingency_table <- function(
    contingency_table,
    title = 'Contingency table - observations - subject vs movement') {
  contingency_table %>% 
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
      size = guide_legend(title = 'Observations', nrow = 1),
      fill = guide_colorbar(title = NULL, nrow = 1)
    ) +
    scale_x_discrete(name = 'Subject') +
    scale_y_discrete(name = 'Movement') +
    scale_size_continuous(
      range = c(0.5, 7),
      breaks = c(1e1, 1e2, 1e3, 1e4, 2e4, 3e4, 4e4, 5e4, 6e4, 7e4)
    ) +
    scale_fill_viridis_c(direction = 1) +
    ggtitle(title) +
    theme_bw() + theme(
      legend.position = 'top',
      legend.box = 'horizontal',
      legend.box.spacing = unit(2, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      legend.title = element_text(vjust = 0.5),
      panel.background = element_blank(),
      axis.title = element_text(face="bold", size = 10),
      axis.text.x = element_text(colour="black", size = 8),
      axis.text.y = element_text(colour="black", size = 8),
      axis.line = element_line(size=0.5, colour = "black"),
      plot.title = element_text(hjust = 0.5),
      panel.grid = element_line(colour = 'grey', size = 0.2)
    )
}
    
# Loop through all fingers and calculate contingency tables
for (finger in 1:5) {
  plot_contingency_table(
    contingency_tables[[finger]], 
    title = paste(
      'Contingency table - finger', finger,  
      '- num observations per subject and movement'
      )
    )
  ggsave(
    filename = paste0(
      'plots/contingency_table_finger_', finger, 
      '_anatomical_small_range_iqr_filt.png'
    ),
    width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
}

# Condense contingency tables too check the number of movements per subject 
# (and for each finger) where enough data exists for further analysis
contingency_tables_count_movements <- tibble(
  finger = integer(),
  movement = integer(),
  subject = integer(),
  count = integer()
)
for (finger in 1:5) {
  contingency_tables_count_movements <- contingency_tables_count_movements %>% 
    add_row(
      contingency_tables[[finger]] %>%
        rename(
          movement = restimulus_subject
          ) %>%
        pivot_longer(cols = -movement, names_to = 'subject', values_to = 'count') %>%
        mutate(
          movement = as.numeric(movement),
          subject = as.numeric(subject),
          finger = finger
          )
    )
}

# Plot condensed contingency table
contingency_tables_count_movements %>%
  filter(count != 0) %>%
  group_by(finger, subject) %>% 
  mutate(
    obs_aver = mean(count),
    mov_count = n_distinct(movement)
  ) %>% 
  ungroup() %>% 
  arrange(finger, subject) %>% 
  mutate(
    finger = as.ordered(finger),
    movement = as.ordered(subject)
    ) %>%
  ggplot() +
  geom_count(
    aes(
      x = movement, y = finger, fill = obs_aver, size = mov_count
      ),
    shape = 21, stroke = 0.2
    ) +
  guides(
    size = guide_legend(title = 'No. movements', nrow = 1),
    fill = guide_colorbar(title = 'Aver. observations per movement', nrow = 1)
  ) +
  scale_x_discrete(name = 'Subject') +
  scale_y_discrete(name = 'Finger') +
  scale_size_continuous(
    range = c(0.5, 3),
    breaks = seq(0, 20, 5)
  ) +
  scale_fill_viridis_c(
    direction = 1,
    breaks = c(1e3, 2e4, 4e4, 6e4, 7e4)
    ) +
  # ggtitle('') +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 3, 2, 3, 'mm'),
    legend.title = element_text(size = 6, vjust = 0.5),
    legend.key.width = unit(0.9, 'lines'),
    legend.key.height = unit(0.5, 'lines'),
    legend.text = element_text(size = 5, vjust = 0.5),
    panel.background = element_blank(),
    panel.spacing = unit(0, 'mm'),
    axis.title = element_text(face="bold", size = 6),
    axis.text.x = element_text(colour="black", size = 4.5),
    axis.text.y = element_text(colour="black", size = 5),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    strip.text.x = element_text(size = 5, colour = "black"),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.1),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
    )
ggsave(
  filename = 'plots/contingency_table_fingers_subjects_condensed_IQR_filter.png',
  width = 6.2, height = 1.80, units = 'in', dpi = 320, pointsize = 12)


# Intra-finger correlation matrices - take into account and rerepetition
corr_mat_f <- function(dat_spark, finger, filtered_selection, min_obs = 100) {
  # Obtain entire correlation matrices for groups (restimulus, subject, rerepetition)
  dat_corr <- dat_spark %>% 
    select(restimulus, subject, rerepetition, contains(as.character(!!finger))) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Create column with restimulus - subject combination for filtering
    mutate(
      restimulus_subject = paste(restimulus, subject, sep = '-')
    ) %>%
    # Filter entire dataset using filtered_selection
    filter(
      restimulus_subject %in% !!filtered_selection[[finger]]
    ) %>% 
    select(-restimulus_subject) %>% 
    group_by(restimulus, subject, rerepetition) %>% 
    # Filter out all groups with less than min_obs observations per same repetition
    filter(n() >= !!min_obs) %>% ungroup() %>% compute('dat_corr')
  # TODO: Add number of observations to use as weight factor later
  cart_prod <- dat_corr %>%
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
    print(paste('Calculating:', i, '/', num_groups))
    groups_filt <- dat_corr %>%
      filter(
        restimulus == !!cart_prod$restimulus[i],
        subject == !!cart_prod$subject[i],
        rerepetition == !!cart_prod$rerepetition[i]
        )
    groups_corr <- groups_corr %>% add_row(
      restimulus = cart_prod$restimulus[i],
      subject = cart_prod$subject[i],
      rerepetition = cart_prod$rerepetition[i],
      corrs = list(correlate(
        groups_filt %>% select(!c(restimulus, subject, rerepetition))
        ))
    )
  }
  groups_corr
}

# Intra-finger correlation matrices - take into account only subject
corr_mat_subj_f <- function(dat_spark, finger, filtered_selection, min_obs = 100) {
  # Obtain entire correlation matrices for groups (restimulus, subject, rerepetition)
  dat_corr <- dat_spark %>% 
    select(restimulus, subject, contains(as.character(!!finger))) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Create column with restimulus - subject combination for filtering
    mutate(
      restimulus_subject = paste(restimulus, subject, sep = '-')
    ) %>%
    # Filter entire dataset using filtered_selection
    filter(
      restimulus_subject %in% !!filtered_selection[[finger]]
    ) %>% 
    select(-restimulus_subject) %>% 
    group_by(restimulus, subject) %>% 
    # Filter out all groups with less than min_obs observations per same repetition
    filter(n() >= !!min_obs) %>% ungroup() %>% compute('dat_corr')
  # TODO: Add number of observations to use as weight factor later
  cart_prod <- dat_corr %>%
    select(restimulus, subject) %>%
    sdf_distinct() %>%
    arrange(restimulus, subject) %>%
    collect()
  num_groups <- cart_prod %>% nrow()
  cart_prod %>% as.list()
  groups_corr <- tibble(
    restimulus = integer(),
    subject = integer(),
    corrs = list()
  )
  for (i in 1:num_groups) {
    print(paste('Calculating:', i, '/', num_groups))
    groups_filt <- dat_corr %>%
      filter(
        restimulus == !!cart_prod$restimulus[i],
        subject == !!cart_prod$subject[i]
        )
    groups_corr <- groups_corr %>% add_row(
      restimulus = cart_prod$restimulus[i],
      subject = cart_prod$subject[i],
      corrs = list(correlate(
        groups_filt %>% select(!c(restimulus, subject))
        ))
    )
  }
  groups_corr
}

# Calculate corr matrices for restimulus, subject, rerepetition
# Do not LOOP, instead run for each finger separately and concat list
# Too long to execute and fails instead
filtered_selection[[5]] %>% length()
# corr_mat <- list()
for (finger in 5:5) {
  corr_mat[[finger]] <- corr_mat_f(
  dat_flex_anat, finger = finger, filtered_selection = filtered_selection,
  min_obs = 100
)
}
corr_mat[[5]] %>% glimpse()
corr_mat[[5]] %>% 
  mutate(
    restimulus_subject_rerepetition = paste(restimulus, subject, rerepetition, sep = '-')
  ) %>% 
  distinct(restimulus_subject_rerepetition) %>%
  filter('1-6-5' %in% restimulus_subject_rerepetition)

# Calculate corr matrices for restimulus, subject
# Do not LOOP, instead run for each finger separately and concat list
# Too long to execute and fails instead
corr_mat_subj <- list()
for (finger in 5:5) {
  corr_mat_subj[[finger]] <- corr_mat_subj_f(
  dat_flex_anat, finger = finger, filtered_selection = filtered_selection
)
}
corr_mat_subj[[5]] %>% glimpse()

# Concatenate correlation matrices (rerepetition) in long format
corr_mat_long <- tibble(
  restimulus = integer(),
  subject = integer(),
  rerepetition = integer(),
  x = character(),
  y = character(),
  corr = numeric(),
  finger = integer(),
)
for (finger in 1:5) {
  corr_mat_long <- corr_mat_long %>% add_row(
    corr_mat[[finger]] %>% 
    mutate(
      # Keep only lower triangle
      corrs = map(corrs, shave),
      # Long format for plotting
      corrs = map(corrs, stretch),
      ) %>% unnest(corrs) %>% 
    rename(corr = r) %>% 
    mutate(
      finger = finger
    )
  ) 
}
corr_mat_long <- corr_mat_long %>% 
  select(finger, restimulus, subject, rerepetition, x, y, corr) %>% 
  drop_na() %>% 
  arrange(finger, restimulus, subject, rerepetition, desc(abs(corr)))
corr_mat_long %>% View()

# Number of correlation coefficients
corr_mat_long %>% 
  mutate(
    x_y = paste0(x, y)
  ) %>% 
  select(-x, -y) %>% 
  group_by(finger, restimulus, x_y) %>% 
  summarise(
    count = n()
  ) %>% View()

# Concatenate correlation matrices (subject) in long format
corr_mat_subj_long <- tibble(
  restimulus = integer(),
  subject = integer(),
  x = character(),
  y = character(),
  corr = numeric(),
  finger = integer(),
)
for (finger in 5:5) {
  corr_mat_subj_long <- corr_mat_subj_long %>% add_row(
    corr_mat_subj[[finger]] %>% 
    mutate(
      # Keep only lower triangle
      corrs = map(corrs, shave),
      # Long format for plotting
      corrs = map(corrs, stretch),
      ) %>% unnest(corrs) %>% 
    rename(corr = r) %>% 
    mutate(
      finger = finger
    )
  ) 
}
corr_mat_subj_long <- corr_mat_subj_long %>% 
  select(finger, restimulus, subject, x, y, corr) %>% 
  drop_na() %>% 
  arrange(finger, restimulus, subject, desc(abs(corr)))
corr_mat_subj_long %>% View()

# Number of correlation coefficients (13 - 31 per dependency)
corr_mat_subj_long %>% 
  mutate(
    x_y = paste0(x, y)
  ) %>% 
  select(-x, -y) %>% 
  group_by(finger, restimulus, x_y) %>% 
  summarise(
    count = n()
  ) %>% View()

# Boxplot of correlation coefficients (rerepetition)
boxplot_correlation_coeffs <- function(
    corr_matrix_long_format,
    finger_select = 1,
    title = 'Boxplot of intra-finger correlations - Finger x'
    ) {
  corr_matrix_long_format %>% 
    filter(finger == finger_select) %>% 
    mutate(
      x_y = paste(x, y, sep = ' - '),
    ) %>%
    group_by(restimulus, x_y) %>% 
    mutate(
      corr_median_abs = abs(median(corr)),
      restimulus = paste('Movement:', restimulus)
    ) %>% 
    ungroup() %>%
    ggplot() +
    facet_wrap(. ~ restimulus, scales = 'fixed', nrow = 4) +
    geom_boxplot(
      aes(
        x = corr,
        y = x_y,
        fill = corr_median_abs,
      ),
      width = 0.6, alpha = 1, outlier.size = 1, outlier.stroke = 0.1
    ) + 
    scale_x_continuous(
    name = 'Correlation',
    breaks = seq(-1, 1, 0.25)
    ) +
    scale_fill_viridis_c(
      name = 'Absolute median correlation',
      option = 'viridis',
      begin = 0.4,
      limits = c(0.7, 1),
      breaks = seq(0.7, 1, 0.1)
      ) +
    ggtitle(title) +
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
}
# Save boxplots of intra-finger correlations
for (finger in 1:5) {
  boxplot_correlation_coeffs(
    corr_mat_long, finger_select = finger,
    title = paste("Boxplot of intra-finger correlations - Finger", finger)
    )
  ggsave(
    filename = paste0(
      'plots/boxplot_correlations_intra-finger_', finger,  '.png'
    ),
    width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
}

## Filter correlation boxplot using 1.5 IQR rule (rerepetition)
## Create pipeline for filtering
iqr_corr_filter_pipeline <- . %>% 
  mutate(
    x_y = paste(x, y, sep = ' - '),
  ) %>%
  group_by(finger, restimulus, x_y) %>%
  # Calculate quantiles and IQR for correlations (all rerepetitions grouped)
  mutate(
    q1_group = quantile(corr, 0.25),
    q3_group = quantile(corr, 0.75),
    iqr_group = IQR(corr),
    lbound = q1_group - 1.5 * iqr_group,
    ubound = q3_group + 1.5 * iqr_group,
  ) %>% 
  ungroup() %>% 
  mutate(
    corr = case_when(
      corr <= lbound ~ NA_real_,
      corr >= ubound ~ NA_real_,
      T ~ corr
    )
  ) %>% 
  drop_na()

## Filter dat with 1.5 IQR rule iteratively 14 times
corr_mat_long_iqr_filt <- 
  iqr_corr_filter_pipeline(
    iqr_corr_filter_pipeline(
      iqr_corr_filter_pipeline(
        iqr_corr_filter_pipeline(
          iqr_corr_filter_pipeline(
            iqr_corr_filter_pipeline(
              iqr_corr_filter_pipeline(
                iqr_corr_filter_pipeline(
                  iqr_corr_filter_pipeline(
                    iqr_corr_filter_pipeline(
                      iqr_corr_filter_pipeline(
                        iqr_corr_filter_pipeline(
                          iqr_corr_filter_pipeline(
                            iqr_corr_filter_pipeline(
                              iqr_corr_filter_pipeline(
                                corr_mat_long
                                )
                              )
                            )
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )

# Save boxplots of intra-finger correlations after 1.5 IQR filtering
for (finger in 1:5) {
  boxplot_correlation_coeffs(
    corr_mat_long_iqr_filt, finger_select = finger,
    title = paste0(
      "Boxplot of intra-finger correlations - Finger ", finger, ', 1.5 IQR filtered'
      )
    )
  ggsave(
    filename = paste0(
      'plots/boxplot_correlations_intra-finger_', finger,  '_iqr_filt.png'
    ),
    width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)
}

# Save only plot examples for correlation coefficients per finger dependency - MDPI
corr_mat_long %>%
  mutate(
    outliers = 'with outliers'
    ) %>%
  add_row(
    corr_mat_long_iqr_filt %>%
      select(!c(x_y, q1_group, q3_group, iqr_group, lbound, ubound)) %>% 
      mutate(outliers = '1.5 IQR rule')
    ) %>%
  # filter((finger == 3 & restimulus == 21)) %>%
  filter((finger == 5 & restimulus == 2)) %>%
  mutate(
    x_y = paste(x, y, sep = ' - '),
  ) %>%
  group_by(outliers, finger, restimulus, x_y) %>% 
  mutate(
    corr_median_abs = abs(median(corr)),
    restimulus = paste('Movement:', restimulus)
  ) %>%
  ungroup() %>%
  mutate(
    grouping = factor(paste(restimulus, outliers, sep = ' - '), ordered = F),
    # reverser factor order for panels
    grouping = factor(grouping, levels=rev(levels(grouping)))
  ) %>%
  ggplot() +
  facet_wrap(. ~ grouping, scales = 'fixed', nrow = 1) +
  geom_boxplot(
    aes(
      x = corr,
      y = x_y,
      fill = corr_median_abs,
    ),
    width = 0.3, alpha = 1, outlier.size = 0.3, outlier.stroke = 0.1, size = 0.2
  ) + 
  scale_x_continuous(
    name = expression('Pearson\'s correlation coefficient ('*italic(r)*')'),
    breaks = seq(-1, 1, 0.50)
  ) +
  scale_y_discrete(
    name = 'Dependency'
  ) +
  scale_fill_viridis_c(
    name = 'Absolute median correlation',
    option = 'viridis',
    begin = 0.4,
    limits = c(0.7, 1),
    breaks = seq(0.7, 1, 0.1)
    ) +
  guides(
    fill = guide_colorbar(
      nrow = 1,
      barheight = 0.5
      )
  ) +
  # ggtitle('Finger 3') +
  ggtitle('Finger 5') +
  theme_bw() + theme(
    # legend.position = 'top',
    legend.position = 'none',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    legend.text = element_text(color="black", size = 7),
    legend.title = element_text(color = 'black', size = 7, vjust = 1),
    panel.background = element_blank(),
    panel.spacing = unit(1, 'mm'),
    panel.border = element_rect(size = 0.1),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    axis.title = element_text(colour="black", size = 7),
    # axis.title.x = element_text(face="bold", size = 9),
    axis.title.y = element_blank(),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", size = 7),
    axis.line = element_line(size=0.1, colour = "black"),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
    plot.title = element_text(size = 7, hjust = 0.5),
    strip.text.x = element_text(size = 6.5, colour = "black"),
    strip.background = element_rect(size = 0.1)
  )
# ggsave(
#   filename = 'plots/boxplot_correlations_intra-finger_3_examples_iqr_filt.png',
#   width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)
ggsave(
  filename = 'plots/boxplot_correlations_intra-finger_5_examples_iqr_filt.png',
  width = 3.3, height = 2.36, units = 'in', dpi = 320, pointsize = 12)

## Extract remaining data after filtering based on correlation matrices
## for selection over entire dataset
## filtered_selection_corr[[finger_index]] to extract list of 
## motion-subject-rerepetition combinations
filtered_selection_corr <- list()
for (finger in 1:5) {
  filtered_selection_corr[[finger]] <- 
    corr_mat_long_iqr_filt %>%
    select(finger, restimulus, subject, rerepetition) %>%
    mutate(
      restimulus_subject_rerepetition = paste(
        restimulus, subject, rerepetition, sep = '-'
        )
    ) %>% 
    # Do not forget !! injection since finger exists in tibble and loop variable
    filter(finger == !!finger) %>% 
    select(restimulus_subject_rerepetition) %>% 
    distinct(restimulus_subject_rerepetition) %>% 
    pull(restimulus_subject_rerepetition)
}

# filtered_selection was used before corr matrices
# Check if filtered_selection_corr is enough for filtering (all must be true)
for (finger in 1:5) {
  print(paste('Finger', finger, ':', all(
    (filtered_selection_corr[[finger]] %>% str_sub(end = -3)) %in% 
      filtered_selection[[finger]])))
}

# Filter out all moderately and highly correlated values (>= 0.7)
corr_mat_long_iqr_filt_moderate <- corr_mat_long_iqr_filt %>%
  group_by(finger, restimulus, x_y) %>%
  mutate(
    corr_median_abs = abs(median(corr))
    ) %>% ungroup() %>% 
  filter(corr_median_abs >= 0.7)
# View selected values
corr_mat_long_iqr_filt_moderate %>%
  group_by(finger, restimulus, x_y) %>%
  mutate(
    corr_median = median(corr)
    ) %>% ungroup() %>% 
  distinct(finger, x_y, restimulus, corr_median) %>% 
  # Round corrs for table
  mutate(
    corr_median = round(corr_median, 2)
  ) %>% 
  group_by(finger, x_y) %>% 
  summarise(
    n_rest = n(),
    restimulus = paste(
      restimulus, '(', corr_median, ')', sep = '', collapse = " - "
      )
  ) %>% View()

# View if low corr always present for certain subjects
# Subject 45 a lot of low correlations, but cannot distinct from the rest
corr_mat_long_iqr_filt_moderate %>%
  group_by(finger, restimulus, x_y) %>%
  mutate(
    corr_median = median(corr)
    ) %>% ungroup() %>% 
  ungroup() %>% 
  mutate(
    corr_abs = abs(corr)
  ) %>% 
  select(finger, restimulus, subject, rerepetition, x_y, corr_abs) %>% 
  filter(corr_abs < 0.5) %>% 
  arrange(subject, rerepetition, restimulus, x_y) %>% 
  group_by(subject) %>% 
  summarise(
    n_per_subj = n()
  ) %>% 
  summarise(
    summ = sum(n_per_subj)
  ) %>% 
  View()


## Extract remaining data after filtering based on high 
## correlation matrices for selection over entire dataset
## filtered_selection_corr_moderate to extract list of 
## dependency-motion-subject-rerepetition combinations
filtered_selection_corr_moderate <- corr_mat_long_iqr_filt_moderate %>%
  select(x_y, x, y, restimulus, subject, rerepetition) %>%
  distinct(x_y, x, y, restimulus, subject, rerepetition)

# Additional data sampling with respect to IQR share 
# IQR of rerepetition for movement / Average IQR share across rerepetitions >= 0.5
filtered_selection_corr_high_iqr <- 
  filtered_selection_corr_moderate %>%
  mutate(
    iqr_x = NA,
    iqr_y = NA
  )

for (i in 1:nrow(filtered_selection_corr_high_iqr)){
  iqrs_i <- dat_flex_anat %>%
    select(restimulus, subject, rerepetition,
           !!as.symbol(filtered_selection_corr_high_iqr$x[i]),
           !!as.symbol(filtered_selection_corr_high_iqr$y[i]),
           ) %>% 
    filter(
      restimulus == !!filtered_selection_corr_high_iqr$restimulus[i],
      subject == !!filtered_selection_corr_high_iqr$subject[i],
      rerepetition == !!filtered_selection_corr_high_iqr$rerepetition[i],
      ) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Extract data
    collect() %>% 
    summarise(
      iqr_x = IQR(!!as.symbol(filtered_selection_corr_high_iqr$x[i])),
      iqr_y = IQR(!!as.symbol(filtered_selection_corr_high_iqr$y[i]))
    )
  filtered_selection_corr_high_iqr$iqr_x[i] <-  iqrs_i$iqr_x[1]
  filtered_selection_corr_high_iqr$iqr_y[i] <-  iqrs_i$iqr_y[1]
}

filtered_selection_corr_high_iqr <- filtered_selection_corr_high_iqr %>%
  group_by(x_y, restimulus) %>% 
  mutate(
    iqr_x_median = median(iqr_x),
    iqr_y_median = median(iqr_y)
  ) %>% ungroup() %>% 
  mutate(
    iqr_x_share = iqr_x / iqr_x_median,
    iqr_y_share = iqr_y / iqr_y_median
  ) %>%
  filter(iqr_x_share >= 0.5, iqr_y_share >= 0.5)

corr_mat_long_iqr_filt_high_iqr_share <- right_join(
  x = corr_mat_long_iqr_filt_moderate,
  y = filtered_selection_corr_high_iqr,
  by = c('x_y', 'x', 'y', 'restimulus', 'subject', 'rerepetition'),
  keep = F
  ) %>%
  # Recalculate median absolute correlation by dependency and movement
  group_by(x_y, restimulus) %>% 
  mutate(
    corr_median_abs = abs(median(corr))
  )
corr_mat_long_iqr_filt_high_iqr_share %>% glimpse()

## Filter correlation data with 1.5 IQR rule iteratively 9 times
corr_mat_long_iqr_filt_high_iqr_share_filt <-
  iqr_corr_filter_pipeline(
    iqr_corr_filter_pipeline(
      iqr_corr_filter_pipeline(
        iqr_corr_filter_pipeline(
          iqr_corr_filter_pipeline(
            iqr_corr_filter_pipeline(
              iqr_corr_filter_pipeline(
                iqr_corr_filter_pipeline(
                  iqr_corr_filter_pipeline(
                    iqr_corr_filter_pipeline(
                      corr_mat_long_iqr_filt_high_iqr_share
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
corr_mat_long_iqr_filt_high_iqr_share_filt %>% glimpse()
# After 1.5 IQR outlier removal, some median correlations drop below 0.7
# Drop such observations
corr_mat_long_iqr_filt_high_iqr_share_filt <- 
  corr_mat_long_iqr_filt_high_iqr_share_filt %>% 
  filter(corr_median_abs >= 0.7)

## Extract remaining data after filtering based on high 
## correlation matrices for selection over entire dataset
## filtered_selection_corr_high_iqr_filt to extract list of 
## dependency-motion-subject-rerepetition combinations
filtered_selection_corr_high_iqr_filt <- corr_mat_long_iqr_filt_high_iqr_share_filt %>%
  select(x_y, x, y, restimulus, subject, rerepetition) %>%
  distinct(x_y, x, y, restimulus, subject, rerepetition)

# Count number of dependency-restimulus associations
filtered_selection_corr_high_iqr_filt %>% 
  select(x_y, restimulus) %>% 
  distinct() %>% 
  summarise(
    n_assoc = n()
  )

# Plot only highly correlated values (>= 0.7)
corr_mat_long_iqr_filt_high_iqr_share_filt %>% 
# corr_mat_long_iqr_filt_moderate %>% 
  # filter((restimulus %in% c(1, 2, 3, 5, 10))) %>%
  # filter((restimulus %in% c(21, 22, 16, 11, 12, 13, 7, 8, 9, 23))) %>%
  # filter((restimulus %in% c(17, 18, 19, 20, 6))) %>%
  # filter((restimulus %in% c(4, 14, 15))) %>%
  mutate(
    restimulus = paste('Movement:', restimulus)
  ) %>% 
  ggplot() +
  facet_wrap(. ~ restimulus, scales = 'fixed', nrow = 1) +
  geom_boxplot(
    aes(
      y = x_y,
      x = corr,
      fill = corr_median_abs,
    ),
    # Magnify outliers for easier detection
    width = 0.6, alpha = 1, outlier.size = 10, outlier.stroke = 0.1
  ) + 
  scale_x_continuous(
  name = 'Correlation',
  breaks = seq(-1, 1, 0.25)
  ) +
  scale_fill_viridis_c(
    name = 'Absolute median correlation',
    option = 'viridis',
    begin = 0.4,
    limits = c(0.6, 1),
    breaks = seq(0, 1, 0.2)
    ) +
  ggtitle('Intra-finger correlation boxplot - Moderate-to-high correlations, 1.5 IQR filtered') +
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
    axis.title.y = element_blank(),
    axis.title.x = element_text(face="bold", size = 9),
    axis.text.x = element_text(colour="black", size = 7, angle = 0),
    axis.text.y = element_text(colour="black", size = 7),
    axis.line = element_line(size=0.5, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    panel.grid = element_line(colour = 'grey', size = 0.2)
  )
ggsave(
  filename = paste0(
    # 'plots/boxplot_correlations_intra-finger_m_1_2_3_5_10_iqr_filt.png'
    # 'plots/boxplot_correlations_intra-finger_m_7_8_9_11_12_13_16_21_221_23_iqr_filt.png'
    # 'plots/boxplot_correlations_intra-finger_m_6_17_18_19_20_iqr_filt.png'
    'plots/boxplot_correlations_intra-finger_m_4_14_15_iqr_filt.png'
  ),
  width = 32, height = 10, units = 'cm', dpi = 320, pointsize = 12)
  # width = 32, height = 19, units = 'cm', dpi = 320, pointsize = 12)

# Plot highly correlated intra-joint dependencies vs movement
# corr_mat_long_iqr_filt_moderate %>%
corr_mat_long_iqr_filt_high_iqr_share_filt %>% 
  group_by(finger, restimulus, x_y) %>%
  mutate(
    corr_median = round(median(corr), 2)
    ) %>% ungroup() %>% 
  select(x_y, restimulus, corr_median) %>% 
  mutate(restimulus = as.ordered(restimulus)) %>% 
  distinct(x_y, restimulus, corr_median) %>% 
  group_by(x_y) %>% 
  mutate(count_joint = n()) %>%
  group_by(restimulus) %>%
  mutate(count_rest = n()) %>% 
  ungroup() %>% 
  arrange(count_joint) %>% 
  # mutate(count_joint = as.ordered(count_joint)) %>%
  mutate(
    x_y = factor(x_y, levels = unique(x_y))
  ) %>% 
  arrange(desc(count_rest)) %>% 
  # mutate(count_rest = as.ordered(count_rest)) %>%
  mutate(
    restimulus = factor(restimulus, levels = unique(restimulus))
  ) %>% 
  ggplot(
    aes(
      x = restimulus, y = x_y, size = count_rest, color = count_joint,
      label = corr_median
      )
    ) +
  geom_point() +
  geom_hline(yintercept = 12.5, size = 0.15) +
  geom_text(size = 2, color = 'black') +
  guides(
    size = guide_legend(title = 'Count per movement', nrow = 2),
    color = guide_legend(title = 'Count per dependency', nrow = 2)
  ) +
  scale_x_discrete(name = 'Movement') +
  # scale_y_discrete(name = 'Dependency') +
  scale_y_discrete(name = NULL) +
  scale_size_continuous(
    range = c(2, 4.5),
    # Breaks added manually after plot
    # breaks = c(1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20)
    breaks = 1:20
    ) +
  scale_color_viridis_c(
    direction = 1,
    begin = 0.3,
    end = 0.9,
    breaks = c(1, 2, 3, 4, 5, 6, 7, 18, 19, 20)
    ) +
  # ggtitle('Count of highly correlated \u2265 0.7 joint dependencies per movement') +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 1, 1, 0, 'mm'),
    legend.title = element_text(size = 6, vjust = 0.5),
    legend.key.width = unit(0.9, 'lines'),
    legend.key.height = unit(0.5, 'lines'),
    legend.text = element_text(size = 5, vjust = 0.5),
    panel.background = element_blank(),
    panel.spacing = unit(0, 'mm'),
    axis.title = element_text(face="bold", size = 6),
    axis.text.x = element_text(colour="black", size = 4.5),
    axis.text.y = element_text(colour="black", size = 5),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    strip.text.x = element_text(size = 5, colour = "black"),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.1),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
  )
ggsave(
  filename = paste0(
    'plots/count_joint_dependency_movement_',
    'anatomical_small_range_iqr_filt_after_high_corr.png'
    ),
  width = 6.2, height = 3.80, units = 'in', dpi = 320, pointsize = 12
  )

# Contingency tables after correlation filtering per dependency
# (num observations per dependency, motion and subject-rerepetition)
contingency_tables_after_corr_filt <- list()
dependency_list <- filtered_selection_corr_high_iqr_filt %>% 
  select(x_y) %>% distinct() %>% pull(x_y)
for (dependency in dependency_list) {
  # Selection of joint columns
  x_joint <- filtered_selection_corr_high_iqr_filt %>%
    filter(x_y == dependency) %>% 
    distinct(x, y) %>% pull(x)
  y_joint <- filtered_selection_corr_high_iqr_filt %>%
    filter(x_y == dependency) %>% 
    distinct(x, y) %>% pull(y)
  # Selection of observations after all filtering
  rest_subj_rerep_select <- filtered_selection_corr_high_iqr_filt %>% 
    filter(x_y == dependency) %>%
    distinct(restimulus, subject, rerepetition) %>% 
    mutate(
      restimulus_subject_rerepetition = paste(
        restimulus, subject, rerepetition, sep = '-'
        )
    ) %>% 
    pull(restimulus_subject_rerepetition)
  contingency_tables_after_corr_filt[[dependency]] <- dat_flex_anat %>%
    select(restimulus, subject, rerepetition, !!x_joint, !!y_joint) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Create column with restimulus-subject-rerepetition
    # combinations for filtering
    mutate(
      restimulus_subject_rerepetition = paste(
        restimulus, subject, rerepetition, sep = '-'
        )
    ) %>%
    # Filter entire dataset using rest_subj_rerep
    filter(
      restimulus_subject_rerepetition %in% !!rest_subj_rerep_select
    ) %>%
    # Extract contingency table
    group_by(restimulus, subject,  rerepetition) %>%
    summarise(
      group_count = n()
    ) %>% collect()
}

contingency_tables_after_corr_filt[[dependency]] %>% 
  mutate(
    restimulus = paste('Movement:', restimulus),
    subject = as.ordered(subject),
    rerepetition = as.ordered(rerepetition)
    ) %>% 
  ggplot() +
  facet_wrap(. ~ restimulus, scales = 'free_x') +
  geom_col(aes(x = subject, y = group_count, fill = rerepetition), position = 'stack') +
  scale_fill_nejm()

# Function for plotting contingency tables as stacked bar plots
# Each bar consists of rerepetitions
plot_bars_contingency_table <- function(
    contingency_table,
    title = 'Contingency table - observations - subject vs movement') {
  contingency_table %>%
  # contingency_tables_after_corr_filt[['PIP2 - DIP2']] %>% 
    mutate(
      restimulus = paste('Movement:', restimulus),
      subject = as.ordered(subject),
      rerepetition = as.ordered(rerepetition)
      ) %>% 
    ggplot() +
    facet_wrap(. ~ restimulus, scales = 'free_x') +
    geom_col(
      aes(
        x = subject, y = group_count, fill = rerepetition
        ),
      position = 'stack'
      ) +
    guides(
      fill = guide_legend(title = 'Rerepetition', nrow = 1)
    ) +
    scale_x_discrete(name = 'Subject') +
    scale_y_continuous(
      name = 'Count',
      breaks = seq(0, 60000, 10000)
      ) +
    scale_fill_nejm() +
    ggtitle(title) +
    theme_bw() + theme(
      legend.position = 'top',
      legend.box = 'horizontal',
      legend.box.spacing = unit(2, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      legend.title = element_text(vjust = 0.5),
      panel.background = element_blank(),
      axis.title = element_text(face="bold", size = 10),
      axis.text.x = element_text(colour="black", size = 4),
      axis.text.y = element_text(colour="black", size = 8),
      axis.line = element_line(size=0.5, colour = "black"),
      plot.title = element_text(hjust = 0.5),
      panel.grid = element_line(colour = 'grey', size = 0.2)
    )
}

# Loop through all fingers and plot contingency tables after correlation filtering
for (dependency in dependency_list) {
  plot_bars_contingency_table(
    contingency_tables_after_corr_filt[[dependency]], 
    title = paste(
      'Contingency table (corrs >= 0.7) - Dependency', dependency,  
      'per movement, subject and rerepetition'
      )
    )
  ggsave(
    filename = paste0(
      'plots/contingency_table_dependency_', gsub(" ", "", dependency, fixed = T),
      '_anatomical_small_range_iqr_filt_after_moderate_corr.png'
    ),
    width = 27, height = 19, units = 'cm', dpi = 320, pointsize = 12)
}

# Plot 2 examples for contingency tables - MDPI
# dependency <- 'MCP3_f - PIP3'
dependency <- 'CMC5 - MCP5_f'
contingency_tables_after_corr_filt[[dependency]] %>% 
  # filter(restimulus == 3) %>%
  filter(restimulus == 1) %>%
  mutate(
    # grouping = paste0(dependency,', ', 'Movement: ', restimulus, ', median r = 0.94'),
    grouping = paste0(dependency,', ', 'Movement: ', restimulus, ', median r = 0.76'),
    subject = as.ordered(subject),
    rerepetition = as.ordered(rerepetition)
    ) %>% 
  ggplot() +
  facet_wrap(. ~ grouping, scales = 'free_x') +
  geom_col(
    aes(
      x = subject, y = group_count, fill = rerepetition
      ),
    position = 'stack'
    ) +
  guides(
    fill = guide_legend(title = 'Rerepetition', nrow = 1)
  ) +
  scale_x_discrete(name = 'Subject') +
  scale_y_continuous(
    # name = 'No. observations',
    name = NULL,
    breaks = seq(0, 60000, 10000)
    ) +
  scale_fill_nejm() +
  theme_bw() + theme(
    legend.position = 'top',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 1, 0, 'mm'),
    legend.title = element_text(size = 6, vjust = 0.5),
    legend.key.width = unit(0.9, 'lines'),
    legend.key.height = unit(0.5, 'lines'),
    legend.text = element_text(size = 6, vjust = 0.5),
    panel.background = element_blank(),
    panel.spacing = unit(0, 'mm'),
    axis.title = element_text(face="bold", size = 6),
    # axis.text.x = element_text(colour="black", size = 5.0),
    axis.text.x = element_text(colour="black", size = 6),
    axis.text.y = element_text(colour="black", size = 6),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5),
    strip.text.x = element_text(size = 6, colour = "black"),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.1),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
  )
ggsave(
  filename = paste0(
    'plots/contingency_table_example_dependency_',
    gsub(" ", "", dependency, fixed = T),
    '_anatomical_small_range_iqr_filt_after_high_corr.png'
    ),
  # width = 3.48, height = 2.36, units = 'in', dpi = 320, pointsize = 12
  width = 2.99, height = 2.36, units = 'in', dpi = 320, pointsize = 12
  )

# Count subject, rerepetitions and observations for MCP3_f - PIP3 (movement 3) and 
# CMC5 - MCP5_f (movement 1) dependency
# dependency <- 'MCP3_f - PIP3'
dependency <- 'CMC5 - MCP5_f'
contingency_tables_after_corr_filt[[dependency]] %>% 
  # filter(restimulus == 3) %>%
  filter(restimulus == 1) %>%
  summarise(
    dependency = dependency,
    restimulus = unique(restimulus),
    subj_count = length(unique(subject)),
    rerep_count = n(),
    obs_total = sum(group_count)
  )

# Function for plotting all scatterplots with fit lines for all 
# restimulus-dependency combinations (final data selection)
plot_scatter_fit_restimulus_dependency <- function(
    restimulus_select, x_joint, y_joint, subject_rerepetition_select,
    spark_tbl_data, title
    ){
  spark_tbl_data %>%
    select(restimulus, subject, rerepetition, !!x_joint, !!y_joint) %>%
    filter(restimulus == restimulus_select) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Create column with restimulus-subject-rerepetition
    # combinations for filtering
    mutate(
      subject_rerepetition = paste(subject, rerepetition, sep = '-')
    ) %>%
    # Filter entire dataset using rest_subj_rerep
    filter(
      subject_rerepetition %in% !!subject_rerepetition_select
    ) %>%
    # Extract data
    collect() %>% 
    # Draw raster plot
    ggplot(aes(x = !!as.symbol(x_joint), y = !!as.symbol(y_joint))) +
    facet_grid(rerepetition ~ subject) +
    geom_point(size = 0.1) +
    geom_smooth(
      method = lm, formula = y ~ poly(x, 2),
      color = pal_nejm('default', alpha = 0.6)(8)[1]
      ) +
    geom_smooth(
      method = lm, formula = y ~ x,
      color = pal_nejm('default', alpha = 0.6)(8)[2]
                ) +
    scale_x_continuous(
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      expand = c(0, 0)
    ) +
    ggtitle(title) +
    theme_bw() + theme(
      legend.position = 'none',
      legend.box = 'horizontal',
      legend.box.spacing = unit(0, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(0, 'mm'),
      axis.title = element_text(face="bold", size = 7),
      axis.text.x = element_text(colour="black", size = 5),
      axis.text.y = element_text(colour="black", size = 7),
      axis.line = element_line(size=0.1, colour = "black"),
      plot.title = element_text(hjust = 0.5, size = 7),
      panel.grid = element_line(colour = 'grey', size = 0.1),
      strip.text.x = element_text(size = 7, colour = "black"),
      strip.text.y = element_text(size = 7, colour = "black"),
      panel.border = element_rect(size = 0.1),
      strip.background = element_rect(size = 0.1),
      axis.ticks = element_line(size = 0.1),
      axis.ticks.length = unit(0.1, 'lines')
      )
}

# Plot all scatterplots with fit lines for all restimulus-dependency combinations
# dependency <- 'MCP3_f - PIP3'
# dependency <- 'CMC5 - MCP5_f'
# dependency <- 'CMC1_f - MCP1'
for (dependency in dependency_list) {
  # Selection of joint columns
  x_joint <- filtered_selection_corr_high_iqr_filt %>%
    filter(x_y == dependency) %>% 
    distinct(x, y) %>% pull(x)
  y_joint <- filtered_selection_corr_high_iqr_filt %>%
    filter(x_y == dependency) %>% 
    distinct(x, y) %>% pull(y)
  # Only highly correlated movements
  rest_list <- filtered_selection_corr_high_iqr_filt %>% 
    filter(x_y == dependency) %>%
    distinct(restimulus) %>% 
    pull(restimulus)
  # Loop over each movement
  for (rest_select in rest_list) {
    # Selection of observations after all filtering
    subj_rerep_select <- filtered_selection_corr_high_iqr_filt %>% 
      filter(x_y == dependency & restimulus == rest_select) %>%
      distinct(restimulus, subject, rerepetition) %>% 
      mutate(
        subject_rerepetition = paste(
          subject, rerepetition, sep = '-'
          )
      ) %>% 
      pull(subject_rerepetition)
    # print(paste('Movement: ', rest_select))
    # print(paste('Subject-rerepetition: ', subj_rerep_select))
    print(paste('Plotting - Movement:', rest_select, 'Dependency:', dependency))
    plot_scatter_fit_restimulus_dependency(
      restimulus_select = rest_select, 
      x_joint = x_joint,
      y_joint = y_joint,
      subject_rerepetition_select = subj_rerep_select,
      spark_tbl_data = dat_flex_anat,
      title = paste(
        'Scatter - Movement:', rest_select, '- Dependency:', dependency
        )
      )
    ggsave(
      filename = paste0(
        'plots/scatter_fit_movement_', rest_select, '_dependency_',
        gsub(" ", "", dependency, fixed = T), '.png'
        ),
      width = 34, height = 19, units = 'cm', dpi = 320, pointsize = 12
      )
  }
}

# Summary statistics on remaining data for modelling
# subjects and rerepetitions per dependency-restimulus
filtered_selection_corr_high_iqr_filt %>% 
  group_by(x_y, restimulus, subject) %>%
  summarise(
    # Count rerepetitions per subject
    rerep_count_subj = n()
  ) %>% 
  mutate(
    # Count subjects and rerepetitions per dependency-restimulus
    subj_count = n_distinct(subject),
    rerep_count = sum(rerep_count_subj),
    rerep_median_subj = median(rerep_count_subj)
  ) %>%
  ungroup() %>% 
  select(!c(subject, rerep_count_subj)) %>%
  distinct() %>% 
  rename(
    Subjects = subj_count, Rerepetitions = rerep_count,
    'Median\n rerepetitions\n per subject' = rerep_median_subj
  ) %>% 
  pivot_longer(
    cols = c(Subjects, Rerepetitions, 'Median\n rerepetitions\n per subject'),
    names_to = 'grouping',
    values_to = 'Count',
  ) %>% 
  mutate(
    grouping = ordered(
      grouping, 
      levels = c('Subjects', 'Rerepetitions', 'Median\n rerepetitions\n per subject'))
  ) %>% 
  group_by(grouping) %>% 
  ggplot(
    aes(
      x = Count,
      y = grouping,
      fill = grouping,
      group = grouping,
    ),
  ) +
  facet_wrap(. ~ grouping, scales = 'free', nrow = 3) +
  geom_boxplot(
    width = 0.3, alpha = 1, outlier.size = 0.3, outlier.stroke = 0.1, size = 0.2
  ) + 
  # geom_jitter(width = 0.00, height = 0.2) +
  geom_dotplot(binaxis = 'x', stackdir = 'up', dotsize = 0.07) +
  # stat_summary(
  #   fun.data = function(x, )
  # ) +
  stat_summary(
    fun.data = function(x, y){
      return(tibble(
        label = paste0(
          "Min =", round(min(x), 1), "\n",
          "Q1 =", round(quantile(x, 0.25), 1), "\n",
          "Median =", round(median(x), 1), "\n",
          "Q3 =", round(quantile(x, 0.75), 1), "\n",
          "Max =", round(max(x), 1), "\n"
        ),
        x = 1,
        y = 2
      ))
    },
    geom = 'text',
    hjust = 0,
    position = position_nudge(y = -0.3),
    size = 2.0
    ) +
  scale_x_continuous(
    # name = expression('Pearson\'s correlation coefficient ('*italic(r)*')'),
    # breaks = seq(-1, 1, 0.50)
    limits = c(2, NA)
  ) +
  scale_y_discrete(
    # name = 'Dependency'
  ) +
  scale_fill_nejm() +
  # ggtitle('Data structure') +
  theme_bw() + theme(
    # legend.position = 'top',
    legend.position = 'none',
    legend.box = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'), 
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    legend.text = element_text(color="black", size = 5),
    legend.title = element_text(color = 'black', size = 5, vjust = 1),
    panel.background = element_blank(),
    panel.spacing = unit(0.5, 'mm'),
    panel.border = element_rect(size = 0.1),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    axis.title = element_text(colour="black", face = 'bold', size = 6),
    # axis.title.x = element_text(face="bold", size = 9),
    axis.title.y = element_blank(),
    axis.text.x = element_text(colour="black", size = 6),
    axis.text.y = element_text(colour="black", face = 'bold', size = 6),
    axis.line = element_line(size=0.1, colour = "black"),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
    plot.title = element_text(size = 5, hjust = 0.5),
    # strip.text.x = element_text(size = 6.5, colour = "black"),
    strip.text.x = element_blank(),
    strip.background = element_blank()
  )
ggsave(
  filename = 'plots/boxplot_data_final_structure.png',
  width = 4.5, height = 2.5, units = 'in', dpi = 320, pointsize = 12)

# Stratified split to n fold while using last for test and rest for train data
stratified_train_test <- function(subj_rerep, n_folds=5){
  #' Split data to breaks_num folds with as much subjects as possible in each fold
  #' Take one fold for test data, and rest for cross-validation
  #' Logic is to sequentially assign rerepetitions (sorted by subject, but shuffled)
  #' to different folds to maximise number of subjects per each fold (minimise 
  #' number of rerepetitions per subject in a single fold)
  # Create fold ids in 1:n_folds sequences (total length = num of rows)
  foldids <- rep_len(1:n_folds, length.out = nrow(subj_rerep))
  subj_rerep <- subj_rerep %>% 
    mutate(
      # Shuffle subjects and arrange by levels (same subjects kept together)
      subject = factor(subject, levels = unique(subject[sample(length(subject))]))
      ) %>% 
    arrange(subject) %>% 
    group_by(subject) %>% 
    # Shuffle rerepetitions
    slice_sample(prop = 1) %>% 
    ungroup() %>% 
    # Add fold ids
    mutate(
      foldid = foldids
    )
  return(
    list(
      train = subj_rerep %>% filter(foldid %in% 1:(n_folds-1)),
      test = subj_rerep %>% filter(foldid == n_folds)
        )
    )
}

weight_scale_transform_model_matrix <- function(
    dependency_restimulus_dat, subj_rerep_foldids_df, keep_intercept=F){
  #' Assign weights to observations, scale the response and factors, derive
  #' poly and exp transformations for x_joint and poly, sqrt, log and exp
  #' transformations to subject height and weight
  #' Assign fold ids to data and construct model matrix with interactions
 dependency_restimulus_dat <- dependency_restimulus_dat %>%  
  # Assign weights to observations to balance datasets
    mutate(
      # Count total number of observations
      n_obs = n(),
      # Count total number of rerepetitions
      n_rereps = length(unique(subject_rerepetition))
      ) %>% 
    group_by(subject, rerepetition) %>% 
    mutate(
      # Count number of observations per each rerepetition
      n_rerep_obs = n(),
    ) %>% ungroup() %>% 
    mutate(
      # Sum of weights inside rerepetition must be equal per rerepetition
      rerep_tot_weight = n_obs / n_rereps,
      # Sum of weights inside rerepetition is divided per each observation
      # equally
      obs_weight = rerep_tot_weight / n_rerep_obs,
      # Standardize the response
      y_joint_scaled = scale(get(y_joint)),
      # Dependent factor
      x_joint_scaled = get(x_joint),
      # Second order transformation of the dependee joint
      x_joint_poly = get(x_joint)^2,
      # Exponential transformation of the dependee joint
      x_joint_exp = exp(get(x_joint)),
      # Second order, sqrt, log, exp transform  of subject height
      x_joint_height = height,
      x_joint_height_poly = x_joint_height^2,
      x_joint_height_sqrt = sqrt(x_joint_height),
      x_joint_height_log = log(x_joint_height),
      x_joint_height_exp = exp(x_joint_height),
      # Second order, sqrt, log, exp transform  of subject weight
      x_joint_weight = subj_weight,
      x_joint_weight_poly = x_joint_weight^2,
      x_joint_weight_sqrt = sqrt(x_joint_weight),
      x_joint_weight_log = log(x_joint_weight),
      x_joint_weight_exp = exp(x_joint_weight)
    ) %>% 
    # Assign fold ids to data by left join with df containing fold ids
    left_join(
      y = subj_rerep_foldids_df, 
      by = c('subject_rerepetition')) %>% 
    # Encode subjects as factors
    mutate(subject = as.factor(subject))
 # Model matrix response vs all transformations of dependee and height and width
 # including interactions
  model_matrix_interactions <- model.matrix(
    as.formula(y_joint_scaled ~ .*.),
    # select response and all variables (starting with x_joint)
    dependency_restimulus_dat %>% select(y_joint_scaled, starts_with('x_joint'))
    )
  # Standardize all predictors and their interactions (exclude intercept)
  model_matrix_interactions[,-1] <- scale(
    model_matrix_interactions[,-1], center = T, scale = T
    )
  # Remove the intercept from model.matrix if necessary
  if (keep_intercept) {
    return(list(
      dat = dependency_restimulus_dat, model_matrix = model_matrix_interactions
      ))
    } else {
      return(list(
        dat = dependency_restimulus_dat, model_matrix = model_matrix_interactions[,-1]
      ))
    }
  }

collect_spark_data <- function(
    restimulus_select, x_joint, y_joint, subject_rerepetition_select,
    spark_tbl_data, info, keep_intercept = F
    ){
  #' Collect data belonging to one restimulus and dependency from spark_tbl_data
  #' remove NA values, build model.matrix with interactions
  #' subject_rerepetition_select -> only TRAIN/TEST data with fold ids
  print(info)
  # Concatenate subject and rerepetition for selection
  subject_rerepetition_select <- subject_rerepetition_select %>%
    mutate(
      subject_rerepetition = paste(
        subject, rerepetition, sep = '-'
        )
      ) %>% select(subject_rerepetition, foldid)
  print(paste('Folds: ', paste(unique(subject_rerepetition_select$foldid), collapse = ','), collapse = ''))
  #'[Sampling 40 rerepetitions only, remove after tests]
  # subject_rerepetition_select <- subject_rerepetition_select[
  #   sample(length(subject_rerepetition_select),  size = 40)
  #   ]
  rest_dep <- spark_tbl_data %>%
    select(
      restimulus, subject, rerepetition,
      laterality, gender, age, height, subj_weight = weight,
      !!x_joint, !!y_joint
      ) %>%
    filter(restimulus == restimulus_select) %>%
    # Drop observations with at least one NA value
    filter_all(all_vars(!is.na(.))) %>%
    # Create column with restimulus-subject-rerepetition
    # combinations for filtering
    mutate(
      subject_rerepetition = paste(subject, rerepetition, sep = '-')
    ) %>%
    # Filter entire dataset using rest_subj_rerep and select only train data
    filter(
      subject_rerepetition %in% !!subject_rerepetition_select$subject_rerepetition
    ) %>%
    # Extract data
    collect()
  # Assign weights, fold ids, transform response and variables,
  # Construct model.matrix with interactions
  rest_dep <- weight_scale_transform_model_matrix(
    dependency_restimulus_dat = rest_dep,
    subj_rerep_foldids_df = subject_rerepetition_select,
    keep_intercept = keep_intercept
  )
  return(rest_dep)
}

panel_plot_fit <- function(
    fit, plot_dat, x_joint, y_joint,
    lmer_subj_vec, grid_wrap_facet = facet_grid,
    lambda = '1se', smooth_term_dim = 5) {
  #' Panel scatter plot for single dependency-restimulus with fits
  # If fit object is GLM
  if ('cv.glmnet' %in% class(fit$fit)) {
    plot_dat$dat <- plot_dat$dat %>%
      mutate(
        predictions = predict(
          fit$fit,
          newx = plot_dat$model_matrix[,-1],
          s = paste0('lambda.', lambda),
          gamma = paste0('gamma.', lambda),
          # Rescale response
          ) * attr(plot_dat$dat$y_joint_scaled, 'scaled:scale') +
          attr(plot_dat$dat$y_joint_scaled, 'scaled:center')
        )
  }
  # If fit object is LME
  if ('lmerModLmerTest' %in% class(fit$fit)) {
    model_matrix <- plot_dat$model_matrix %>% as_tibble() %>% 
      mutate(
        y_joint_scaled = plot_dat$dat$y_joint_scaled,
        subject = plot_dat$dat$subject,
        obs_weight = plot_dat$dat$obs_weight
      ) %>% 
      # Possibility of subjects in test set not present in train data
      filter(subject %in% lmer_subj_vec)
    plot_dat$dat <- plot_dat$dat %>% filter(subject %in% lmer_subj_vec)
    plot_dat$dat <- plot_dat$dat %>% 
      mutate(
        predictions = predict(
          fit$fit,
          newdata = model_matrix,
          # Rescale response
          ) * attr(plot_dat$dat$y_joint_scaled, 'scaled:scale') +
          attr(plot_dat$dat$y_joint_scaled, 'scaled:center')
      )
    }
  plot_obj <- plot_dat$dat %>% 
    ggplot(aes_string(x = x_joint, y = y_joint)) +
    geom_point(size = 0.1) +
    geom_line(
      aes_string(x = x_joint, y = 'predictions'),
      color = pal_nejm('default', alpha = 0.8)(8)[2],
      size = 1.5
      ) +
    geom_smooth(
      method = 'gam',
      formula = y ~ s(x, bs = "cs", k=smooth_term_dim), se = F,
      color = pal_nejm('default', alpha = 0.6)(8)[1]
      ) +
    scale_x_continuous(
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      expand = c(0, 0)
    ) +
    ggtitle(paste('Movement:', unique(plot_dat$dat$restimulus))) +
    theme_bw() + theme(
      legend.position = 'none',
      legend.box = 'horizontal',
      legend.box.spacing = unit(0, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(0, 'mm'),
      axis.title = element_text(face="bold", size = 7),
      axis.text.x = element_text(colour="black", size = 5),
      axis.text.y = element_text(colour="black", size = 7),
      axis.line = element_line(size=0.1, colour = "black"),
      plot.title = element_text(hjust = 0.5, size = 7),
      panel.grid = element_line(colour = 'grey', size = 0.1),
      strip.text.x = element_text(size = 7, colour = "black"),
      strip.text.y = element_text(size = 7, colour = "black"),
      panel.border = element_rect(size = 0.1),
      strip.background = element_rect(size = 0.1),
      axis.ticks = element_line(size = 0.1),
      axis.ticks.length = unit(0.1, 'lines')
      )
  if (identical(grid_wrap_facet, facet_grid)) {
    plot_obj + grid_wrap_facet(rerepetition ~ subject)
  }
  else{
    plot_obj + grid_wrap_facet(~subject_rerepetition)
  }
}

# Plot residuals vs fitted values (and gam curve in the middle)
residuals_plot_fit <- function(
    fit, plot_dat, x_joint, y_joint, lambda = '1se', smooth_term_dim = 5) {
  #' Panel scatter plot for single dependency-restimulus with fits
  plot_dat$dat %>%
    mutate(
      predictions = predict(
        fit$fit,
        # Remove intercept from model matrix
        newx = plot_dat$model_matrix[,-1],
        s = paste0('lambda.', lambda),
        gamma = paste0('gamma.', lambda),
      )
        # Rescale response
        # ) * attr(train_dat$dat$y_joint_scaled, 'scaled:scale') +
        # attr(train_dat$dat$y_joint_scaled, 'scaled:center')
      ) %>%
    mutate(
      # resids = get(y_joint) - predictions
      resids = y_joint_scaled - predictions
    ) %>% 
    ggplot(aes_string(x = 'predictions', y = 'resids', color='subject')) +
    geom_point(size = 0.05) +
    geom_smooth(
      method = 'gam',
      formula = y ~ s(x, bs = "cs", k=smooth_term_dim), se = F,
      # color = pal_nejm('default', alpha = 0.6)(8)[1],
      color = 'black'
      ) +
    scale_x_continuous(
      name = 'Standardized fitted values',
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      name = 'Standardized residuals',
      expand = c(0, 0)
    ) +
    scale_color_viridis_d() +
    guides(color = guide_legend(nrow = 2)) +
    ggtitle(
      paste('Movement:', unique(plot_dat$dat$restimulus))) +
    theme_bw() + theme(
      legend.position = 'top',
      legend.box = 'horizontal',
      legend.box.spacing = unit(0, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(0, 'mm'),
      axis.title = element_text(face="bold", size = 7),
      axis.text.x = element_text(colour="black", size = 5),
      axis.text.y = element_text(colour="black", size = 7),
      axis.line = element_line(size=0.1, colour = "black"),
      plot.title = element_text(hjust = 0.5, size = 7),
      panel.grid = element_line(colour = 'grey', size = 0.1),
      strip.text.x = element_text(size = 7, colour = "black"),
      strip.text.y = element_text(size = 7, colour = "black"),
      panel.border = element_rect(size = 0.1),
      strip.background = element_rect(size = 0.1),
      axis.ticks = element_line(size = 0.1),
      axis.ticks.length = unit(0.1, 'lines')
      )
}

# Check for residual normality - histogram
residuals_hist_plot_fit_glm <- function(
    fit, plot_dat, n_bins=10, lambda = '1se') {
  #' Panel scatter plot for single dependency-restimulus with fits
  plot_dat$dat %>%
    mutate(
      predictions = predict(
        fit$fit,
        newx = plot_dat$model_matrix,
        s = paste0('lambda.', lambda),
        gamma = paste0('gamma.', lambda),
      )
        # Rescale response
        # ) * attr(train_dat$dat$y_joint_scaled, 'scaled:scale') +
        # attr(train_dat$dat$y_joint_scaled, 'scaled:center')
      ) %>%
    mutate(
      # resids = get(y_joint) - predictions
      resids = y_joint_scaled - predictions
    ) %>% 
    ggplot(aes_string(x = 'resids')) +
    geom_histogram(
      bins = n_bins, alpha = 0.9, boundary = 0, color="black",  position = 'identity'
    ) +
    geom_vline(aes(xintercept=0), linetype="dashed", size=0.8) +
    scale_x_continuous(
      name = NULL,
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      name = 'Standardized residuals',
      expand = c(0, 0)
    ) +
    scale_color_viridis_d() +
    guides(color = guide_legend(nrow = 2)) +
    ggtitle(
      paste('Movement:', unique(plot_dat$dat$restimulus))) +
    theme_bw() + theme(
      legend.position = 'top',
      legend.box = 'horizontal',
      legend.box.spacing = unit(0, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(0, 'mm'),
      axis.title = element_text(face="bold", size = 7),
      axis.text.x = element_text(colour="black", size = 5),
      axis.text.y = element_text(colour="black", size = 7),
      axis.line = element_line(size=0.1, colour = "black"),
      plot.title = element_text(hjust = 0.5, size = 7),
      panel.grid = element_line(colour = 'grey', size = 0.1),
      strip.text.x = element_text(size = 7, colour = "black"),
      strip.text.y = element_text(size = 7, colour = "black"),
      panel.border = element_rect(size = 0.1),
      strip.background = element_rect(size = 0.1),
      axis.ticks = element_line(size = 0.1),
      axis.ticks.length = unit(0.1, 'lines')
      )
}
# Check for residual normality - q-q plot
residuals_qq_plot_fit_glm <- function(
    fit, plot_dat, lambda = '1se') {
  #' Panel scatter plot for single dependency-restimulus with fits
  plot_dat$dat %>%
    mutate(
      predictions = predict(
        fit$fit,
        newx = plot_dat$model_matrix,
        s = paste0('lambda.', lambda),
        gamma = paste0('gamma.', lambda),
      )
        # Rescale response
        # ) * attr(train_dat$dat$y_joint_scaled, 'scaled:scale') +
        # attr(train_dat$dat$y_joint_scaled, 'scaled:center')
      ) %>%
    mutate(
      # resids = get(y_joint) - predictions
      resids = y_joint_scaled - predictions
    ) %>% 
    ggplot(aes_string(sample = 'resids')) +
    geom_qq_line(size = 1.3, color = pal_nejm('default', alpha = 0.6)(8)[1]) +
    geom_qq() +
    scale_x_continuous(
      name = 'Theoretical quantiles',
      expand = c(0, 0)
    ) +
    scale_y_continuous(
      name = 'Sample quantiles',
      expand = c(0, 0)
    ) +
    scale_color_viridis_d() +
    guides(color = guide_legend(nrow = 2)) +
    ggtitle(
      paste('Movement:', unique(plot_dat$dat$restimulus))) +
    theme_bw() + theme(
      legend.position = 'top',
      legend.box = 'horizontal',
      legend.box.spacing = unit(0, 'mm'),
      legend.spacing = unit(2, 'mm'),
      legend.margin = margin(0, 0, 0, 0, 'mm'),
      panel.background = element_blank(),
      panel.spacing = unit(0, 'mm'),
      axis.title = element_text(face="bold", size = 7),
      axis.text.x = element_text(colour="black", size = 5),
      axis.text.y = element_text(colour="black", size = 7),
      axis.line = element_line(size=0.1, colour = "black"),
      plot.title = element_text(hjust = 0.5, size = 7),
      panel.grid = element_line(colour = 'grey', size = 0.1),
      strip.text.x = element_text(size = 7, colour = "black"),
      strip.text.y = element_text(size = 7, colour = "black"),
      panel.border = element_rect(size = 0.1),
      strip.background = element_rect(size = 0.1),
      axis.ticks = element_line(size = 0.1),
      axis.ticks.length = unit(0.1, 'lines')
      )
}

# Fit generalized linear regression model to all data - function
fit_glm_restimulus_dependency <- function(
    rest_dep, keep_variables = c(1, 2), fit_intercept = T, 
    gammas = c(0, 0.25, 0.5, 0.75, 1), lambda.min.ratio = 5e-3,
    nlambda = 100, info
    ){
  print(info)
  # Penalty factors - always include linear and second order relationship in model, 
  # x_joint_scaled and x_joint_poly must be first and second variables
  # (intercept removed)
  model_penalty_factors <- rep(1, dim(rest_dep$model_matrix[, -1])[2])
  model_penalty_factors[keep_variables] <- 0.0
  # Generalized linear model fit
  fit <- cv.glmnet(
    # Remove intercept from modelm matrix
    x = rest_dep$model_matrix[, -1],
    y = rest_dep$dat[['y_joint_scaled']],
    penalty.factor = model_penalty_factors,
    weights = rest_dep$dat[['obs_weight']],
    alpha = 1,
    relax = T,
    gamma = gammas,
    nlambda = nlambda,
    lambda.min.ratio = lambda.min.ratio,
    family = 'gaussian',
    standardize = F,
    intercept = fit_intercept,
    foldid = rest_dep$dat[['foldid']],
    type.measure = 'mse',
    # Avoid convergence issue - regulized path
    path = F,
    # Show progress
    trace.it = 0
    # Coefficient values limits
    # lower.limits = -10,
    # upper.limits = 10
      )
  # Extract coefficient names regularized with lambda.1se for LME fit
  coef_names_1se <- coef(fit, s = 'lambda.1se', gamma = 'gamma.1se')[, 1] %>% 
    enframe() %>%
    mutate(abs_value = abs(value)) %>%
    filter(abs_value > 1e-6) %>% pull(name)
  # Remove intercept from list (LME fits intercepts adn random effects)
  coef_names_1se <- coef_names_1se[-1]
  return(list('fit' = fit, 'coef_names_1se' = coef_names_1se))
}

# Fit linear mixed effects model to all data - function
fit_lme_restimulus_dependency <- function(
    rest_dep, keep_variables = c('x_joint_scaled', 'x_joint_poly'), info
    ){
  print(info)
  model_matrix <- rest_dep$model_matrix %>%
    as_tibble() %>% 
    mutate(
      y_joint_scaled = rest_dep$dat$y_joint_scaled,
      subject = rest_dep$dat$subject,
      obs_weight = rest_dep$dat$obs_weight
    )
  # Model fit
  fit <- lmer(
    as.formula(
      paste0(
        'y_joint_scaled~',
        # Fixed effects
        paste0(
          keep_variables,
          collapse = '+'
        ),
        # Don't fit intercept
        '+0',
        # Random effects
        '+', paste0(
          c('(1|subject)'),
          collapse = '+'
        )
        )
      ),
    data = model_matrix,
    weights = model_matrix$obs_weight,
    REML = T
    )
  return(list('fit' = fit))
}
  
compute_predictions_residuals <- function(in_dat, fit, y_joint){
  #' Use train/test data and model matrix to compute predictions and residuals,
  #' both standardized and rescaled 
  # If fit object is GLM
  if ('cv.glmnet' %in% class(fit$fit)) {
    dat_with_pred_resid <- in_dat$dat %>%
      mutate(
        # Standardized predictions
        predictions_scaled_glm = predict(
          fit$fit,
          # Remove intercept from model matrix
          newx = in_dat$model_matrix[,-1],
          s = 'lambda.1se',
          gamma = 'gamma.1se',
          ),
        # Standardized residuals
        resids_scaled_glm = y_joint_scaled - predictions_scaled,
        # Rescaled predictions
        predictions_glm = predictions_scaled *
          attr(in_dat$dat$y_joint_scaled, 'scaled:scale') +
          attr(in_dat$dat$y_joint_scaled, 'scaled:center'),
        # Rescaled residuals
        resids_glm = get(y_joint) - predictions
        )
    }
  # If fit object is LME
  if ('lmerModLmerTest' %in% class(fit$fit)) {
    # Add column with scaled response to model matrix and convert to tibble
    model_matrix <- in_dat$model_matrix %>% as_tibble() %>% 
      mutate(
        y_joint_scaled = in_dat$dat$y_joint_scaled
      )
    dat_with_pred_resid <- in_dat$dat %>% 
      mutate(
        # Standardized predictions and residuals
        predictions_scaled_lme = predict(
          fit$fit,
          newdata = model_matrix,
          # Predict for new subject levels using only fixed effects
          allow.new.levels = T
          ),
        # Standardized residuals
        resids_scaled_lme = y_joint_scaled - predictions_scaled,
        # Rescaled predictions
        predictions_lme = predictions_scaled *
          attr(in_dat$dat$y_joint_scaled, 'scaled:scale') +
          attr(in_dat$dat$y_joint_scaled, 'scaled:center'),
        # Rescaled residuals
        resids_lme = get(y_joint) - predictions
        )
    }
  # Return new data frame with predictions and residuals
  return(dat_with_pred_resid)
}
  
# dependency <- 'MCP3_f - PIP3'
# dependency <- 'CMC5 - MCP5_f'
# dependency <- 'CMC1_f - MCP1'
# Lowest number of subjects present
# dependency <- 'MCP5_f - PIP5'
dependency <- sample(dependency_list, 1)
# for (dependency in dependency_list) {
#   # Selection of joint columns
  x_joint <- filtered_selection_corr_high_iqr_filt %>%
    filter(x_y == dependency) %>%
    distinct(x, y) %>% pull(x)
  y_joint <- filtered_selection_corr_high_iqr_filt %>%
    filter(x_y == dependency) %>%
    distinct(x, y) %>% pull(y)
  # Only highly correlated movements
  rest_list <- filtered_selection_corr_high_iqr_filt %>% 
    filter(x_y == dependency) %>%
    distinct(restimulus) %>% 
    pull(restimulus)
  # Loop over each movement
  rest_select <- sample(rest_list, 1)
  # Lowest number of subjects present
  # rest_select <- 13
  # for (rest_select in rest_list) {
    # Selection of observations after all filtering
    subj_rerep_select <- filtered_selection_corr_high_iqr_filt %>% 
      filter(x_y == dependency & restimulus == rest_select) %>%
      distinct(restimulus, subject, rerepetition) %>% 
      select(subject, rerepetition) %>% 
      # Stratified 5-fold data split (last fold -> test, 4 folds -> train)
      stratified_train_test(n_folds = 5)
    # Collect data from Spark - train
    train_dat <- collect_spark_data(
      restimulus_select = rest_select, 
      x_joint = x_joint,
      y_joint = y_joint,
      subject_rerepetition_select = subj_rerep_select$train,
      spark_tbl_data = dat_flex_anat,
      keep_intercept = T,
      info = paste(
        'Collecting train spark data - Movement:', rest_select, '- Dependency:', dependency
      )
    )
    # Collect data from Spark - test
    test_dat <- collect_spark_data(
      restimulus_select = rest_select,
      x_joint = x_joint,
      y_joint = y_joint,
      subject_rerepetition_select = subj_rerep_select$test,
      spark_tbl_data = dat_flex_anat,
      keep_intercept = T,
      info = paste(
        'Collecting test spark data - Movement:',
        rest_select, '- Dependency:', dependency
        ))
    # Fitting GLM model
    fit_glm <- fit_glm_restimulus_dependency(
      rest_dep = train_dat,
      # Always include - 1 (linear), 2(polynomial)
      keep_variables = c(1, 2),
      fit_intercept = T,
      info = paste(
        'GLM cv fitting - Movement:', rest_select, '- Dependency:', dependency
        )
      )
    # Fitting LME model
    fit_lme <- fit_lme_restimulus_dependency(
      rest_dep = train_dat,
      # Keep variables from GLM fit
      keep_variables = fit_glm$coef_names_1se,
      # True to keep all variables
      # keep_variables = T,
      info = paste(
        'LME fitting - Movement:', rest_select, '- Dependency:', dependency
        )
      )
    #TODO: train_data, test_data add resid and prediction columns
  # }
# }
    
    
fit_glm$fit %>% plot(se.bands=F)
# and between weight and height
fit_glm$fit %>% print()
fit_lme$fit %>% summary()
fit_lme$fit %>% print()
qqnorm(resid(fit_lme$fit, type = 'pearson', scaled = T))
qqline(resid(fit_lme$fit, type = 'pearson', scaled = T))
plot((fit_lme$fit), type=c("p","smooth"), col.line=1)
# anova(fit_lme$fit)
with(fit_glm, {
  a <- coef(fit, s = 'lambda.1se', gamma = 'gamma.1se')[, 1]
  # a <- coef(fit, s = 'lambda.min', gamma = 'gamma.min')[, 1]
  enframe(a) %>% mutate(abs_value = abs(value)) %>%  filter(abs_value > 1e-6)
}) %>% View()

# Takes too long with smooth and qqbands
resid_panel(fit_lme$fit, smoother = F, qqbands = F)

panel_plot_fit(
  # fit = fit_glm,
  fit = fit_lme,
  lmer_subj_vec = train_dat$dat %>% select(subject) %>% distinct() %>% pull(subject),
  grid_wrap_facet = facet_wrap,
  # plot_dat = train_dat,
  plot_dat = test_dat,
  x_joint = x_joint,
  y_joint = y_joint,
  lambda = '1se',
  # lambda = 'min',
  smooth_term_dim = 4
  )
residuals_plot_fit_glm(
  fit = fit_glm,
  # plot_dat = train_dat,
  plot_dat = test_dat,
  x_joint = x_joint,
  y_joint = y_joint,
  lambda = '1se',
  # lambda = 'min',
  smooth_term_dim = 10
  )
residuals_hist_plot_fit_glm(
  fit = fit_glm,
  # plot_dat = train_dat,
  plot_dat = test_dat,
  n_bins = 20,
  lambda = '1se'
  # lambda = 'min'
  )
residuals_qq_plot_fit_glm(
  fit = fit_glm,
  plot_dat = train_dat,
  # plot_dat = test_dat,
  lambda = '1se'
  # lambda = 'min'
  )

# b <- stratified_train_test(
#   subj_rerep =  tibble(
#     subject = c(1, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 5, 5, 5, 5),
#     rerepetition = c(1, 1, 2, 1, 2, 3, 4, 5, 6, 1, 2, 1, 2, 3, 4)
#     ),
#   break_num = 5
#   )
a <- cut(seq(1, nrow(subj_rerep_select)), breaks = 5, labels=F)
a <- a[sample(length(a))]
subj_rerep_select %>%
  mutate(
    foldid = a
  ) %>%
# b <- stratified_train_test(subj_rerep = subj_rerep_select, n_folds = 5)
# b$train %>% add_row(b$test) %>%
  group_by(foldid) %>%
  summarise(
    n_subj = length(unique(subject)),
    n_rerep = length(rerepetition),
    subj_list = paste(unique(subject), collapse = ', ')
  )



b <- sample(subj_rerep_select, 1)
a <- dat_flex_anat %>% select(restimulus, subject, rerepetition, CMC5, MCP5_f) %>%
  mutate(
    subject_rerepetition = paste(subject, rerepetition, sep = '-')
    ) %>%
  # Filter entire dataset using rest_subj_rerep
  filter(
    restimulus == 1,
    subject_rerepetition %in% b
    ) %>% 
  # Drop observations with at least one NA value
  filter_all(all_vars(!is.na(.))) %>%
  select(!subject_rerepetition) %>% 
  collect()
a %>% dim()
c <- RamerDouglasPeucker(
    x = a$CMC5, y = a$MCP5_f, epsilon = 1.0
    ) %>% as_tibble() %>% rename(CMC5 = x, MCP5_f = y) %>% mutate(
      dat_orig = 'RDP',
      restimulus = a$restimulus[1],
      subject = a$subject[1],
      rerepetition = a$rerepetition[1]
    )
c %>% dim()
a %>% mutate(
  dat_orig = 'orig'
) %>% 
  add_row(c) %>% 
  ggplot(aes(x = CMC5, y = MCP5_f)) + 
    geom_point(size = 0.001) +
    facet_grid(. ~ dat_orig) +
  geom_smooth(method = lm, formula = y ~ poly(x, 1), color = 'dodgerblue2') +
  geom_smooth(method = lm, formula = y ~ poly(x, 2), color = 'orange')
    
  
resid(fit$fit)

# TODO
for (dependency in dependency_list) {
  plot_bars_contingency_table(
    contingency_tables_after_corr_filt[[dependency]], 
    title = paste(
      'Contingency table (corrs >= 0.7) - Dependency', dependency,  
      'per movement, subject and rerepetition'
      )
    )
  ggsave(
    filename = paste0(
      'plots/contingency_table_dependency_', gsub(" ", "", dependency, fixed = T),
      '_anatomical_small_range_iqr_filt_after_moderate_corr.png'
    ),
    width = 27, height = 19, units = 'cm', dpi = 320, pointsize = 12)
}

pipeline <- ml_pipeline(sc) %>% 
  ft_dplyr_transformer(
    dplyr_pipe
  )


ml_transform(ml_fit(pipeline, dat_flex), dat_flex) %>% sdf_nrow()

# Descriptive statistics on data
dat_summary <- dat_flex %>%
  select(!c(exercise, subject, laterality, gender, age, height, weight, rerepetition)) %>% 
  # Filter out rest position
  filter(restimulus != 0) %>%
  group_by(restimulus) %>% 
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
dat_summary %>% arrange(restimulus) %>% View()


# Construct dplyr part of the Spark pipe
# dplyr_pipe <-  


# Disconnect from Spark
spark_disconnect(sc)
spark_disconnect_all()

# Load data from s_*_angles, clean and save them in clean_data folder for further
# processing

library(R.matlab)
library(tidyverse)
# Pasting values from web as tribble
library(datapasta)
# Store in parquet file type
library(arrow)

importCleanData <- function(){
  # Dataset contains 77 subjects
  # Originally released as part of three multimodal datasets (Ninapro DB1, DB2, DB5)
  # including uncalibrated kinematic data of respectively 27, 40 and 10 subjects.
  # Exercises included:
  ## DB1 (s1-s27): B(E2) - 2, C(E3) - 3 (wrong numbering, needs -1 for proper labeling)
  ## DB2 (s28-s67): B(E1) - 1, C(E2) - 2 (proper numbering)
  ## DB5 (s68-s77): B(E2) - 1, C(E3) - 2 (proper numbering)
  # Several databases are stitched together, so subject numbering does not coincide
  # with DB9_Index (need to relabel)
  # Hand movements:
  # 40 hand movements plus rest and correspond to exercise B(1) and C(2)
  ## Exercise B(1)
  ### 8 isometric and isotonic hand configurations
  ### 9 basic movements of the wrist
  ### 0 label is rest
  ## Exercise C(2)
  ### 23 grasping and functional movements (everyday objects)
  ### 0 label is rest
  ## DB2 (s28-s67) - Exercise 2 movements (restimulus) labeled from (0)18-40
  ## instead of (0)1-23. Need to subtract 17 (except from rest 0) to correctly label.
  
  # Subject data table (http://ninapro.hevs.ch/data9):
  subject_data <- tibble::tribble(
  ~DB9_Index,    ~Laterality,  ~Gender, ~Age, ~Height, ~Weight,
          1L, "Right Handed",   "Male",  31L,    170L,     75L,
          2L, "Right Handed",   "Male",  27L,    170L,     62L,
          3L, "Right Handed",   "Male",  22L,    180L,     85L,
          4L, "Right Handed",   "Male",  27L,    183L,     95L,
          5L, "Right Handed",   "Male",  27L,    178L,     75L,
          6L, "Right Handed", "Female",  22L,    163L,     48L,
          7L, "Right Handed",   "Male",  28L,    170L,     60L,
          8L, "Right Handed", "Female",  27L,    164L,     54L,
          9L, "Right Handed",   "Male",  23L,    173L,     63L,
         10L, "Right Handed", "Female",  30L,    160L,     60L,
         11L, "Right Handed",   "Male",  28L,    170L,     67L,
         12L, "Right Handed",   "Male",  25L,    185L,     80L,
         13L, "Right Handed",   "Male",  27L,    184L,     85L,
         14L,  "Left Handed", "Female",  29L,    155L,     54L,
         15L, "Right Handed", "Female",  26L,    162L,     60L,
         16L,  "Left Handed",   "Male",  29L,    167L,     67L,
         17L, "Right Handed",   "Male",  30L,    175L,     76L,
         18L, "Right Handed",   "Male",  29L,    178L,     68L,
         19L, "Right Handed",   "Male",  34L,    173L,     82L,
         20L, "Right Handed", "Female",  26L,    165L,     54L,
         21L, "Right Handed",   "Male",  38L,    178L,     73L,
         22L, "Right Handed", "Female",  35L,    168L,     65L,
         23L, "Right Handed",   "Male",  30L,    180L,     65L,
         24L, "Right Handed",   "Male",  26L,    180L,     65L,
         25L, "Right Handed",   "Male",  28L,    180L,     70L,
         26L, "Right Handed",   "Male",  40L,    179L,     66L,
         27L, "Right Handed",   "Male",  28L,    185L,    100L,
         28L, "Right Handed",   "Male",  29L,    187L,     75L,
         29L, "Right Handed",   "Male",  29L,    183L,     75L,
         30L, "Right Handed",   "Male",  31L,    174L,     69L,
         31L,  "Left Handed", "Female",  30L,    154L,     50L,
         32L, "Right Handed",   "Male",  25L,    175L,     70L,
         33L, "Right Handed",   "Male",  35L,    172L,     79L,
         34L, "Right Handed",   "Male",  27L,    187L,     92L,
         35L, "Right Handed",   "Male",  45L,    173L,     73L,
         36L, "Right Handed",   "Male",  23L,    172L,     63L,
         37L, "Right Handed",   "Male",  34L,    173L,     84L,
         38L, "Right Handed", "Female",  32L,    150L,     54L,
         39L, "Right Handed",   "Male",  29L,    184L,     90L,
         40L,  "Left Handed",   "Male",  30L,    182L,     70L,
         41L, "Right Handed", "Female",  30L,    173L,     59L,
         42L, "Right Handed",   "Male",  30L,    169L,     58L,
         43L, "Right Handed",   "Male",  34L,    173L,     76L,
         44L, "Right Handed",   "Male",  29L,    175L,     70L,
         45L, "Right Handed", "Female",  30L,    169L,     90L,
         46L, "Right Handed", "Female",  31L,    158L,     52L,
         47L, "Right Handed", "Female",  26L,    155L,     52L,
         48L, "Right Handed",   "Male",  32L,    170L,     75L,
         49L,  "Left Handed", "Female",  28L,    162L,     54L,
         50L, "Right Handed",   "Male",  25L,    170L,     66L,
         51L, "Right Handed",   "Male",  28L,    170L,     73L,
         52L,  "Left Handed",   "Male",  31L,    168L,     70L,
         53L,  "Left Handed",   "Male",  30L,    186L,     90L,
         54L, "Right Handed",   "Male",  29L,    170L,     65L,
         55L, "Right Handed", "Female",  29L,    160L,     61L,
         56L, "Right Handed",   "Male",  27L,    171L,     64L,
         57L, "Right Handed",   "Male",  30L,    173L,     68L,
         58L, "Right Handed",   "Male",  29L,    185L,     98L,
         59L, "Right Handed",   "Male",  28L,    173L,     72L,
         60L, "Right Handed",   "Male",  25L,    183L,     71L,
         61L, "Right Handed",   "Male",  31L,    192L,     78L,
         62L, "Right Handed", "Female",  24L,    170L,     52L,
         63L, "Right Handed", "Female",  27L,    155L,     44L,
         64L, "Right Handed",   "Male",  34L,    190L,    105L,
         65L, "Right Handed", "Female",  30L,    163L,     62L,
         66L, "Right Handed",   "Male",  31L,    183L,     96L,
         67L, "Right Handed",   "Male",  31L,    173L,     65L,
         68L, "Right Handed",   "Male",  23L,    187L,     67L,
         69L, "Right Handed",   "Male",  28L,    187L,     75L,
         70L, "Right Handed",   "Male",  28L,    170L,     63L,
         71L, "Right Handed", "Female",  22L,    156L,     52L,
         72L, "Right Handed", "Female",  28L,    160L,     61L,
         73L, "Right Handed",   "Male",  24L,    170L,     65L,
         74L, "Right Handed",   "Male",  32L,    172L,     78L,
         75L, "Right Handed",   "Male",  31L,    170L,     74L,
         76L, "Right Handed",   "Male",  34L,    176L,     68L,
         77L, "Right Handed",   "Male",  30L,    173L,     83L
  )
  
  # Loop through all folders
  for (subject in subject_data$DB9_Index) {
  # for (subject in 42:44) {
    # Folder with appropriate name
    folder = paste0('s_', subject, '_angles')
    # List of files in folder (two files in each folder for two exercises)
    data_paths <- list.files(folder, pattern = '*.mat', full.names = T)
    for (path in data_paths) {
      # Read MAT data as an object
      print(paste('Loading dataset:', path))
      dat_raw <- readMat(path, verbose = F)
      print(paste0('Subject: ', subject, ', Exercise: ', dat_raw$exercise[[1]]))
      gc()
      
      # Vector of ordered angles
      order.of.angles <- dat_raw$order.of.angles %>% unlist
      # Create names vector for tibble
      names_sorted <- str_sub(
        order.of.angles, start = str_locate(order.of.angles, ':')[,1]+1
        )[order(as.numeric(str_sub(order.of.angles, end = str_locate(order.of.angles, ':')[,1]-1)))]
      
      # Create tibble with angles and no names
      dat_angles <- as_tibble(
        dat_raw$angles,
        # Silent name repair
        .name_repair = ~ vctrs::vec_as_names(..., repair = "unique", quiet = TRUE)
      )
      gc()
      # print(type(as.integer(pmax(dat_raw$restimulus[,1]-17, 0))))
      # print(type(as.integer(dat_raw$restimulus[,1]-17)))
      # print(type(as.integer(dat_raw$restimulus[,1])))
      # Add angle names 
      colnames(dat_angles) <- names_sorted
      
      # Add exercise and subject columns, movement number and repetition index
      dat_angles <- dat_angles %>% add_column(
        # Relabeling subject
        subject = subject,
        laterality = subject_data$Laterality[subject],
        gender = subject_data$Gender[subject],
        age = subject_data$Age[subject],
        height = subject_data$Height[subject],
        weight = subject_data$Weight[subject],
        exercise = case_when(
          # Relabel exercise for first 27 subjects (-1)
          subject <= 27 ~ dat_raw$exercise[[1]] - 1,
          T ~ dat_raw$exercise[[1]]
        ),
        restimulus = case_when(
          # For subjects 28-67 and exercise 2 relabel restimulus from 18-40 to 1-23
          # and fix -17 to zero with pmax
          (subject %in% 28:67) & (exercise == 2) ~ as.integer(pmax(dat_raw$restimulus[,1]-17, 0)),
          T ~ as.integer(dat_raw$restimulus[,1])
        ),
        rerepetition = dat_raw$rerepetition[,1]
      )
      gc()
      
      # Save clean data as csv in clean_data folder
      # write_csv(
      #   x = dat_angles,
      #   file = paste0('clean_data/s', subject, '_E', dat_angles$exercise[1], '.csv')
      # )
      write_parquet(
        x = dat_angles,
        sink = paste0('clean_data/s', subject, '_E', dat_angles$exercise[1], '.parquet')
      )
      gc()
    }
  }
}

importCleanData()

# # Read single file
# dat_raw <- readMat('s_50_angles/S50_E2_A1.mat', verbose = T)
# str(dat_raw)
# 
# # Matrix of calibrated angles
# dat_raw$angles %>% dim
# dat_raw$angles %>% head
# # Exercise type
# dat_raw$exercise[[1]]
# # Subject number
# dat_raw$subject[[1]]
# # The a-posteriori refined label and repetiton of the movement
# summary(dat_raw$restimulus[,1])
# dat_raw$restimulus[,1] %>% unique()
# summary(dat_raw$rerepetition[,1])
# dat_raw$rerepetition[,1] %>% unique()
# 
# # Vector of ordered angles
# order.of.angles <- dat_raw$order.of.angles %>% unlist
# 
# # Remove index and : from order.of.angles and leave only names 
# str_sub(order.of.angles, start = str_locate(order.of.angles, ':')[,1]+1)
# 
# # Vector for sorting by indices
# as.numeric(str_sub(order.of.angles, end = str_locate(order.of.angles, ':')[,1]-1))
# 
# # Create names vector for tibble
# names_sorted <- str_sub(
#   order.of.angles, start = str_locate(order.of.angles, ':')[,1]+1
#   )[order(as.numeric(str_sub(order.of.angles, end = str_locate(order.of.angles, ':')[,1]-1)))]
# 
# 
# # Create tibble with angles and no names
# dat_angles <- as_tibble(
#   dat_raw$angles,
#   .name_repair = 'universal'
# )
# 
# # Add angle names and exercise and subject columns
# colnames(dat_angles) <- names_sorted
# dat_angles$exercise <- dat_raw$exercise[[1]]
# dat_angles$subject <- dat_raw$subject[[1]]
# 
# # Add movement number and repetiton index
# dat_angles <- dat_angles %>% add_column(
#   restimulus=dat_raw$restimulus[,1],
#   rerepetition=dat_raw$rerepetition[,1]
# )
# View(dat_angles)
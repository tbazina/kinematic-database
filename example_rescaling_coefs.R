library(tidyverse)
library(readr)
# Plotting
library(ggplot2)
library(ggsci)
library(ggmosaic)
library(RColorBrewer)
library(ggdendro)
# Pallettes
library(Polychrome)
# Copy pase tables library
library(clipr)

res_dat <- read_csv("model_results_intra_finger_params9.csv")

res_dat %>% 
  filter(wwmape_glm_test < wwmape_lme_test) %>%
  select(dependency, restimulus, num_coeffs_glm, num_coeffs_lme, starts_with('wwmape'), starts_with('wmae')) %>% 
  View()

# Remove first row and select only models with lowest wwmape on test dataset
# MCP5_f - PIP5, restimulus 6 - models have 5-7 coeffs, one with 5 coeffs is pretty bad, so keeep 6
# MCP3_f - PIP3, rest 3 has 4 coeffs, including height
# MCP3_f - PIP3, rest 20 has 4 coeffs, including poly, exp trans and exp:height_exp interaction
# MCP5_f - PIP5, rest 20 has 4 coeffs, including poly, lin trans and poly:height_exp interaction
# PIP2 - DIP2, rest 12 has 4 coeffs, including poly, lin trans and poly:weght_exp interaction
# PIP2 - DIP2, rest 10 has 4 coeffs, including poly, lin trans and poly:height_exp interaction
# MCP4_f - PIP4, rest 13 has 4 coeffs, including poly, lin trans and poly:height_exp interaction
res_dat_min <- res_dat %>% slice(-1) %>% 
  # Remove dependency since model with 3 coeffs gives only slightly worse errors
  filter(!(dependency == 'CMC5 - MCP5_f' & restimulus == 2 & num_coeffs_lme == 5)) %>%
  # Remove dependency since model with 3 coeffs gives only slightly worse errors
  filter(!(dependency == 'MCP2_f - DIP2' & restimulus == 17 & num_coeffs_lme == 5)) %>%
  # Remove dependency since model with 3 coeffs gives only slightly worse errors
  filter(!(dependency == 'MCP3_f - PIP3' & restimulus == 6 & num_coeffs_lme == 4)) %>%
  # Remove dependency since model with 3 coeffs gives only slightly worse errors
  filter(!(dependency == 'MCP3_f - PIP3' & restimulus == 11 & num_coeffs_lme == 4)) %>%
  # Remove dependency since second best model with 4 coeffs has lin coeff and one less interaction
  filter(!(dependency == 'PIP2 - DIP2' & restimulus == 17 & num_coeffs_lme == 4 & is.na(x_joint_scaled_stand_est))) %>%
  group_by(dependency, restimulus) %>% 
  filter(wwmape_lme_test == min(wwmape_lme_test)) %>% ungroup()
  # filter(wwmape_lme_train == min(wwmape_lme_train)) %>% ungroup()

#### MAnually check models with a lot of parameters
res_dat %>% filter(dependency == 'CMC1_f - IP1' & restimulus == 5) %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>% View()

res_dat_min %>%
  select(where(~sum(!is.na(.x)) > 0)) %>% View()
  # summarise(across(.cols = everything(), list(min = min, max = max, mean = mean, sd = sd)))


# Summary on coeffs
res_dat_min %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>%
  select(ends_with('_est')) %>% 
  rename_with(.fn = ~ gsub('x_joint_', '', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('scaled', 'lin', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('_stand_est', '', .x, fixed = T)) %>%
  relocate(
    lin, poly, exp, height, starts_with('poly:'), starts_with('exp:'), 
    starts_with('lin:')
  ) %>%
  rowwise() %>% 
  summarise(
    across(
      everything(),
      list(
        count = ~ length(.x[!is.na(.x)])
        # min = ~ min(.x, na.rm = T),
        # q1 = ~ quantile(.x, 0.25, na.rm = T),
        # median = ~ median(.x, na.rm = T),
        # q3 = ~ quantile(.x, 0.75, na.rm = T),
        # max = ~ max(.x, na.rm = T)
      )
    )
  ) %>% 
  rowwise() %>% 
  mutate(
    num_coeffs = sum(c_across(everything()))
  ) %>% 
  select(num_coeffs) %>% 
  ungroup() %>% 
  mutate(
    num_coeffs = num_coeffs + 1
  ) %>% 
  summarise(
    across(
      everything(),
      list(
        min = ~ min(.x, na.rm = T),
        q1 = ~ quantile(.x, 0.25, na.rm = T),
        median = ~ median(.x, na.rm = T),
        mean = ~ mean(.x, na.rm = T),
        q3 = ~ quantile(.x, 0.75, na.rm = T),
        max = ~ max(.x, na.rm = T)
      )
    )
  ) %>% View()
  

# Summary statistics on coefficients - boxplots
res_dat_min %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>%
  select(ends_with('_est')) %>% 
  rename_with(.fn = ~ gsub('x_joint_', '', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('scaled', 'lin', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('_stand_est', '', .x, fixed = T)) %>%
  relocate(
    lin, poly, exp, height, starts_with('poly:'), starts_with('exp:'), 
    starts_with('lin:')
  ) %>% 
  # summarise(
  #   across(
  #     everything(),
  #     list(
  #       count = ~ length(.x[!is.na(.x)]),
  #       min = ~ min(.x, na.rm = T),
  #       q1 = ~ quantile(.x, 0.25, na.rm = T),
  #       median = ~ median(.x, na.rm = T),
  #       q3 = ~ quantile(.x, 0.75, na.rm = T),
  #       max = ~ max(.x, na.rm = T)
  #     )
  #   )
  # )
  pivot_longer(cols = everything(), names_to = 'metric') %>% 
  drop_na() %>% 
  group_by(metric) %>%
  mutate(
    levels = length(value[!is.na(value)])
  ) %>% 
  ungroup() %>% 
  arrange(levels) %>% 
  mutate(
    metric = ordered(metric, levels = unique(metric))
  ) %>% 
  ggplot(
    aes(
      x = value,
      y = metric,
      fill = metric,
    ),
  ) +
  # facet_grid(rows = vars(metric), switch = 'y') +
  geom_boxplot(
    width = 0.2, alpha = 1, outlier.size = 0.4, outlier.stroke = 0.1, size = 0.2
  ) + 
  stat_summary(
    fun.data = function(x){
      tibble(
        label = paste0(
          "In ", round(length(x)), " models", ';    ',
          "Min = ", round(min(x), 2), ";    ",
          "Q1 = ", round(quantile(x, 0.25), 2), ";    ",
          "Median = ", round(median(x), 2), ";    ",
          "Q3 = ", round(quantile(x, 0.75), 2), ";    ",
          "Max = ", round(max(x), 2), ";"
          ),
        # x = 1.,
        y = -1.5,
        ) %>% 
        mutate(
          label = gsub('-', '\u2212', label)
        )
    },
    geom = 'text',
    hjust = 0,
    position = position_nudge(y=0.35, x = -0.0),
    size = 2.4
    ) +
  scale_x_continuous(
    # name = expression('Pearson\'s correlation coefficient ('*italic(r)*')'),
    breaks = seq(-2, 8, 0.50),
    labels = ~sub("-", "\u2212", .x)
    # limits = c(-10, NA)
  ) +
  scale_y_discrete(
    # name = 'Dependency'
    # limits = factor(c(0, 0.1)),
    expand = expansion(c(0, 0), c(0.2, 0.7))
  ) +
  scale_fill_cosmic() +
  # ggtitle('Data structure') +
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
    panel.spacing = unit(0.5, 'mm'),
    panel.border = element_rect(size = 0.1),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    # axis.title = element_text(colour="black", face = 'bold', size = 5),
    # axis.title.x = element_text(face="bold", size = 9),
    # axis.title.y = element_text(colour="black", size = 5),
    axis.title = element_blank(),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", face = 'bold', size = 7),
    axis.line = element_line(size=0.1, colour = "black"),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
    plot.title = element_text(size = 7, hjust = 0.5),
    strip.text.y.left = element_text(
      colour="black", face = 'bold', size = 7, angle = 0),
    plot.margin = unit(c(1, 1, 1, 1), 'mm')
    # strip.background = element_blank()
  )
ggsave(
  filename = 'plots/boxplot_coeffs_summary.png',
  width = 18.5, height = 10, units = 'cm', dpi = 320, pointsize = 12)

# Export variable means and std for rescaling coefficients
res_dat_min %>% 
  select(dependency, restimulus, num_coeffs_lme, ends_with('_scale'), ends_with(('_center'))) %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>%
  rename_with(.fn = ~ gsub('x_joint_', '', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('scaled', 'lin', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('y_joint_', 'y_', .x, fixed = T)) %>%
  select(order(colnames(.))) %>% 
  relocate(
    dependency, restimulus, num_coeffs_lme, y_center, y_scale, lin_center,
    lin_scale, poly_center, poly_scale, exp_center, exp_scale, height_center,
    height_scale, starts_with('poly')
    ) %>% 
  arrange(desc(num_coeffs_lme), dependency, restimulus) %>% 
  write_clip()

# Export model information (train, test datasets, GLM and LME number of coefficients and errors)
res_dat_min %>%
  select(
    -ends_with('_scale'), -ends_with(('_center')), -ends_with('_t_val'), 
    -ends_with('_est'), -starts_with('rand_eff'), -starts_with('subj_intercept')) %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>%
  arrange(dependency, restimulus) %>% 
  write_clip()

# Export random effect information
res_dat_min %>%
  select(dependency, restimulus, num_subj_train, subj_intercept_mean, rand_eff_var, rand_eff_ICC) %>% 
  arrange(dependency, restimulus) %>% 
  write_clip()

# Summary statistics on random effects - ICC and variance - plot
res_dat_min %>% select(
  dependency,
  restimulus,
  starts_with('rand_eff'),
  ) %>% 
  rename(
    Variance = rand_eff_var,
    ICC = rand_eff_ICC
  ) %>%
  summarise(
    poor = sum(ICC < 0.5),
    moderate = sum(ICC >= 0.5 & ICC < 0.75),
    good = sum(ICC >= 0.75 & ICC < 0.9),
    excellent = sum(ICC >= 0.9),
  )
# subjects and rerepetitions per dependency-restimulus
res_dat_min %>% select(
  starts_with('rand_eff'),
  ) %>% 
  rename(
    Variance = rand_eff_var,
    ICC = rand_eff_ICC
  ) %>% 
  pivot_longer(cols = everything(), names_to = 'metric') %>% 
  ggplot(
    aes(
      x = value,
      # y = value,
      fill = metric,
    ),
  ) +
  facet_grid(vars(metric), scales = 'free_x', switch = 'y') +
  geom_boxplot(
    width = 0.4, alpha = 1, outlier.size = 0.4, outlier.stroke = 0.1, size = 0.2
  ) + 
  geom_dotplot(
    binaxis = 'x', stackdir = 'up', dotsize = 0.05,
    position = position_nudge(y=-1.0),
    ) +
  stat_summary(
    aes(y=1.),
    fun.data = function(x){
      tibble(
        label = paste0(
          "Min=", round(min(x), 2), "\n",
          "Q1=", round(quantile(x, 0.25), 2), "\n",
          "Median=", round(median(x), 2), "\n",
          "Q3=", round(quantile(x, 0.75), 2), "\n",
          "Max=", round(max(x), 2), "\n"
          ),
        # Print for glm model (don't know why lme doesn't work)
        x = 1.,
        y = 0.,
        )
    },
    geom = 'text',
    hjust = 0,
    position = position_nudge(y=-1.4, x = -0.2),
    size = 2.2
    ) +
  scale_x_continuous(
    # name = expression('Pearson\'s correlation coefficient ('*italic(r)*')'),
    # breaks = seq(-1, 1, 0.50)
    # limits = c(-10, NA)
  ) +
  scale_y_discrete(
    # name = 'Dependency'
    # limits = factor(c(0, 0.1)),
    expand = expansion(c(0, 0), c(0, 0.2))
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
    legend.text = element_text(color="black", size = 7),
    legend.title = element_text(color = 'black', size = 7, vjust = 1),
    panel.background = element_blank(),
    panel.spacing = unit(0.5, 'mm'),
    panel.border = element_rect(size = 0.1),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    # axis.title = element_text(colour="black", face = 'bold', size = 5),
    # axis.title.x = element_text(face="bold", size = 9),
    # axis.title.y = element_text(colour="black", size = 5),
    axis.title = element_blank(),
    axis.text.x = element_text(colour="black", size = 7),
    axis.text.y = element_text(colour="black", face = 'bold', size = 7),
    axis.line = element_line(size=0.1, colour = "black"),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
    plot.title = element_text(size = 7, hjust = 0.5),
    strip.text = element_text(colour="black", face = 'bold', size = 7),
    plot.margin = unit(c(1, 1, 1, 1), 'mm')
    # strip.background3 = element_blank()
  )
ggsave(
  filename = 'plots/boxplot_ICC_variance_rand_eff.png',
  width = 13, height = 4.5, units = 'cm', dpi = 320, pointsize = 12)


# Summary statistics on error metrics - plot
# subjects and rerepetitions per dependency-restimulus
res_dat_min %>% select(
  starts_with('wmae'),
  starts_with('wwmape')
  ) %>% 
  mutate(
    across(starts_with('wwmape'), ~.x * 100)
  ) %>% 
  pivot_longer(cols = everything()) %>% 
  mutate(
    metric = str_split(name, fixed('_')),
    metric = sapply(metric, function(x) x[1]),
    model = str_split(name, fixed('_')),
    model = sapply(model, function(x) x[2]),
    data = str_split(name, fixed('_')),
    data = sapply(data, function(x) x[3]),
    model = case_when(
      model == 'glm' ~ 'GLM',
      model == 'lme' ~ 'LME',
      TRUE ~ model
    )
  ) %>% 
  mutate(
    metric = case_when(
      metric == 'wwmape' ~ 'wwmape (%)',
      metric == 'wmae' ~ 'wmae (°)',
      T ~ metric
    )
  ) %>% 
  mutate(
    metric = toupper(metric),
    data = paste0(
      toupper(str_sub(data, end = 1)), 
      str_sub(data, start = 2, end = -1),
      ' data'
      )
  ) %>% 
  ggplot(
    aes(
      x = value,
      y = model,
      fill = model,
    ),
  ) +
  facet_grid(vars(data), vars(metric), scales = 'free_x') +
  geom_boxplot(
    width = 0.3, alpha = 1, outlier.size = 0.3, outlier.stroke = 0.1, size = 0.2
  ) + 
  stat_summary(
    fun.data = function(x){
      tibble(
        label = paste0(
          "Min=", round(min(x), 1), "\n",
          "Q1=", round(quantile(x, 0.25), 1), "\n",
          "Median=", round(median(x), 1), "\n",
          "Q3=", round(quantile(x, 0.75), 1), "\n",
          "Max=", round(max(x), 1), "\n"
          ),
        # Print for glm model (don't know why lme doesn't work)
        x = 2.,
        y = -3.,
        )
    },
    geom = 'text',
    hjust = 0,
    size = 1.6,
    position = position_nudge(y=-0.15),
    ) +
  stat_summary(
    fun.data = function(x){
      tibble(
        label = paste0(
          "Min=", round(min(x), 1), "\n",
          "Q1=", round(quantile(x, 0.25), 1), "\n",
          "Median=", round(median(x), 1), "\n",
          "Q3=", round(quantile(x, 0.75), 1), "\n",
          "Max=", round(max(x), 1), "\n"
          ),
        # Print for lme model (don't know why glm doesn't work)
        x = 1.,
        y = -3.,
        )
    },
    geom = 'text',
    hjust = 0,
    vjust=0.6,
    size = 1.6,
    ) +
  scale_x_continuous(
    # name = expression('Pearson\'s correlation coefficient ('*italic(r)*')'),
    # breaks = seq(-1, 1, 0.50)
    # limits = c(-10, NA)
    expand = expansion(c(0.02, 0.05), c(0, 0))
  ) +
  scale_y_discrete(
    # name = 'Dependency'
    expand = expansion(c(0., 0.), c(0.4, 0.3))
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
    # axis.title = element_text(colour="black", face = 'bold', size = 5),
    axis.title = element_blank(),
    # axis.title.x = element_text(face="bold", size = 9),
    # axis.title.y = element_text(colour="black", size = 5),
    axis.text.x = element_text(colour="black", size = 5),
    axis.text.y = element_text(colour="black", face = 'bold', size = 5),
    axis.line = element_line(size=0.1, colour = "black"),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
    plot.title = element_text(size = 5, hjust = 0.5),
    strip.text = element_text(colour="black", face = 'bold', size = 5),
    # strip.background = element_blank()
  )
ggsave(
  filename = 'plots/boxplot_error_metric_summary.png',
  width = 14, height = 6.5, units = 'cm', dpi = 320, pointsize = 12)

# Plot error metrics - WWMAPE
res_dat_min %>% select(
  dependency, restimulus, starts_with('wwmape')
  ) %>%
  mutate(
    dep_rest = factor(
      paste(restimulus, dependency, sep = ':'),
      levels = str_sort(paste(restimulus, dependency, sep = ':'), numeric=T)
      ),
    across(starts_with('wwmape'), ~.x*100)
    ) %>% 
  arrange(wwmape_lme_test, dep_rest) %>% 
  pivot_longer(
    cols = !c(dependency, restimulus, dep_rest),
    names_to = 'error_metric',
    values_to = 'error_value'
    ) %>%
  mutate(
    error_metric = str_sub(error_metric, start=8),
    error_metric = str_replace(error_metric, 'glm', 'GLM'),
    error_metric = str_replace(error_metric, 'lme', 'LME'),
    error_metric = str_replace(error_metric, '_', ' '),
    ) %>% 
  ggplot(aes(y = error_metric, x = error_value)) +
  facet_wrap(facets = vars(dep_rest), ncol=7) +
  geom_col(
    orientation='y',
    aes(fill = error_metric),
    width = 0.6, 
    ) +
  geom_text(
    aes(label = paste0(round(error_value, 0), '%')), 
    vjust = 0.5, size=1.5, hjust=-0.1
    ) +
  scale_y_discrete(
    expand = expansion(mult = c(0, 0), add = c(0.6, 0.6)),
  ) +
  scale_x_continuous(
    name = 'WWMAPE (%)',
    expand = expansion(mult = c(0, 0), add = c(0, 0)),
    limits = c(0, 100)
  ) +
  scale_fill_nejm(
  ) +
  # ggtitle('Error metrics') +
  theme_bw() + theme(
    legend.position = 'top',
    # legend.position = c(0.75, 0.03),
    legend.title = element_blank(),
    legend.box = 'horizontal',
    legend.direction = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 1, 0, 'mm'),
    legend.key.size = unit(3, 'mm'),
    legend.text = element_text(colour="black", size = 5),
    panel.background = element_blank(),
    panel.spacing.y = unit(1, 'mm'),
    panel.spacing.x = unit(0, 'mm'),
    axis.title.x = element_text(face="bold", size = 5),
    axis.text.x = element_text(colour="black", size = 5),
    # axis.text.x = element_text(colour="black", size = 5),
    # Remove x axis text and title
    axis.title.y = element_blank(),
    axis.text.y = element_blank(),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5, size = 5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.01),
    strip.text = element_text(
      colour = 'black', size = 6.0, margin = margin(b = 0.5, t = 0.5, unit='mm')
      ),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
    )
ggsave(
  filename = 'plots/error_metrics_wwmape.png',
  width = 14, height = 18, units = 'cm', dpi = 320, pointsize = 12)

# Plot error metrics - WMAE
res_dat_min %>% select(
  dependency, restimulus, starts_with('wmae')
) %>%
  mutate(
    dep_rest = factor(
      paste(restimulus, dependency, sep = ':'),
      levels = str_sort(paste(restimulus, dependency, sep = ':'), numeric=T)
    )
  ) %>% 
  arrange(dep_rest) %>% 
  pivot_longer(
    cols = !c(dependency, restimulus, dep_rest),
    names_to = 'error_metric',
    values_to = 'error_value'
  ) %>%
  mutate(error_metric = str_sub(error_metric, start=6)) %>% 
  mutate(
    error_metric = str_replace(error_metric, 'glm', 'GLM'),
    error_metric = str_replace(error_metric, 'lme', 'LME'),
    error_metric = str_replace(error_metric, '_', ' '),
    ) %>% 
  ggplot(aes(y = error_metric, x = error_value)) +
  facet_wrap(facets = vars(dep_rest), ncol=7) +
  geom_col(
    orientation='y',
    aes(fill = error_metric),
    width = 0.6, 
  ) +
  geom_text(
    aes(label = paste0(round(error_value, 0), '°')), 
    vjust = 0.5, size=1.5, hjust=-0.1
  ) +
  scale_y_discrete(
    expand = expansion(mult = c(0, 0), add = c(0.6, 0.6)),
  ) +
  scale_x_continuous(
    name = 'WMAE (°)',
    expand = expansion(mult = c(0, 0), add = c(0, 0)),
    limits = c(0, 31)
  ) +
  scale_fill_nejm(
  ) +
  theme_bw() + theme(
    legend.position = 'top',
    # legend.position = c(0.75, 0.03),
    legend.title = element_blank(),
    legend.box = 'horizontal',
    legend.direction = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 1, 0, 'mm'),
    legend.key.size = unit(3, 'mm'),
    legend.text = element_text(colour="black", size = 5),
    panel.background = element_blank(),
    panel.spacing.y = unit(1, 'mm'),
    panel.spacing.x = unit(0, 'mm'),
    axis.title.x = element_text(face="bold", size = 5),
    axis.text.x = element_text(colour="black", size = 5),
    # axis.text.x = element_text(colour="black", size = 5),
    # Remove x axis text and title
    axis.title.y = element_blank(),
    axis.text.y = element_blank(),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5, size = 5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.01),
    strip.text = element_text(
      colour = 'black', size = 6.0, margin = margin(b = 0.5, t = 0.5, unit='mm')
    ),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
  )
ggsave(
  filename = 'plots/error_metrics_wmae.png',
  width = 14, height = 18, units = 'cm', dpi = 320, pointsize = 12)

# Plot standardize regression coeffs - only random + poly + linear
res_dat_min %>% 
  select(dependency, restimulus, subj_intercept_mean, ends_with('_est')) %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>% 
  mutate(
    dep_rest = factor(
      paste(restimulus, dependency, sep = ':'),
      levels = str_sort(paste(restimulus, dependency, sep = ':'), numeric=T)
    )
  ) %>% 
  arrange(dep_rest) %>% 
  pivot_longer(
    cols = !c(dependency, restimulus, dep_rest),
    names_to = 'coef_name',
    values_to = 'coef_value'
  ) %>%
  mutate(
    coef_name = str_replace(coef_name, pattern='x_joint_', replacement=''),
    coef_name = str_replace(coef_name, pattern='x_joint_', replacement=''),
    coef_name = str_sub(coef_name, end=-11),
    coef_name = str_replace(coef_name, pattern='scaled', replacement='lin'),
    ) %>% 
  # pull(coef_name) %>% unique()
  # filter(
  #   str_detect(coef_name, 'height') | str_detect(coef_name, 'exp') | str_detect(coef_name, ':')
  # ) %>% 
  # drop_na() %>% 
  filter(
    (
      str_detect(coef_name, 'lin') |  str_detect(coef_name, 'poly') | 
        str_detect(coef_name, 'subj_')
      ) & !str_detect(coef_name, ':')
  ) %>%
  ggplot(aes(y = coef_name, x = coef_value)) +
  facet_wrap(facets = vars(dep_rest), ncol=7) +
  geom_col(
    orientation='y',
    aes(fill = coef_name),
    width = 0.7, 
    na.rm = T,
  ) +
  geom_text(
    aes(label = sub('-', '\u2212', round(coef_value, 2)), x=-1), 
    vjust = 0.5, size=1.6, hjust=0.5, na.rm=T
  ) +
  scale_y_discrete(
    expand = expansion(mult = c(0, 0), add = c(0.6, 0.6)),
  ) +
  scale_x_continuous(
    name = 'Standardized coefficient',
    expand = expansion(mult = c(0, 0), add = c(0, 0)),
    labels = ~sub("-", "\u2212", .x) 
    # limits = c(-10, 10)
  ) +
  scale_fill_nejm(
  ) +
  theme_bw() + theme(
    legend.position = 'top',
    # legend.position = c(0.75, 0.03),
    legend.title = element_blank(),
    legend.box = 'horizontal',
    legend.direction = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 1, 0, 'mm'),
    legend.key.size = unit(3, 'mm'),
    legend.text = element_text(colour="black", size = 5),
    plot.margin = margin(0.5, 0.5, 0.5, 0.5, 'mm'),
    panel.background = element_blank(),
    panel.spacing.y = unit(1, 'mm'),
    panel.spacing.x = unit(0, 'mm'),
    axis.title.x = element_text(face="bold", size = 5),
    axis.text.x = element_text(colour="black", size = 5),
    # axis.text.x = element_text(colour="black", size = 5),
    # Remove x axis text and title
    axis.title.y = element_blank(),
    axis.text.y = element_blank(),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5, size = 5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.01),
    strip.text = element_text(
      colour = 'black', size = 6.0, margin = margin(b = 0.5, t = 0.5, unit='mm')
    ),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines')
  )
ggsave(
  filename = 'plots/standardized_coeffs.png',
  width = 14, height = 18, units = 'cm', dpi = 320, pointsize = 12)

# Plot standardize regression coeffs - only exp, height and interactions
res_dat_min %>% 
  select(dependency, restimulus, subj_intercept_mean, ends_with('_est')) %>% 
  select(where(~sum(!is.na(.x)) > 0)) %>% 
  mutate(
    dependency = sub('-', '\u2212', dependency),
    dep_rest = factor(
      paste(restimulus, dependency, sep = ':'),
      levels = str_sort(paste(restimulus, dependency, sep = ':'), numeric=T)
    )
  ) %>% 
  arrange(dep_rest) %>% 
  pivot_longer(
    cols = !c(dependency, restimulus, dep_rest),
    names_to = 'coef_name',
    values_to = 'coef_value'
  ) %>%
  mutate(
    coef_name = str_replace(coef_name, pattern='x_joint_', replacement=''),
    coef_name = str_replace(coef_name, pattern='x_joint_', replacement=''),
    coef_name = str_sub(coef_name, end=-11),
    coef_name = str_replace(coef_name, pattern='scaled', replacement='lin'),
    ) %>% 
  # pull(coef_name) %>% unique()
  filter(
    str_detect(coef_name, 'height') | str_detect(coef_name, 'exp') | str_detect(coef_name, ':')
  ) %>%
  drop_na() %>%
  ggplot(aes(y = coef_value, x = coef_name)) +
  facet_wrap(
    facets = vars(dep_rest), labeller = label_value, scales = 'free_x', ncol=11,
    strip.position = "left") +
  geom_col(
    aes(fill = coef_name),
    width = 0.3, 
    na.rm = T,
  ) +
  geom_text(
    aes(label = sub('-', '\u2212', round(coef_value, 2))), 
    vjust=0.5, size=1.5, hjust=-0.1, na.rm=T, angle=90
  ) +
  scale_x_discrete(
    expand = expansion(mult = c(0, 0), add = c(0.6, 0.6)),
  ) +
  scale_y_continuous(
    name = 'Standardized coefficient',
    expand = expansion(mult = c(0, 0), add = c(0, 0)),
    limits = c(-1, 8),
    breaks = -1:8,
    labels = ~sub("-", "\u2212", .x) 
  ) +
  scale_fill_nejm(
  ) +
  theme_bw() + theme(
    legend.position = 'top',
    # legend.position = c(0.75, 0.03),
    legend.title = element_blank(),
    legend.box = 'horizontal',
    legend.direction = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 1, 0, 'mm'),
    legend.key.size = unit(2, 'mm'),
    legend.key.spacing = unit(0, 'mm'),
    legend.text = element_text(colour="black", size = 5),
    panel.background = element_blank(),
    panel.spacing.y = unit(1, 'mm'),
    panel.spacing.x = unit(0, 'mm'),
    axis.title.y = element_text(face="bold", size = 5),
    axis.text.y = element_text(colour="black", size = 5),
    # axis.text.x = element_text(colour="black", size = 5),
    # Remove x axis text and title
    axis.title.x = element_blank(),
    axis.text.x = element_blank(),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5, size = 5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.01),
    strip.text = element_text(
      colour = 'black', size = 5, hjust = 0,
      margin = margin(b = 0.5, t = 0.5, unit='mm')
    ),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
    axis.ticks.x = element_blank()
  )
ggsave(
  filename = 'plots/standardized_coeffs_interactions.png',
  width = 14, height = 3, units = 'cm', dpi = 320, pointsize = 12)

# Clustering - compute distance matrix
res_dat_min_dist <- res_dat_min %>% 
  select(dependency, restimulus, subj_intercept_mean, ends_with('_est')) %>% 
  select(where(~sum(!is.na(.x)) > 0))  %>%
  mutate(
    dep_rest = factor(
      paste(restimulus, dependency, sep = ':'),
      levels = str_sort(paste(restimulus, dependency, sep = ':'), numeric=T)
    )
  ) %>% 
  select(-dependency, -restimulus) %>% 
  arrange(dep_rest) %>% 
  # Replace all NA with 0
  replace(is.na(.), 0) %>% 
  relocate(dep_rest)

dist_coeff <- as.matrix(res_dat_min_dist[,-1])
rownames(dist_coeff) <- res_dat_min_dist %>% pull(dep_rest)
dist_mat <- dist(dist_coeff, method = 'euclidean')

# Cluster data
coeff_clust <- hclust(dist_mat, method = 'complete')
dendr_dat <- coeff_clust %>% 
  as.dendrogram() %>%
  dendro_data(type = 'rectangle')

# Color the similar groups
# 30 clusters exactly at distance 0.52
num_clust <- 30
# color_pallette <- createPalette(
#   num_clust, c("#010101", "#ff0000"), range = c(10, 80), M=100000
#   )
# Use Glasbey palette, but remove white color
color_pallette <- glasbey.colors(n = num_clust+1)[-1]
color_groups <- cutree(coeff_clust, k = num_clust)
color_groups <- ordered(
  color_groups[order(match(color_groups %>% names(), label(dendr_dat)$label))]
  )
# Convert color groups to roman
levels(color_groups) <- as.roman(levels(color_groups))
# Add roman clusters to the labels in the plot
dendr_dat$labels <- label(dendr_dat) %>% 
  mutate(
    label = case_when(
      label == names(color_groups) ~ paste(label, color_groups, sep = ':'),
      .default = label
    )
  )
cutree(coeff_clust, h=0.52) %>% max()

color_groups

# Count number of same movements in the same cluster
dendr_dat %>% 
  label() %>% 
  select(label) %>% 
  mutate(
    str_splt = str_split(label, ':'),
    restimulus = sapply(str_splt, function(x) x[1]) %>% as.numeric,
    dependency = sapply(str_splt, function(x) x[2]),
    cluster = sapply(str_splt, function(x) x[3]),
  ) %>%
  select(-str_splt) %>%
  arrange(cluster, label) %>% 
  View()
  # pull(restimulus) %>% 
  # table
  group_by(cluster) %>% 
  reframe(
    freq_tbl = table(restimulus) %>% as.data.frame(),
    restimulus = sapply(freq_tbl, function(x) names(x)),
    freq = sapply(freq_tbl, function(x) x[1]),
  )
  rename(restimulus = 'restimulus$restimulus', freq = 'restimulus$Freq')
View()


# Plot clusters
dendr_dat %>% segment() %>% 
  ggplot() +
  geom_segment(
    aes(x = x, y = y, xend = xend, yend = yend),
    linewidth = 0.2
    ) +
  geom_hline(
    yintercept = 0.52, linewidth = 0.3, linetype = 'dashed', color = 'orangered3'
    ) +
  geom_text(
    data = label(dendr_dat),
    aes(x = x, y = y, label = label, color=color_groups), 
    vjust = 0.5, size = 1.5, hjust = 1.1,
    key_glyph='rect'
    ) +
  coord_flip() + 
  scale_color_manual(
    name = 'Cluster',
    values = unname(color_pallette)
  ) +
  scale_x_continuous(
    expand = expansion(mult = c(0, 0), add = c(1., 1.)),
  ) +
  scale_y_continuous(
    name='Euclidean distance',
    breaks = seq(0, 8, 0.2),
    limits = c(0.0, 2.35),
    expand = expansion(mult = c(0, 0), add = c(0.4, 0.0)),
  ) +
  theme_dendro() +
  guides(color = guide_legend(title.position = 'top')) +
  theme(
    # legend.position = 'top',
    legend.position = c(0.75, 0.435),
    legend.title = element_text(colour="black", size = 7),
    legend.box = 'horizontal',
    legend.direction = 'horizontal',
    legend.box.spacing = unit(0, 'mm'),
    legend.spacing = unit(2, 'mm'),
    legend.margin = margin(0, 0, 1, 0, 'mm'),
    legend.key.size = unit(3, 'mm'),
    legend.text = element_text(colour="black", size = 7),
    axis.title.x = element_text(colour='black', face="bold", size = 7),
    axis.text.x = element_text(colour="black", size = 7),
    axis.line.x = element_line(size=0.2, colour = "black"),
    axis.ticks.x = element_line(size = 0.1),
    axis.ticks.length = unit(0.1, 'lines'),
  )
ggsave(
  filename = 'plots/hierarchical_clusters_coeffs.png',
  width = 14, height = 18, units = 'cm', dpi = 320, pointsize = 12)

# Plot clustered standardized coeffs with error bars representing min and max cluster value
clustered_dependencies <- color_groups %>% as_tibble(
  rownames='dep_rest') %>%
  rename(cluster = value) %>% 
  mutate(
    dep_rest = str_split(dep_rest, fixed(':')),
    dependency = sapply(dep_rest, function(x) x[2]),
    restimulus = sapply(dep_rest, function(x) as.numeric(x[1]))
  ) %>% 
  select(-dep_rest)
res_dat_min %>% 
  full_join(clustered_dependencies, by = c('dependency', 'restimulus')) %>%
  select(dependency, restimulus, cluster, subj_intercept_mean, ends_with('_est')) %>% 
  # Remove two dependencies destroying readability
  # filter(
  #   !(dependency == 'MCP3_f - PIP3' & restimulus == 20) &
  #   !(dependency == 'MCP5_f - PIP5' & restimulus == 6)
  #     ) %>%
  # Keep two dependencies destroying readability as a separate plot
  filter(
    (dependency == 'MCP3_f - PIP3' & restimulus == 20) |
    (dependency == 'MCP5_f - PIP5' & restimulus == 6)
      ) %>%
  select(where(~sum(!is.na(.x)) > 0)) %>%
  rename_with(.fn = ~ gsub('x_joint_', '', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('_stand_est', '', .x, fixed = T)) %>%
  rename_with(.fn = ~ gsub('scaled', 'lin', .x, fixed = T)) %>%
  rename(intercept = subj_intercept_mean) %>% 
  mutate(
    dep_rest = factor(
      paste(restimulus, dependency, sep = ':'),
      levels = str_sort(paste(restimulus, dependency, sep = ':'), numeric=T)
    )
  ) %>% 
  arrange(dep_rest) %>% 
  group_by(cluster) %>%
  mutate(
    cluster_full = paste0(dep_rest, collapse = ', ')
  ) %>%
  pivot_longer(
    cols = !c(dependency, restimulus, dep_rest, cluster, cluster_full),
    names_to = 'coef_name',
    values_to = 'coef_value'
  ) %>% 
  drop_na() %>% 
  group_by(cluster, coef_name) %>% 
  mutate(
    across(
      .cols = c(coef_value), 
      list(
        min = ~min(.x, na.rm = T),
        max = ~max(.x, na.rm = T),
        mean = ~mean(.x, na.rm = T)
        )
      )
  ) %>% 
  ungroup() %>% 
  select(-c(dependency, restimulus, dep_rest, coef_value)) %>% 
  distinct() %>% 
  ggplot(aes(y = coef_name, x = coef_value_mean)) +
  facet_wrap(
    facets = vars(cluster), ncol=6, scales = 'fixed', 
    labeller = label_both
    ) +
  geom_col(
    orientation='y',
    aes(fill = coef_name),
    width = 0.8, 
    na.rm = T,
  ) +
  geom_errorbar(
    aes(
      xmin = coef_value_min,
      xmax = coef_value_max,
    ),
    linewidth = 0.2,
    width = 0.7,
  ) +
  # geom_text(
  #   aes(label = sub('-', '\u2212', round(coef_value_mean, 2)), x=-1.9),
  #   vjust = 0.5, size=1.8, hjust=0.5, na.rm=T
  # ) +
  geom_text(
    aes(label = sub('-', '\u2212', round(coef_value_mean, 2)), x=-1.0),
    vjust = 0.5, size=1.6, hjust=0.5, na.rm=T
  ) +
  scale_y_discrete(
    expand = expansion(mult = c(0, 0), add = c(0.3, 0.3)),
  ) +
  scale_x_continuous(
    name = 'Standardized coefficient',
    expand = expansion(mult = c(0, 0), add = c(0.6, 0)),
    labels = ~sub("-", "\u2212", .x) 
    # limits = c(-10, 10)
  ) +
  # scale_fill_nejm(
  #   name = 'Coefficient',
  # ) +
  scale_fill_cosmic(
    name = 'Coefficient',
  ) +
  theme_bw() + theme(
    legend.position = 'bottom',
    # legend.position = 'top',
    legend.title = element_text(
      colour="black", size = 5, margin = margin(0, 2, 0, 0, 'mm')
      ),
    legend.box = 'horizontal',
    legend.direction = 'horizontal',
    legend.box.spacing = unit(0.5, 'mm'),
    legend.spacing.y = unit(0, 'mm'),
    legend.spacing.x = unit(0, 'mm'),
    legend.margin = margin(0, 0, 0, 0, 'mm'),
    legend.key.spacing = unit(0, 'mm'),
    legend.key.size = unit(2, 'mm'),
    legend.text = element_text(
      colour="black", size = 5, margin = margin(0, 0.5, 0, 0, 'mm')
      ),
    plot.margin = margin(0.5, 0.5, 0.5, 0.5, 'mm'),
    panel.background = element_blank(),
    panel.spacing.y = unit(1, 'mm'),
    panel.spacing.x = unit(0, 'mm'),
    axis.title.x = element_text(face="bold", size = 5),
    axis.text.x = element_text(colour="black", size = 5),
    # Remove x axis text and title
    axis.title.y = element_blank(),
    axis.text.y = element_blank(),
    axis.line = element_line(size=0.1, colour = "black"),
    plot.title = element_text(hjust = 0.5, size = 5),
    panel.grid = element_line(colour = 'grey', size = 0.1),
    panel.border = element_rect(size = 0.1),
    strip.background = element_rect(size = 0.01),
    strip.text = element_text(
      colour = 'black', size = 6.0, margin = margin(b = 0.5, t = 0.5, unit='mm')
    ),
    axis.ticks = element_line(size = 0.1),
    axis.ticks.y = element_blank(),
    axis.ticks.length = unit(0.1, 'lines')
  )
# ggsave(
#   filename = 'plots/standardized_coeffs_clust.png',
#   width = 18, height = 10, units = 'cm', dpi = 320, pointsize = 12)
ggsave(
  filename = 'plots/standardized_coeffs_clust_interact.png',
  width = 7.5, height = 3.5, units = 'cm', dpi = 320, pointsize = 12)


a = tibble(
  y = 21:30,
  x1 = sin(1:10),
  x2 = (2:11)^2,
  x3 = cos(1:10)
) %>% mutate(
  y = scale(y)
)

b <- model.matrix(
  as.formula(y ~ .*.),
  data=a
)

b <- scale(
  b, center = T, scale = T
  )

b[,1] = 1
b

x = lm(a$y ~ . + 0, as.data.frame(b[,-c(1, (5:7))]))
summary(x)

predict(x) * attr(a$y, 'scaled:scale') + attr(a$y, 'scaled:center')

c = coef(x)['x1'] * attr(a$y, 'scaled:scale') / attr(b, 'scaled:scale')['x1']
d = coef(x)['x2'] * attr(a$y, 'scaled:scale') / attr(b, 'scaled:scale')['x2']
e = coef(x)['x3'] * attr(a$y, 'scaled:scale') / attr(b, 'scaled:scale')['x3']
# f = coef(x)["`(Intercept)`"] * attr(a$y, 'scaled:scale')
g = attr(a$y, 'scaled:center') -
  coef(x)['x1'] * attr(a$y, 'scaled:scale') / attr(b, 'scaled:scale')['x1'] * attr(b, 'scaled:center')['x1'] -
  coef(x)['x2'] * attr(a$y, 'scaled:scale') / attr(b, 'scaled:scale')['x2'] * attr(b, 'scaled:center')['x2'] -
  coef(x)['x3'] * attr(a$y, 'scaled:scale') / attr(b, 'scaled:scale')['x3'] * attr(b, 'scaled:center')['x3']

a$x1 * c + a$x2 * d + a$x3 * e + g

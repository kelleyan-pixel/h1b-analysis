# 305c project

library(dplyr)
library(nanoparquet)
library(readxl)
library(ggplot2)
library(ggridges)
# Read the file. df <- read_parquet("final_cleaned.parquet")
#final_cleaned <- nanoparquet::read_parquet("final_cleaned.parquet")


# (1) FY18 data, model

FY18 <- read_excel("FY2018.xlsx")

FY18_Clean <- FY18 %>%
  filter(CASE_STATUS == "CERTIFIED",
         PW_UNIT_OF_PAY == "Year",
         WAGE_UNIT_OF_PAY == "Year",
         VISA_CLASS == "H-1B") %>%
  select(DECISION_DATE, EMPLOYER_NAME, SOC_NAME, 
         PREVAILING_WAGE, PW_WAGE_LEVEL, PW_SOURCE, 
         WAGE_RATE_OF_PAY_FROM) %>%
  mutate(Wage_Ratio = WAGE_RATE_OF_PAY_FROM/PREVAILING_WAGE) %>%
  mutate(Source_Group = ifelse(PW_SOURCE == "OES", "OES", "Other")) %>%
  filter(Wage_Ratio < 10)  # Ignore ratios over 1000% for the plot
  

# Calculate means 
mu <- FY18_Clean %>%
  group_by(Source_Group) %>%
  summarize(grp_mean = mean(Wage_Ratio, na.rm = TRUE))

# Reorder colors to make plot easier to look at
FY18_Clean$Source_Group <- 
  factor(FY18_Clean$Source_Group, levels = c("Other", "OES"))


ggplot(FY18_Clean, aes(x = Wage_Ratio, fill = Source_Group)) +
  geom_density(alpha = 0.4) +
  geom_vline(data = mu, aes(xintercept = grp_mean, color = Source_Group),
             linetype = "dashed", size = 1) +
  coord_cartesian(xlim = c(0.95, 1.3)) +
  scale_fill_manual(
    values = c("OES" = "#F8766D", "Other" = "#00BFC4"),
    name = "Data Source"
  ) +
  scale_color_manual(
    values = c("OES" = "#F8766D", "Other" = "#00BFC4"),
    name = "Data Source",
    labels = c("OES" = "OES (mean)", "Other" = "Other (mean)")  # <-- key change
  ) +
  guides(
    fill = guide_legend(order = 1),
    color = guide_legend(
      order = 2,
      override.aes = list(linetype = "dashed", linewidth = 1)  
    )
  ) +
  labs(title = "Distribution of Wage Ratios", 
       x = "Wage Ratio", y = "Density") +
  theme_minimal()






# (2) FY26Q1 Data: looking for difference between OES and other source of PW

FY26Q1 <- read_excel("FY26_Q1.xlsx")

  FY26Q1_Clean <- FY26Q1 %>%
  filter(CASE_STATUS == "Certified",
         WAGE_UNIT_OF_PAY == "Year",
         VISA_CLASS == "H-1B" ) %>%
  select(CASE_NUMBER, DECISION_DATE,EMPLOYER_NAME, SOC_CODE, 
         PW_OTHER_SOURCE, PREVAILING_WAGE, PW_SURVEY_PUBLISHER,
         PW_WAGE_LEVEL,WAGE_RATE_OF_PAY_FROM) %>%
  mutate(Wage_Ratio = WAGE_RATE_OF_PAY_FROM/PREVAILING_WAGE) %>%
  mutate(Source_Group = ifelse(is.na(PW_OTHER_SOURCE), "OES", "Other")) %>%
    filter(Wage_Ratio < 10)  # Ignore ratios over 1000% for the plot
  
  # Calculate means 
  mu_26 <- FY26Q1_Clean %>%
    group_by(Source_Group) %>%
    summarize(grp_mean = mean(Wage_Ratio, na.rm = TRUE))
  
  ggplot(FY26Q1_Clean, aes(x = Wage_Ratio, fill = Source_Group)) +
    geom_density(alpha = 0.4) +
    geom_vline(data = mu_26, aes(xintercept = grp_mean, color = Source_Group),
               linetype = "dashed", linewidth = 1) +
    coord_cartesian(xlim = c(0.95, 1.5)) +
    scale_fill_manual(
      values = c("OES" = "#F8766D", "Other" = "#00BFC4"),
      name = "Data Source"
    ) +
    scale_color_manual(
      values = c("OES" = "#F8766D", "Other" = "#00BFC4"),
      name = "Data Source",
      labels = c("OES" = "OES (mean)", "Other" = "Other (mean)")  # <-- key change
    ) +
    guides(
      fill = guide_legend(order = 1),
      color = guide_legend(
        order = 2,
        override.aes = list(linetype = "dashed", linewidth = 1)  
      )
    ) +
    labs(title = "Distribution of Wage Ratios", 
         x = "Wage Ratio", y = "Density") +
    theme_minimal()
  

  
  
  
  
  
#(3) Prevailing wage 26 data

PW26Q1 <- read_excel("PW26Q1.xlsx")
PW26Q1_Clean <- PW26Q1 %>%
  filter(VISA_CLASS == "H-1B")
  
mu_PY26Q1 <- PY26Q1_Clean %>%
  group_by(COVERED_BY_ACWIA) %>%
  summarize(grp_mean = mean(Wage_Ratio, na.rm = TRUE))

# Reorder colors to make plot easier to look at
PW26Q1_Clean$COVERED_BY_ACWIA <- 
  factor(PW26Q1_Clean$COVERED_BY_ACWIA, levels = c("N", "Y"))


ggplot(PW26Q1_Clean, aes(x = Wage_Ratio, fill = COVERED_BY_ACWIA)) +
  geom_density(alpha = 0.4) +
  geom_vline(data = mu_PW26Q1, aes(xintercept = grp_mean, color = 
            COVERED_BY_ACWIA),linetype = "dashed", size = 1) +
  coord_cartesian(xlim = c(0.95, 1.3)) +
  scale_fill_manual(
    values = c("N" = "#F8766D", "Y" = "#00BFC4"),
    name = "Data Source"
  ) +
  scale_color_manual(
    values = c("N" = "#F8766D", "Y" = "#00BFC4"),
    name = "Data Source",
    labels = c("N" = "All Industries (mean)", "Y" = "ACWIA (mean)")  # <-- key change
  ) +
  guides(
    fill = guide_legend(order = 1),
    color = guide_legend(
      order = 2,
      override.aes = list(linetype = "dashed", linewidth = 1)  
    )
  ) +
  labs(title = "Distribution of Wage Ratios", 
       x = "Wage Ratio", y = "Density") +
  theme_minimal()





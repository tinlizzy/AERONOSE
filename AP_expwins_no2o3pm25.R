### Creating air pollution exposure windows for pm25, no2, o3 using intervalaverage package 
# Author: Tara Jenson
# Created: 11/20/2023
# Last Edited: 12/1/2023

library(tidyverse)
library(intervalaverage)
library(data.table)
library(anytime)
library(ggplot2)

setwd("/insert/working/dir/here")

# 1) import & prep exposure window data 
# 1a) import no2/o3/pm25 data subsetted to only those with 7-yr exp windows, and limited to only rows included in the exp win
died_map_no2_o3_7yrwins_only <- read.csv("./died_map_no2_o3_7yrwins_only.csv")
head(died_map_no2_o3_7yrwins_only)

died_map_pm25_7yrwins_only <- read.csv("./died_maponly_pm25_7ywins_only.csv")
head(died_map_pm25_7yrwins_only)

# 1b) keep only columns we need: start, end, measures, and projid
died_map_no2_o3_7yrwins_only_minvars <- died_map_no2_o3_7yrwins_only %>% 
  select(one_of(c("projid","no2_start","no2_end","no2_st","o3_start","o3_end","o3_st")))
head(died_map_no2_o3_7yrwins_only_minvars)
died_map_no2_o3_7yrwins_only_minvars <- died_map_no2_o3_7yrwins_only_minvars %>% 
  mutate(no2_start=as.IDate(no2_start)) # convert date cols to IDate as per intervalaverage reqs
died_map_no2_o3_7yrwins_only_minvars <- died_map_no2_o3_7yrwins_only_minvars %>% 
  mutate(no2_end=as.IDate(no2_end)) # convert date cols to IDate as per intervalaverage reqs
died_map_no2_o3_7yrwins_only_minvars <- died_map_no2_o3_7yrwins_only_minvars %>% 
  mutate(o3_start=as.IDate(o3_start)) # convert date cols to IDate as per intervalaverage reqs
died_map_no2_o3_7yrwins_only_minvars <- died_map_no2_o3_7yrwins_only_minvars %>% 
  mutate(o3_end=as.IDate(o3_end)) # convert date cols to IDate as per intervalaverage reqs
died_map_no2_o3_7yrwins_only_minvarsDT = as.data.table(died_map_no2_o3_7yrwins_only_minvars) # THIS IS THE WAY TO CONVERT TO DATA.TABLE!

died_map_pm25_7yrwins_only_minvars <- died_map_pm25_7yrwins_only %>% 
  select(one_of(c("projid","pm25_start","pm25_end","pm25_st")))
head(died_map_pm25_7yrwins_only_minvars)
died_map_pm25_7yrwins_only_minvars <- died_map_pm25_7yrwins_only_minvars %>% 
  mutate(pm25_start=as.IDate(pm25_start)) # convert date cols to IDate as per intervalaverage reqs
died_map_pm25_7yrwins_only_minvars <- died_map_pm25_7yrwins_only_minvars %>% 
  mutate(pm25_end=as.IDate(pm25_end)) # convert date cols to IDate as per intervalaverage reqs
died_map_pm25_7yrwins_only_minvarsDT = as.data.table(died_map_pm25_7yrwins_only_minvars) # THIS IS THE WAY TO CONVERT TO DATA.TABLE!


# 2) import and prep table of averaging periods 
# 2a) import table of averaging periods
avg_periods_no2 <- read.csv("./avg_periods_no2.csv")
head(avg_periods_no2)
avg_periods_o3 <- read.csv("./avg_periods_o3.csv")
head(avg_periods_o3)
avg_periods_pm25 <- read.csv("./avg_periods_pm25.csv")
head(avg_periods_pm25)

avg_periods_no2_IDate <- avg_periods_no2 %>% 
  mutate(no2_start=as.IDate(no2_start)) # convert date cols to IDate as per intervalaverage reqs
avg_periods_no2_IDate <- avg_periods_no2_IDate %>% 
  mutate(no2_end=as.IDate(no2_end)) # convert date cols to IDate as per intervalaverage reqs
head(avg_periods_no2_IDate)

avg_periods_o3_IDate <- avg_periods_o3 %>% 
  mutate(o3_start=as.IDate(o3_start)) # convert date cols to IDate as per intervalaverage reqs
avg_periods_o3_IDate <- avg_periods_o3_IDate %>% 
  mutate(o3_end=as.IDate(o3_end)) # convert date cols to IDate as per intervalaverage reqs
head(avg_periods_o3_IDate)

avg_periods_pm25_IDate <- avg_periods_pm25 %>% 
  mutate(pm25_start=as.IDate(pm25_start)) # convert date cols to IDate as per intervalaverage reqs
avg_periods_pm25_IDate <- avg_periods_pm25_IDate %>% 
  mutate(pm25_end=as.IDate(pm25_end)) # convert date cols to IDate as per intervalaverage reqs
head(avg_periods_pm25_IDate)

avg_periods_no2_IDateDT = as.data.table(avg_periods_no2_IDate) # THIS IS THE WAY TO CONVERT TO DATA.TABLE!
avg_periods_o3_IDateDT = as.data.table(avg_periods_o3_IDate) # THIS IS THE WAY TO CONVERT TO DATA.TABLE!
avg_periods_pm25_IDateDT = as.data.table(avg_periods_pm25_IDate) # THIS IS THE WAY TO CONVERT TO DATA.TABLE!

# 2b) expand the avg periods files to replicate exp periods for each projid using CJ.dt
# --> first grab & create table of the unique projids
no2o3_data_unique_projids <- data.table(projid=unique(died_map_no2_o3_7yrwins_only_minvarsDT$projid)) # grabs unique ids
dim(no2o3_data_unique_projids) # 1050 unique individuals
pm25_data_unique_projids <- data.table(projid=unique(died_map_pm25_7yrwins_only_minvarsDT$projid)) # grabs unique ids
dim(pm25_data_unique_projids) 

# no2
avg_periods_no2_IDateDT_w_projids <- CJ.dt(avg_periods_no2_IDateDT, no2o3_data_unique_projids) # expands avg periods by projid
avg_periods_no2_IDateDT_w_projids_dt = as.data.table(avg_periods_no2_IDateDT_w_projids) # reconvert to DT
head(avg_periods_no2_IDateDT_w_projids_dt)
dim(avg_periods_no2_IDateDT_w_projids_dt) # 23100 --> 1050 ids * 22 averaging periods

#o2
avg_periods_o3_IDateDT_w_projids <- CJ.dt(avg_periods_o3_IDateDT, no2o3_data_unique_projids) # expands avg periods by projid
avg_periods_o3_IDateDT_w_projids_dt = as.data.table(avg_periods_o3_IDateDT_w_projids) # reconvert to DT
head(avg_periods_o3_IDateDT_w_projids_dt)
dim(avg_periods_o3_IDateDT_w_projids_dt) # 23100 --> 1050 ids * 22 averaging periods

#pm25

avg_periods_pm25_IDateDT_w_projids <- CJ.dt(avg_periods_pm25_IDateDT, pm25_data_unique_projids) # expands avg periods by projid
avg_periods_pm25_IDateDT_w_projids_dt = as.data.table(avg_periods_pm25_IDateDT_w_projids) # reconvert to DT
head(avg_periods_pm25_IDateDT_w_projids_dt)
dim(avg_periods_pm25_IDateDT_w_projids_dt)

# 3) run intervalaverage by group/id
# no2 
averaged_no2_byProjid <- intervalaverage(x=died_map_no2_o3_7yrwins_only_minvarsDT,
                                          y=avg_periods_no2_IDateDT_w_projids_dt,
                                          interval_vars=c("no2_start","no2_end"),
                                          value_vars=c("no2_st"),
                                          group_vars="projid")[, list(projid, no2_start, no2_end, no2_st)]
                                          # note: leave required_percentage arg as default 100 
                                          #   as we only want one window per person not any leak-over windows
head(averaged_no2_byProjid)

#o3
averaged_o3_byProjid <- intervalaverage(x=died_map_no2_o3_7yrwins_only_minvarsDT,
                                         y=avg_periods_o3_IDateDT_w_projids_dt,
                                         interval_vars=c("o3_start","o3_end"),
                                         value_vars=c("o3_st"),
                                         group_vars="projid")[, list(projid, o3_start, o3_end, o3_st)]
                                        # note: leave required_percentage arg as default 100 
                                        #   as we only want one window per person not any leak-over windows
head(averaged_o3_byProjid)

averaged_pm25_byProjid <- intervalaverage(x=died_map_pm25_7yrwins_only_minvarsDT,
                                          y=avg_periods_pm25_IDateDT_w_projids_dt,
                                          interval_vars=c("pm25_start","pm25_end"),
                                          value_vars=c("pm25_st"),
                                          group_vars="projid")[, list(projid, pm25_start, pm25_end, pm25_st)]
# note: leave required_percentage arg as default 100 
#   as we only want one window per person not any leak-over windows
head(averaged_pm25_byProjid) 

# 4) subset the intervalaverage results to non-NA for the AP measures (one valid calc for each person)
averaged_no2_byProjid_onlyValues <- averaged_no2_byProjid %>% 
  filter(!is.na(no2_st))
dim(averaged_no2_byProjid_onlyValues)

averaged_o3_byProjid_onlyValues <- averaged_o3_byProjid %>% 
  filter(!is.na(o3_st))
dim(averaged_o3_byProjid_onlyValues) 

averaged_pm25_byProjid_onlyValues <- averaged_pm25_byProjid %>% 
  filter(!is.na(pm25_st))
dim(averaged_pm25_byProjid_onlyValues) 


# 5) boxplot the results
head(averaged_no2_byProjid_onlyValues)
head(averaged_o3_byProjid_onlyValues)
head(averaged_pm25_byProjid_onlyValues)

# 5a) create new var pulling year from the no2_end/o3_end (which will be year of death)
averaged_no2_byProjid_onlyValues <- averaged_no2_byProjid_onlyValues %>% 
  mutate(Year = (substr(no2_end, 1,4))) 
head(averaged_no2_byProjid_onlyValues)

averaged_o3_byProjid_onlyValues <- averaged_o3_byProjid_onlyValues %>% 
  mutate(Year = (substr(o3_end, 1,4))) 
head(averaged_o3_byProjid_onlyValues)

averaged_pm25_byProjid_onlyValues <- averaged_pm25_byProjid_onlyValues %>% 
  mutate(Year = (substr(pm25_end, 1,4))) 
head(averaged_pm25_byProjid_onlyValues)

# 5b) boxplot of pm avgs by year
base_pm25 <- ggplot(averaged_pm25_byProjid_onlyValues, aes(x = Year, y = pm25_st))
base_pm25 + geom_boxplot()+ labs(title = "Figure 2. Seven-Year Average PM2.5 from Death, by Year of Death. \nMemory and Aging Project (N=695)", 
                                 y = "7-Year PM2.5 Average from Death (μg/m3)", x = "Year of Death")

base_no2 <- ggplot(averaged_no2_byProjid_onlyValues, aes(x = Year, y = no2_st))
base_no2 + geom_boxplot()+ labs(title = "Figure 3. Seven-Year Average NO2 from Death, by Year of Death. \nMemory and Aging Project (N=1048)", 
                  y = "7-Year NO2 Average from Death (ppb)", x = "Year of Death") +
                  theme(axis.text.x = element_text(angle = 45))

base_o3 <- ggplot(averaged_o3_byProjid_onlyValues, aes(x = Year, y = o3_st))
base_o3 + geom_boxplot()+ labs(title = "Figure 4. Seven-Year Average O3 (Ozone) from Death, by Year of Death. \nMemory and Aging Project (N=695)", 
                  y = "7-Year O3 (Ozone) Average from Death (ppb)", x = "Year of Death") +
                  theme(axis.text.x = element_text(angle = 45))


# 6) export averaged AP to csv
write.csv(averaged_pm25_byProjid_onlyValues, file = "./averaged_pm25_byProjid_onlyValues.csv")
write.csv(averaged_no2_byProjid_onlyValues, file = "./averaged_no2_byProjid_onlyValues.csv")
write.csv(averaged_o3_byProjid_onlyValues, file = "./averaged_o3_byProjid_onlyValues.csv")



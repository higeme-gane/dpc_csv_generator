rm(list = ls())
library(conflicted)
library(tidyverse)
library(arrow)
conflicts_prefer(dplyr::filter)

# GitHub Actions から渡されるパラメータ
args <- commandArgs(trailingOnly = TRUE)
dpc6 <- args[1]
all_0_ope_1 <- as.numeric(args[2])

all_ope <- if_else(all_0_ope_1 == 0, "all", "ope")
fy <- c(2016:2024)
files <- str_c("df_long_fy_", fy, ".parquet")

df_master <- read_parquet("facility_summary_table_fy2016_2024.parquet") |> 
  mutate(across(c(kokuji_2016, kokuji_2017, kokuji_2018,
                  kokuji_2019, kokuji_2020, kokuji_2021,
                  kokuji_2022, kokuji_2023, kokuji_2024), as.character)) |> 
  select(kokuji_2016, kokuji_2017, kokuji_2018,
         kokuji_2019, kokuji_2020, kokuji_2021,
         kokuji_2022, kokuji_2023, kokuji_2024,
         hosp_name_2024, prefecture_2024) |> 
  mutate(kokuji_last_year = kokuji_2024) |> 
  pivot_longer(cols = kokuji_2016:kokuji_2024, names_to = "fy", values_to = "kokuji_num")
df_join <- select(.data = df_master, fy, kokuji_num, kokuji_last_year)
df_hosp_name <- select(.data = df_master, kokuji_last_year, prefecture_2024, hosp_name_2024) |> 
  distinct(kokuji_last_year, .keep_all = TRUE)

df <- map(files, ~{
  read_parquet(.x)  |> 
    pivot_longer(cols = c(-dpc_n_d, -value), names_to = "fy", values_to = "kokuji_num") |> 
    mutate(kokuji_num = as.character(kokuji_num))
}) |> list_rbind() |> 
  filter(str_detect(dpc_n_d, "n")) |> 
  filter(str_detect(dpc_n_d, dpc6)) |> 
  mutate(ope = str_sub(dpc_n_d, 9, 11)) |>
  filter(ope != "97o")
if (all_0_ope_1 == 1){
  df <- filter(.data = df, ope != "99_")
}
df <- left_join(df, df_join, by = c("fy", "kokuji_num"),
                relationship = "many-to-many") |> 
  drop_na() |> 
  distinct()

piv <- summarise(.data = df, n = sum(value), .by = c("kokuji_last_year", "fy")) |> 
  mutate(fy = str_c("fy", str_sub(fy, -4, -1))) |> 
  pivot_wider(names_from = fy, values_from = n) |> 
  left_join(df_hosp_name, by = "kokuji_last_year") |> 
  select(kokuji_last_year, prefecture_2024, hosp_name_2024, fy2016:fy2024) |> 
  arrange(desc(fy2024))

output_file <- str_c(dpc6, "_", all_ope, "_2016_2024.csv")
write_excel_csv(piv, output_file)
cat("✅ 生成完了:", output_file, "\n")
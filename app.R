# ================================================
# DPC6桁コード別 病院別患者数集計ツール（Shiny版・最終完全修正済み）
# ================================================
library(shiny)
library(tidyverse)
library(arrow)

# データ読み込み（列を明示的に指定して安全化）
if (!exists("wide")) {
  wide <- read_parquet("facility_summary_table_fy2016_2024.parquet") |> 
    # 必要な列だけを明示的に選択（これでn_months_*などが混入しない）
    select(kokuji_2016, kokuji_2017, kokuji_2018, kokuji_2019, kokuji_2020,
           kokuji_2021, kokuji_2022, kokuji_2023, kokuji_2024,
           hosp_name_2024, prefecture_2024) |> 
    mutate(across(starts_with("kokuji_"), as.character)) |> 
    mutate(kokuji_last_year = kokuji_2024)
  
  # 病院名マスタ
  df_hosp_name <- wide |> 
    select(kokuji_last_year, prefecture_2024, hosp_name_2024) |> 
    distinct(kokuji_last_year, .keep_all = TRUE)
  
  # join用longデータ
  df_join <- wide |> 
    pivot_longer(cols = kokuji_2016:kokuji_2024, 
                 names_to = "fy", values_to = "kokuji_num") |> 
    select(fy, kokuji_num, kokuji_last_year)
  
  # 長期データ（df_long_fy_xxxx.parquet）
  files <- paste0("df_long_fy_", 2016:2024, ".parquet")
  df_raw <- map(files, ~read_parquet(.x) |> 
                  pivot_longer(cols = c(-dpc_n_d, -value), 
                               names_to = "fy", values_to = "kokuji_num") |> 
                  mutate(kokuji_num = as.character(kokuji_num))) |> 
    list_rbind()
}

# UI
ui <- fluidPage(
  titlePanel("DPC6桁コード別 病院別患者数集計ツール（2016-2024）"),
  sidebarLayout(
    sidebarPanel(
      textInput("dpc6", "DPC6桁コード", value = "040040", width = "100%"),
      radioButtons("all_0_ope_1", "集計対象の患者数",
                   choices = c("全件（すべて）" = 0, "手術あり（99以外）の実件数" = 1),
                   selected = 0),
      actionButton("generate", "2024年度件数降順集計", 
                   class = "btn-primary btn-lg", width = "100%")
    ),
    mainPanel(
      h4("生成結果（プレビュー）"),
      tableOutput("preview"),
      downloadButton("download", "📥 CSVをダウンロード", class = "btn-success btn-lg")
    )
  )
)

# Server
server <- function(input, output, session) {
  
  result <- eventReactive(input$generate, {
    req(input$dpc6)
    validate(need(nchar(input$dpc6) == 6 && grepl("^[0-9]+$", input$dpc6), 
                  "DPCコードは6桁の数字で入力してください"))
    
    all_0_ope_1 <- as.numeric(input$all_0_ope_1)
    all_ope <- if (all_0_ope_1 == 0) "all" else "ope"
    
    df <- df_raw |> 
      filter(str_detect(dpc_n_d, "n")) |> 
      filter(str_detect(dpc_n_d, input$dpc6)) |> 
      mutate(ope = str_sub(dpc_n_d, 9, 11)) |> 
      filter(ope != "97o")
    
    if (all_0_ope_1 == 1) df <- filter(df, ope != "99_")
    
    df <- df |> 
      left_join(df_join, by = c("fy", "kokuji_num")) |> 
      drop_na() |> 
      distinct()
    
    piv <- df |> 
      summarise(n = sum(value), .by = c("kokuji_last_year", "fy")) |> 
      mutate(fy = str_c("fy", str_sub(fy, -4, -1))) |> 
      pivot_wider(names_from = fy, values_from = n) |> 
      left_join(df_hosp_name, by = "kokuji_last_year") |> 
      select(kokuji_last_year, prefecture_2024, hosp_name_2024, fy2016:fy2024) |> 
      arrange(desc(fy2024))
    
    filename <- paste0(input$dpc6, "_", all_ope, "_2016_2024.csv")
    
    list(data = piv, filename = filename)
  })
  
  output$preview <- renderTable({
    head(result()$data, 10)
  })
  
  output$download <- downloadHandler(
    filename = function() { result()$filename },
    content = function(file) {
      write_excel_csv(result()$data, file)
    }
  )
}

shinyApp(ui = ui, server = server)
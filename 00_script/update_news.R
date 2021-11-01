library(tidyverse)
library(lubridate)
library(readxl)
library(DBI)
library(odbc)
library(dbplyr)
library(tidyquant)
library(httr)
library(jsonlite)
library(rvest)
library(furrr)
library(RSQLite)

source("./00_script/news_crawling.R", encoding = "UTF-8")

ticker_tbl <- kstock_get()

# 처음 데이터 적재용
# dat <- readRDS("news.RDS") %>%
#   as_tibble()

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
# dbWriteTable(con, "JONG_NEWS", jong_news, overwrite = TRUE)

jong_news <- tbl(con, "JONG_NEWS") 

article_id <- jong_news %>% 
  select(id, code) %>% 
  collect()

dbDisconnect(con)


add_indv_news <- function(code, i) { 
  continue <- TRUE
  page <- 1
  news_dat <- tibble()
  
  while(continue) {
    dat <- possibly_get_news_dat(page, code)
    
    # 검색되는 뉴스가 없으면 break
    if(nrow(dat) == 0) break
    
    # 이미 있는 기사는 제거
    dat <- dat %>% 
      anti_join(article_id, by = c("id", "code"))
    
    # 제거 후 남는 데이터가 없으면 더이상 진행하지 않음.
    if(nrow(dat) == 0) {
      continue <- FALSE
      break
    }
    
    page <- page + 1 
    
    news_dat <- news_dat %>% 
      bind_rows(dat)
  }
  
  print(str_glue("Code {code} done... {i} companies completed..."))
  return(news_dat)
}

plan(multisession)
additional_news <- future_imap_dfr(ticker_tbl$code , ~add_indv_news(.x, .y))
plan(sequential)


dbWriteTable(con, "JONG_NEWS", additional_news, append = TRUE)
dbDisconnect(con)



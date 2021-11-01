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

get_theme_lastpage <- function() {
  pgRR <- GET(
    url = "https://finance.naver.com/sise/theme.nhn",
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) %>% 
    content() %>% 
    html_node("td.pgRR")
  
  if(!is.na(pgRR)) {
    page_no <- pgRR %>% 
      html_node("a") %>% 
      html_attr("href") %>% 
      str_extract("&page=[0-9]+") %>% 
      str_remove("&page=") %>% 
      as.integer()
  } else {
    return(NA_real_) # 게시글이 없는 경우 NA
  }
  
  return(page_no)
}

get_theme_list <- function(page_no) {
  dat <- GET(
    url = "https://finance.naver.com/sise/theme.nhn",
    query = list(
      page = page_no
    ),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) %>% 
    content()
  
  theme_name <- dat %>% 
    html_nodes("td.col_type1") %>% 
    html_text()
  
  theme_link <- dat %>% 
    html_nodes("td.col_type1") %>% 
    html_nodes("a") %>% 
    html_attr("href")
  
  res <- tibble(theme_name = theme_name,
                theme_link = theme_link)
  
  return(res)
}

get_theme_jong <- function(theme_link) {
  dat <- GET(
    url = str_c("https://finance.naver.com", theme_link),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) %>% 
    content()
  
  Sys.setlocale("LC_ALL", "English")
  res <- dat %>% 
    html_table() %>% 
    pluck(3)
  Sys.setlocale("LC_ALL", "Korean")
  
  res <- res %>%
    select(name = 1, exp = 2) %>% 
    filter(name != "") %>% 
    mutate(name = str_remove(name, "\\*") %>% str_trim(),
           exp = str_remove(exp, "테마 편입 사유") %>% str_remove_all("\\n|\\t"),
           name_length = str_length(name)) %>%
    mutate(exp = str_sub(exp, name_length+1, end = -1L)) %>% 
    select(-name_length)
  
  return(res)
}

theme_last_page <- get_theme_lastpage()

theme_tbl <- c(1:theme_last_page) %>% 
  map_dfr(get_theme_list) %>% 
  mutate(theme_jong = map(theme_link, get_theme_jong)) %>% 
  select(-theme_link) %>% 
  unnest(theme_jong)

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
dbWriteTable(con, "THEME_JONG", theme_tbl, overwrite = TRUE)
dbDisconnect(con)

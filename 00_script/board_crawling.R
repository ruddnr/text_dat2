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

# 티커 ----
con <- dbConnect(odbc::odbc(), dsn = "QPMS_op", uid = "sa", pwd = "qpmsdb!@#", 
                 encoding = "EUC-KR")

ticker_tbl <- tbl(con, in_schema(sql("MARKET.dbo"), sql("CA"))) %>% 
  # 상폐, ETF, 우선주 제외 
  filter(DELIST_DATE == "99999999", ETF_GUBUN != "1", str_sub(CODE, 6, 6) == "0") %>% 
  select(code = CODE, name = KN) %>% 
  collect()
# 
# ticker_tbl <- readRDS("./02_data/stock_dat.RDS")$ca %>% 
#   # 상폐, ETF, 우선주 제외 
#   filter(DELIST_DATE == "99999999", ETF_GUBUN != "1", str_sub(CODE, 6, 6) == "0") %>% 
#   select(code = CODE, name = KN)
#   

# 종목게시판


# 단일 토론방 크롤링 ----
get_board_naver <- function(page, code) {
  
  get_res <- GET(
    url = "https://finance.naver.com/item/board.nhn",
    query = list(
      code = code,
      page = page
    ),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) 
  
  dat <- content(get_res)
  
  if(get_res$status_code == 200) {
    Sys.setlocale("LC_ALL", "English")
    res <- dat %>% 
      html_node("#content > div.section.inner_sub > table.type2") %>%
      html_table()
    Sys.setlocale("LC_ALL", "Korean")
    
    res <- res %>% 
      select(1:6) %>% 
      magrittr::set_names(c("date", "title", "writer", "view", "like", "dislike")) %>% 
      # mutate(date = ymd_hm(date)) %>% 
      na.omit()
    
    # board_content <- get_board_link(dat)
    
    # res <- res %>% bind_cols(board_content)
    
    return(res)
  } else {
    print(str_glue("Status code for {code} : {get_res$status_code} Error!"))
  }
}




# 뉴스 링크로부터 뉴스 본문 추출 함수
get_board_content <- function(code, nid) {
  
  dat <- GET(
    url = "https://finance.naver.com/item/board_read.nhn",
    query = list(
      code = code,
      nid = nid
    ),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) %>% content()
  
  board_content <- dat %>% 
    html_elements(css = "#body") %>% 
    html_text()
  
  return(board_content)
}

get_board_link <- function(dat) {
  board_link <- dat %>% 
    html_node("tbody") %>% 
    html_nodes("a") %>% 
    html_attr("href") %>% 
    tibble(link = .) %>% 
    separate(link, into = c("code", "nid"), sep = "&", extra = "drop") %>% 
    mutate(across(c(code, nid), ~str_extract(., "[0-9]+"))) %>% 
    rowwise() %>%
    mutate(board_content = get_board_content(code, nid)) %>%
    ungroup()
  
  return(board_link)
}

# 토론방 마지막 페이지 ----
get_board_lastpage <- function(code) {
  pgRR <- GET(
    url = "https://finance.naver.com/item/board.nhn",
    query = list(
      code = code
    ),
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


# 토론방 전체 게시물 크롤링 ----
# from_date 설정하여 특정일 이후의 데이터만 추출 가능 (정확히 특정일 이후는 아님)
get_full_board_naver <- function(code, i, from_date) {
  
  last_page <- get_board_lastpage(code)
  
  res <- tibble()
  
  if(!is.na(last_page)) {
    continue <- TRUE
    board_dat <- tibble()
    page <- 1
    
    while(continue) {
      tmp <- get_board_naver(page, code)
      
      board_dat <- board_dat %>% bind_rows(tmp)
      page <- page + 1
      # if(!all(tmp$date >= date(from_date))) continue <- FALSE
      if(!all(tmp$date >= from_date)) continue <- FALSE
    }
  }
  
  print(str_glue("Code {code} done... {i} companies completed..."))
  
  # dbWriteTable(con, "JONG_BOARD", board_dat, append = TRUE)
  test[[code]] <<- board_dat
  # return(board_dat)
}



res <- get_full_board_naver(code = "058630", i = 1, from_date = "2020.04.19")

test <- list()
plan(multisession)
ticker_tbl$CODE%>%
  iwalk(possibly(get_full_board_naver, tibble()), from_date = "2020.01.01")
plan(sequential)



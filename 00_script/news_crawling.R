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
# 2. 뉴스 ----

# get_res <- GET(
#   url = "https://finance.naver.com/item/news_news.nhn",
#   query = list(
#     code = "122990",
#     page = 1,
#     sm = "title_entity_id.basic"
#   ),
#   user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
# ) 
# 
# dat <- content(get_res)

# 마지막 페이지 가져오는 함수
get_lastpage <- function(code) {
  
  get_res <- GET(
    url = "https://finance.naver.com/item/news_news.nhn",
    query = list(
      code = code,
      page = 1,
      sm = "title_entity_id.basic"
    ),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) 
  
  dat <- content(get_res)
  # 
  
  navi_pgrr <- NA
  
  try(
    navi_pgrr <- dat %>% 
      html_node(".Nnavi") %>% 
      html_node(".pgRR"),
    silent = TRUE
  )

  
  if(!is.na(navi_pgrr)) {
    last_page <- dat %>% 
      html_node(".pgRR") %>% 
      html_node("a") %>% 
      html_attr("href") %>% 
      str_extract("page=[0-9]+") %>% 
      str_extract("[0-9]+") %>% 
      as.numeric()
  } else {
    last_page <- 1
  }
  
  return(last_page)
}

# 뉴스링크
# id 와 office_cd , code 를 조합해서 
# https://finance.naver.com/item/news_read.nhn?article_id=0004883032&office_id=018&code=000660&page=1&sm=title_entity_id.basic
# 위와 같이 링크를 만들면 됨. 
get_news_link <- function(dat) {
  news_link <- dat %>% 
    html_node("tbody") %>% 
    html_nodes("a") %>% 
    html_attr("href") %>% 
    tibble(link = .) %>% 
    filter(link != "#") %>% 
    separate(link, into = c("id", "office_id", "code"), sep = "&", extra = "drop") %>% 
    mutate(across(id:code, ~str_extract(., "[0-9]+"))) %>% 
    rowwise() %>% 
    mutate(news_content = get_news_content(id, office_id, code)) %>% 
    ungroup()
  
  return(news_link)
}

# 뉴스 링크로부터 뉴스 본문 추출 함수
get_news_content <- function(article_id, office_id, code) {
  
  dat <- GET(
    url = "https://finance.naver.com/item/news_read.nhn",
    query = list(
      article_id = article_id,
      office_id = office_id,
      code = code,
      page = 1,
      sm = "title_entity_id.basic"
    ),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) %>% content()
  
  news_content <- dat %>% 
    html_elements(css = "#news_read") %>% 
    html_elements(xpath = "//*[@id='news_read']//text()[not(ancestor::span)][not(ancestor::a)][not(ancestor::h3)][not(ancestor::p)]") %>%
    html_text() %>% 
    `[`(!str_detect(., "ⓒ|[[:alnum:]]+@")) %>% 
    str_remove_all("\n\t\t\t") %>% 
    str_c(collapse = " ") %>% 
    str_squish() %>% 
    str_trim()
  
  return(news_content)
}

# 각 페이지와 코드에 대해 뉴스 데이터 추출하는 함수 
get_news_dat <- function(page, code) {
  get_res <- GET(
    url = "https://finance.naver.com/item/news_news.nhn",
    query = list(
      code = code,
      page = page,
      sm = "title_entity_id.basic"
    ),
    user_agent(agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36")
  ) 
  
  dat <- content(get_res)
  
  Sys.setlocale("LC_ALL", "English")
  news_list <- html_table(dat) 
  Sys.setlocale("LC_ALL", "Korean")
  
  # 메인 뉴스테이블
  news_raw <- news_list[[1]] %>% 
    select(1:3)
  
  #  뉴스가 없는 경우
  if(nrow(news_raw) == 1 && str_detect(news_raw$제목, "뉴스가 없습니다")) return(tibble())
  
  cluster_vec <- rep(NA_character_, times = nrow(news_raw))
  
  # 클러스터 여부 판단
  if(any(str_sub(news_raw$제목, 1,4) == "연관기사")) {
    # 클러스터 처리 작업 (연관기사)
    # 클러스터당 기사 수
    # cluster head 기사 + '연관기사 목록' 두 기사를 더하기 위해 2 더함. 
    num_article_per_cluster <- news_list[2:(length(news_list)-1)] %>% 
      map_dbl(nrow) %>% 
      `+`(2)
    
    cluster_head_rownum <- news_raw %>% 
      mutate(row_number = row_number()) %>% 
      filter(str_detect(제목, "연관기사")) %>% 
      pull(row_number) %>% 
      `-`(1)
    
    # 클러스터 ID
    # 앞의 세자리를 제외한 나머지 부분이 cluster head 기사의 article id 와 동일함
    cluster_id <- dat %>% 
      html_node("tbody") %>% 
      html_nodes("[class*=relation_lst]") %>% 
      html_attr("class") %>% 
      str_extract("[0-9]+") 
    
    for(i in seq_along(num_article_per_cluster)) {
      start <- cluster_head_rownum[i]
      end <- start + num_article_per_cluster[i] - 1
      cluster_vec[start:end] <- rep(cluster_id[i], times = num_article_per_cluster[i])
    }
  }
  
  # 뉴스링크
  
  news_link <- get_news_link(dat)
  
  # 최종결과
  news <- news_raw %>% 
    add_column(cluster_id = cluster_vec) %>% 
    rename(title = 1, office = 2, date = 3) %>%
    filter(!str_detect(title, "연관기사 목록")) %>% 
    bind_cols(news_link) %>% 
    # 클러스터가 없는 기사 혹은 클러스터의 첫번째 기사를 유니크 기사로 표시
    mutate(is_unique = if_else(is.na(cluster_id) | str_sub(cluster_id, 4L, -1L) == id, 1, 0))
  
  return(news)
}

# 전체 페이지에 대해 뉴스 추출하는 함수
get_tot_news <- function(code, i) {
  
  last_page <- get_lastpage(code)
  
  res <- c(1:last_page) %>% 
    map_dfr(possibly(get_news_dat, otherwise = tibble(), quiet = FALSE), code = code)
  
  print(str_glue("{i} : News for {code} done..."))
  Sys.sleep(runif(1, 1, 2))
  return(res)
}

# get_tot_news("009140", 1) -> test
# 
# ca <- readRDS("stock_dat.RDS")$ca %>% 
  # filter(str_sub(CODE, 6, 6) == "0", ETF_GUBUN != "1", DELIST_DATE == "99999999")

# news_res <- ca$CODE %>% 
#   imap_dfr(get_tot_news)
# 
# news_res %>% 
#   saveRDS("news.RDS")

# 주가 크롤링 ----
kstock_get <- function(isuCd) {
  POST("http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
       query = list(
         bld = "dbms/MDC/STAT/standard/MDCSTAT01901",
         mktId = "ALL",
         share = 1
       )) %>% 
    read_html() %>% 
    html_text() %>% 
    fromJSON() %>% 
    purrr::pluck(1) %>%  
    as_tibble() %>% 
    filter(MKT_TP_NM != "KONEX") %>% 
    select(code = ISU_SRT_CD, name = ISU_ABBRV, market = MKT_TP_NM)
}

possibly_get_news_dat <- possibly(get_news_dat, tibble())



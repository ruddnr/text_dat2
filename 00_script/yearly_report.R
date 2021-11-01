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
library(RSQLite)
# library(telegram)

api_key <- "22c0a53f62a3507fdab2bdb960a0367ace917b12"

# start_date <- "20200301"
# end_date <- "20200331"
# 
# # A003 분기보고서
# # A001 사업보고서
# report_type <- "A001"

get_tot_page <- function(start_date, end_date, report_type) {
  tot_page <- GET(url = "https://opendart.fss.or.kr/api/list.json",
                  query = list(crtfc_key = api_key,
                               bgn_de = start_date,
                               end_de = end_date,
                               pblntf_detail_ty = report_type, #사업보고서
                               page_no = 1,
                               page_count = "100",
                               last_reprt_at = "N")) %>% 
    content(as = "text") %>% 
    fromJSON() %>% 
    purrr::pluck("total_page")
  
  return(tot_page)
}

get_post_list <- function(page_no, start_date, end_date, report_type) {
  GET(url = "https://opendart.fss.or.kr/api/list.json",
     query = list(crtfc_key = api_key,
                  bgn_de = start_date,
                  end_de = end_date,
                  pblntf_detail_ty = report_type,
                  page_no = page_no,
                  page_count = "100",
                  last_reprt_at = "N")) %>% 
    content(as = "text") %>% 
    fromJSON() %>% 
    purrr::pluck("list") %>% 
    as_tibble()
}

filter_post_list <- function(post_list_tbl_raw, filter_string) {
  # 필터링 하는 사업보고서 스트링 수정해주어야 함.
  post_list_tbl <- post_list_tbl_raw %>% 
    # 코스피/코스닥 종목만 추출, 코스피 = Y, 코스닥 = K
    filter(corp_cls %in% c("K", "Y")) %>% 
    # 3분기 분기보고서만 추출
    # filter(str_detect(report_nm, "사업보고서 \\(2019.12\\)")) %>%
    filter(report_nm == filter_string) %>%
    select(corp_code, corp_name, stock_code, corp_cls, rcept_no)
  
  return(post_list_tbl)
}
  
# post_list_tbl_raw <- c(1:tot_page) %>% 
#   map_dfr(get_post_list, report_type)



get_bs_dat <- function(rcpNo, dcmNo, eleId, offset, length, dtd) {
  
  dat <- GET(url = "http://dart.fss.or.kr/report/viewer.do?",
             query = list(
               rcpNo = rcpNo,
               dcmNo = dcmNo,
               eleId = eleId,
               offset = offset,
               length = length,
               dtd = dtd
             )) 
  
  res <- content(dat) %>% html_text()
  
  return(res)
}

get_post <- function(rcept_no, i){
  # 여기 공시 보고서 가지고 와서, function에서 사업의 내용에 들어가는 인수를 찾아야 함.
  dat <- GET(url = "http://dart.fss.or.kr/dsaf001/main.do?",
             query = list(
               rcpNo = rcept_no
             )) 
  func_arg_tbl <- content(dat) %>% 
    html_node(xpath = "/html/head/script[9]") %>% 
    html_text() %>% 
    str_replace_all('"', "") %>% 
    str_remove_all("[\\t\\n ]") %>% 
    str_split(";") %>% 
    unlist() %>% str_subset("사업의 내용|사업의내용") %>% 
    str_extract("viewDoc\\(.*\\)") %>% 
    str_remove_all("viewDoc|[()']") %>% 
    str_split(",", simplify = TRUE) %>% 
    setNames(c("rcpNo", "dcmNo", "eleId", "offset", "length", "dtd")) %>% 
    as_tibble_row()
  
  Sys.sleep(runif(1, 0.5, 1))
  
  bs_dat <- get_bs_dat(rcpNo = func_arg_tbl$rcpNo, dcmNo = func_arg_tbl$dcmNo,
                       eleId = func_arg_tbl$eleId, offset = func_arg_tbl$offset, 
                       length = func_arg_tbl$length, dtd = func_arg_tbl$dtd)
  
  # print(str_glue("{scales::percent(i/nrow(post_list_tbl), .01)} Done..."))
  print(str_glue("{i} Done..."))
  
  Sys.sleep(runif(1, 0.5, 1))
  
  return(bs_dat)
}


# func_arg_tbl <- post_list_tbl %>% 
#   # slice(1:100) %>% 
#   pull(rcept_no) %>% 
#   imap_dfr(possibly(get_post_func_arg, tibble()))
# 
# base_dat_tbl <- post_list_tbl %>% 
#   rename(rcpNo = rcept_no) %>% 
#   left_join(func_arg_tbl) %>% 
#   mutate(i = row_number())

get_yearly_report <- function(start_date, 
                              end_date, 
                              filter_string, 
                              category, 
                              report_type = "A001") {
  
  # force(list(start_date, end_date, filter_string, category, report_type))
  # force(start_date)
  
  tot_page <- get_tot_page(start_date, end_date, report_type)
  
  post_list_tbl <- c(1:tot_page) %>% 
    map_dfr(get_post_list, start_date, end_date, report_type) %>% 
    filter_post_list(filter_string)
  
  res <- post_list_tbl %>% 
    # slice(1:2) %>%
    mutate(content = imap(rcept_no, get_post)) %>% 
    mutate(category = category,
           content = unlist(content))
  
  return(res)
}
# 
start_date <- "20210220"
end_date <- "20210515"
filter_string <- "사업보고서 (2020.12)"
category <- "사업보고서_2020"
# 
res <- get_yearly_report(start_date, end_date, filter_string, category)


# report_tbl <- tibble(
#   start_date = make_date(2018:2020, month = 2, day = 20) %>% str_remove_all("-"),
#   end_date = make_date(2018:2020, month = 4, day = 30) %>% str_remove_all("-"),
#   filter_string = str_c("사업보고서 (", c(2017:2019), ".12)"),
#   category = str_c("사업보고서_", c(2017:2019))
# )

# report_res <- report_tbl %>% 
#   # slice(3) %>%
#   pmap(get_yearly_report)


report_res %>% saveRDS("yearly_report.RDS")

dat <- readRDS("./02_data/yearly_report.RDS")

dat <- do.call(bind_rows, dat) %>% 
  mutate(content = unlist(content))

dat <- dat %>% 
  bind_rows(res) 

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")

dbWriteTable(con, "JONG_FILINGS", dat)
dbDisconnect(con)

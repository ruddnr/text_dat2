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
library(slider)
library(pdftools)
library(fs)
library(plotly)
library(dygraphs)
library(timetk)


# con_qpms <- dbConnect(odbc::odbc(), dsn = "QPMS_op", uid = "sa", pwd = "qpmsdb!@#", encoding = "EUC-KR")
# 
# MA <- tbl(con_qpms, in_schema(sql("AGGR.dbo"), sql("MJMPDF_TMP"))) %>%
#   filter(ORDER_YMD == "20210420") %>%
#   collect() %>% 
#   select(etf_code = ETF_CD, code = GS_JM_CD, name = JM_NM, date = ORDER_YMD, qty = CU_UNIT_QTY) %>% 
#   mutate(etf_code = str_sub(etf_code, 4, 9), code = str_sub(code, 4, 9))
# 
# etf_dat <- read_xlsx("./02_data/ETF_category.xlsx") %>% 
#   mutate(code = str_remove(code, "A")) %>% 
#   rename(etf_code = code, etf_name = name) %>% 
#   left_join(MA)
# 
# dbWriteTable(con, "ETF_PDF", etf_dat, overwrite = TRUE)

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")

stock_info <- tbl(con, "STOCK_INFO") %>%
  filter(str_sub(CODE, 6, 6) == "0", ETF_GUBUN != "1", DELIST_DATE == "99999999") %>% 
  select(code = 1, name = 2) %>% 
  collect()

date_tbl <- tbl(con, "MA") %>% 
  select(date = FD, DDAY, TR) %>% 
  collect() %>% 
  mutate(date = ymd(date)) %>% 
  filter(date >= "2019-12-02", date <= "2021-04-04") %>% 
  mutate(wseq = (row_number()-1) %/% 7 + 1) %>% 
  filter(date >= "2019-12-30")

theme_jong <- tbl(con, "THEME_JONG")

theme_ticker <- theme_jong %>% filter(theme_name == "풍력에너지") %>% 
  collect() %>% 
  left_join(stock_info %>% select(code = 1, name = 2))

daily_return <- tbl(con, "DAILY_RETURN") %>%
  filter(CODE %in% !!theme_ticker$code) %>% 
  collect() %>% 
  select(code = CODE, date = TD, return = RTN) %>% 
  mutate(date = ymd(date), return = return + 1 )

news <- tbl(con, "JONG_NEWS") %>% 
  filter(code %in% !!theme_ticker$code) %>% 
  collect() %>% 
  mutate(date = parse_datetime(date, "%Y.%m.%d %H:%M") %>% as_date()) %>% 
  arrange(date) %>% 
  count(date, name = "news_count")  
  

report <- tbl(con, "FN_COMP_LIST") %>% 
  collect() %>% 
  mutate(code = str_remove(ITEM_CD, "A")) %>% 
  semi_join(theme_ticker, by = "code") %>% 
  select(date = FILTER_BULLET_DT, code) %>% 
  mutate(date = ymd(date)) %>% 
  count(date, name = "report_count")

  
# 포함 종목 동일가중 주간수익률  
weekly_return <- date_tbl %>% 
  left_join(daily_return) %>% 
  mutate(return = replace_na(return, 1)) %>% 
  
  group_by(code, wseq) %>% 
  summarise(return = prod(return) - 1) %>% 
  ungroup() %>% 
  
  group_by(wseq) %>% 
  summarise(return = mean(return)) %>% 
  ungroup() %>% 
  left_join(date_tbl %>% 
              group_by(wseq) %>% 
              filter(date == max(date)) %>% 
              ungroup() %>% 
              select(date, wseq)) %>% 
  select(-wseq) %>% 
  select(date, return)

comb <- date_tbl %>% 
  left_join(news) %>% 
  left_join(report) %>% 
  mutate(across(where(is.numeric), ~replace_na(., 0))) %>% 
  group_by(wseq) %>% 
  summarise(date = max(date),
            news_count = sum(news_count),
            report_count = sum(report_count)) %>% 
  ungroup() %>% 
  select(-wseq) %>% 
  left_join(weekly_return) %>% 
  filter(date >= "2020-03-31")

g <- comb %>% 
  
  mutate(across(where(is.numeric), ~scale(.))) %>% 
  pivot_longer(-date) %>% 
  ggplot(aes(date, value, color = name)) +
  geom_point() + 
  geom_line() 

ggplotly(g)

comb %>% 
  mutate(across(where(is.numeric), ~scale(.))) %>% 
  mutate(news_count = lag(news_count), report_count = lag(report_count)) %>% 
  tk_xts() %>% 
  dygraph
  

test <- tbl(con, "FN_COMP_LIST") %>% 
  collect()

test %>% 
  mutate(count = str_count(SUMMARY, "자전거")) %>% 
  select(ITEM_NM, count) %>% 
  group_by(ITEM_NM) %>% 
  summarise(count = sum(count)) %>%  
  arrange(desc(count)) %>% 
  View()



news_test <- tbl(con, "JONG_NEWS") %>% 
  collect()

news_test %>% 
  mutate(count = str_count(title, "낸드")) %>% 
  select(code, count) %>% 
  group_by(code) %>% 
  summarise(count = sum(count)) %>%  
  arrange(desc(count)) %>% 
  left_join(stock_info) %>% 
  View()



comb %>% 
  
  mutate(across(where(is.numeric), ~scale(.))) %>% 
  pivot_longer(-date) %>% 
  ggplot(aes(date, value, color = name)) +
  geom_point() + 
  geom_line() +
  facet_wrap(~name, ncol = 1, scales = "free_y") 

news_wind %>%
  count(date) %>% 
  ggplot(aes(date, n)) +
  geom_point() + 
  geom_line() + 
  geom_smooth() + 
  theme_light() 










con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")

fnlist <- tbl(con, "FN_COMP_LIST") %>% collect()

get_pdf <- function(doc_number) {
  file_name <- dir_ls("./01_pdf") %>% 
    `[`(str_detect(., doc_number))
  
  pdf_file <- pdf_text(file_name) %>% 
    str_c(collapse = " ") %>% 
    str_squish() 
  
  return(pdf_file)
}



dir_ls("./01_pdf")

stopwords_pdf <- read_excel("./02_data/stopwords_pdf.xlsx") %>% 
  pull(stopwords) %>% 
  str_c(collapse = "|")

file_name <- "./01_pdf/20210405_585035_원익IPS.pdf"

pdf_text(file_name) %>% 
  str_c(collapse = " ") %>% 
  str_remove_all("\r\n") %>% 
  # 숫자 문자 마침표 공백만 남기고 제거
  str_remove_all("[^[:alnum:][:space:].]") %>% 
  # 2개 이상 숫자 제거
  str_remove_all("\\d{2,}|[0-9]*\\.[0-9]+|[0-9]+\\.|^[0-9]+$") %>% 
  # 월일 제거
  str_remove_all("[0-9]+월|[0-9]+일|[0-9]{1}[QF]") %>% 
  # 필요없는 단어 제거
  str_remove_all(stopwords_pdf) %>% 
  str_replace_all("ㆍ", " ") %>% 
  # 영어 약어만 남기기
  # 소문자는 제거, 대문자 하나 + 소문자 제거, 6글자 이상의 대문자 제거
  str_remove_all("[a-z]+|[A-Z]{1}[a-z]+|[A-Z]{6,}|(^[[:alnum:]]{1}$)") %>% 
  str_squish()



fnlist %>% 
  count(FILTER_BULLET_DT, ITEM_NM) %>% 
  rename(date = 1, name = 2, n = 3) %>% 
  mutate(date = ymd(date)) %>% 
  filter(name == "삼성전자") %>% 
  ggplot(aes(date, n)) + 
  geom_line()


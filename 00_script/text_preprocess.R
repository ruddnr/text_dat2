library(tidyverse)
library(lubridate)
library(readxl)
library(DBI)
library(odbc)
library(dbplyr)
library(tidyquant)
library(tidytext)
library(RcppMeCab)
library(KoNLP)
library(furrr)
library(dtplyr)
library(quanteda)
library(quanteda.textmodels)
library(plotly)

  

# 텍스트 전처리 ----
txt_preprocess <- function(text) {
  
  text <- text %>% 
    str_remove_all("\r\n") %>% 
    # 문자 마침표 공백만 남기고 제거
    str_remove_all("[^[:alpha:][:space:]]") %>% 
    str_replace_all("ㆍ", " ") %>% 
    # 영어 약어만 남기기
    # 소문자는 제거, 대문자 하나 + 소문자 제거, 6글자 이상의 대문자 제거
    str_remove_all("[a-z]+|[A-Z]{1}[a-z]+|[A-Z]{6,}") %>% 
    
    
    str_squish()
  
  return(text)
}

my_stopwords <- read_xlsx("./02_data/stopwords_pdf.xlsx") %>% 
  mutate(pos = pos(stopwords)) %>%
  unnest(pos) %>% 
  separate(pos, into = c("word", "pos"), sep = "/") %>% 
  filter(str_length(word) > 1)

pos_helper <- function(code, text) {
  pos_tbl <- tibble(pos = pos(text) %>% purrr::pluck(1)) %>% 
    separate(pos, into = c("word", "pos"), sep = "/") %>% 
    
    # NNG 일반명사, NNG 고유명사, SL 외국어
    filter(str_detect(pos, "NNG|NNP|SL")) %>% 
    filter(str_length(word) != 1) %>% 
    filter(!word %in% my_stopwords$word) %>% 
    count(word, pos, sort = TRUE) %>% 
    mutate(code = code)
  
  return(pos_tbl)
}

possibly_pos_helper <- possibly(pos_helper, tibble())



con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")

# raw 텍스트 - 

report_raw <- tbl(con, "FN_COMP_LIST") %>% 
  select(BULLET_NO_OLD, code = ITEM_CD) %>% 
  left_join(tbl(con, "FN_PDF")) %>% 
  collect() %>% 
  mutate(code = str_remove(code, "A")) %>% 
  group_by(code) %>% 
  summarise(count = n(), 
            text = str_c(PDF_RAW, collapse = " ")) %>% 
  ungroup() %>% 
  mutate(text = txt_preprocess(text))
  
news_raw <- tbl(con, "JONG_NEWS") %>% 
  filter(is_unique == 1) %>% 
  collect() %>% 
  select(code, title, news_content) %>% 
  mutate(news_content = str_remove_all(news_content, "\\[.+\\]")) %>% 
  mutate(news_content = str_c(title, news_content), sep = " ") %>% 
  select(-title) %>% 
  
  group_by(code) %>% 
  summarise(count = n(), 
            text = str_c(news_content, collapse = " ")) %>% 
  ungroup() %>% 
  mutate(text = txt_preprocess(text))

news_raw2 <- tbl(con, "JONG_NEWS") %>% 
  filter(is_unique == 1) %>% 
  collect() %>% 
  select(code, title) %>% 
  
  group_by(code) %>% 
  summarise(count = n(), 
            text = str_c(title, collapse = " ")) %>% 
  ungroup() %>% 
  mutate(text = txt_preprocess(text))


filings_raw <- tbl(con, "JONG_FILINGS") %>% 
  filter(category == "사업보고서_2020") %>% 
  select(code = stock_code, text = content) %>%
  collect() %>% 
  mutate(text = txt_preprocess(text))


# stemming ----

# library(tictoc)
# tic()
# plan(multisession)
# report_words <- future_map2_dfr(.x = report_raw$code, .y = report_raw$text, .f = possibly_pos_helper)
# news_words2 <- future_map2_dfr(.x = news_raw2$code, .y = news_raw2$text, .f = possibly_pos_helper)
# news_words <- future_map2_dfr(.x = news_raw$code, .y = news_raw$text, .f = possibly_pos_helper)
# filings_words <- future_map2_dfr(.x = filings_raw$code, .y = filings_raw$text, .f = possibly_pos_helper)
# plan(sequential)
# saveRDS(news_words, "news_words.RDS")
# toc()  
# 
# saveRDS(news_words2, "news_words2.rds")
# saveRDS(filings_words, "filings_words.rds")

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")

# tf_idf 계산 ----
report_words <- readRDS("./report_words.RDS") 
  # left_join(report_raw %>% select(code, count)) %>% 
  # mutate(n = n / count) %>% 
  # select(-count)

news_words <- readRDS("./news_words2.RDS") 
  # left_join(news_raw2 %>% select(code, count)) %>% 
  # mutate(n = n / count) %>% 
  # select(-count)

filings_words <- readRDS("./filings_words.RDS") 

# 주식정보 업데이트
# con_qpms <- dbConnect(odbc::odbc(), dsn = "QPMS_op", uid = "sa", pwd = "qpmsdb!@#", 
#                  encoding = "EUC-KR")
# 
# CA <- tbl(con_qpms, in_schema(sql("MARKET.dbo"), sql("CA"))) %>% collect()
# 
# dbWriteTable(con, "STOCK_INFO", CA, overwrite = TRUE)

stock_info <- tbl(con, "STOCK_INFO") %>% 
  select(code = CODE, name = KN) %>% 
  collect()

dbDisconnect(con)

to_tfidf_dfm <- function(words_tbl) {
  
  words_dfm <- words_tbl %>% 
    anti_join(stock_info, by = c("code", "word" = "name")) %>% 
    left_join(stock_info, by = "code") %>% 
    cast_dfm(document = name, term = word, value = n) %>% 
    
    # 최소길이 2
    dfm_keep(min_nchar = 2) %>% 
    # 빈도 및 비율조건
    dfm_trim(max_docfreq = 0.4, docfreq_type = "prop") %>% 
    dfm_weight(scheme = "logcount") %>% 
    dfm_tfidf(force = TRUE)
  
  return(words_dfm)  
}

to_dfm <- function(words_tbl, to_tfidf = FALSE) {
  
  words_dfm <- words_tbl %>% 
    anti_join(stock_info, by = c("code", "word" = "name")) %>% 
    left_join(stock_info, by = "code") %>% 
    cast_dfm(document = name, term = word, value = n) %>% 
    
    # 최소길이 2
    dfm_keep(min_nchar = 2) %>% 
    # 빈도 및 비율조건
    dfm_trim(max_docfreq = 0.4, docfreq_type = "prop") %>% 
    dfm_weight(scheme = "logcount") 
  
  if(to_tfidf) {
    words_dfm <- words_dfm %>% 
      dfm_tfidf(force = TRUE)
  }
  return(words_dfm)  
}

report_dfm <- to_dfm(report_words, to_tfidf = TRUE)
news_dfm <- to_dfm(news_words, to_tfidf = TRUE)
filings_dfm <- to_dfm(filings_words, to_tfidf = TRUE)

dfm_list <- list(
  report_dfm = report_dfm,
  news_dfm = news_dfm,
  filings_dfm = filings_dfm
)

saveRDS(dfm_list, "dfm_list.RDS")



sort_words <- function(source_dfm, query) { 
  source_dfm %>% 
    tidy() %>% 
    filter(str_detect(term, query)) %>% 
    arrange(desc(count))
}



news_dfm %>% 
  sort_words("친환경")

report_dfm %>% 
  sort_words("친환경")

filings_dfm %>% 
  sort_words("친환경")


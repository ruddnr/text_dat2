library(tidyverse)
library(lubridate)
library(DBI)
library(odbc)
library(dbplyr)
library(httr)
library(jsonlite)
library(rvest)
library(furrr)
library(slider)
library(pdftools)
library(fs)

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
fnlist <- tbl(con, "FN_COMP_LIST") %>% collect()

dir_bullet_no_list <- dir_ls("./01_pdf") %>% 
  str_split("[[:punct:]]", simplify = TRUE) %>% 
  `[`(,6)

fnlist <- fnlist %>% 
  filter(!BULLET_NO_OLD %in% dir_bullet_no_list)



# FN가이드에 로그인


login <- "https://www.fnguide.com/home/login"

pgsession <- session(login)
pgform <- html_form(pgsession)[[1]]
filled_form <- html_form_set(pgform, MemberID = "research1", PassWord = "research2")
session_submit(pgsession, filled_form)

fnlist_nrow <- nrow(fnlist)

# 다운로드에 사용되는 인수에 대응되는 테이블 칼럼
# bukd = "R100"
# ofincd = OFFER_INST_CD
# btym = BULLET_DT의 연월까지 yyyymm
# buno = BULLETNO
save_pdf <- function(FILTER_BULLET_DT, 
                     BULLET_NO_OLD, 
                     ITEM_NM, 
                     BULLET_KIND_OLD, 
                     OFFER_INST_CD, 
                     BULLET_DT, 
                     row_number,
                     ...) {
  
  file_path <- str_c(FILTER_BULLET_DT, BULLET_NO_OLD, ITEM_NM, sep = "_") %>% 
    str_c("./01_pdf/", ., ".pdf")
  
  dir_bullet_no_list <- dir_ls("./01_pdf") %>% 
    str_split("[[:punct:]]", simplify = TRUE) %>% 
    `[`(,6)
  
  if(!BULLET_NO_OLD %in% dir_bullet_no_list) {
    res <- session_jump_to(pgsession, 
                   url = 'https://file.fnguide.com/upload1/SVR_pdfDownload.asp?',
                   query = list(
                     bukd = BULLET_KIND_OLD,
                     ofincd = OFFER_INST_CD,
                     btym = str_sub(BULLET_DT, 1, 6),
                     buno = BULLET_NO_OLD
                   ),
                   write_disk(file_path, overwrite = TRUE))
    
    print(str_glue("Status: {res$response$status_code}
                   Content: {res$response$headers$`content-disposition`}
                   Process: {scales::percent(row_number/fnlist_nrow, accuracy = 0.01)} Done..."))
    
    # Sys.sleep(runif(1, 0, 0.1))
  } else {
    print(str_c(file_path, " Already done."))
  }
}

fnlist %>% 
  pwalk(save_pdf)

test <- fnlist %>% slice(1)
BULLET_KIND_OLD <- test$BULLET_KIND_OLD
OFFER_INST_CD <- test$OFFER_INST_CD
BULLET_DT <- test$BULLET_DT
BULLET_NO_OLD <-　test$BULLET_NO_OLD
tmp <- tempfile()

res <- session_jump_to(pgsession, 
                       url = 'https://file.fnguide.com/upload1/SVR_pdfDownload.asp?',
                       query = list(
                         bukd = BULLET_KIND_OLD,
                         ofincd = OFFER_INST_CD,
                         btym = str_sub(BULLET_DT, 1, 6),
                         buno = BULLET_NO_OLD
                       ),
                       write_disk(tmp, overwrite = TRUE))


          
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

source("00_script/db_function.R")

# 
# test <- fnlist %>% slice(1)
# BULLET_KIND_OLD <- test$BULLET_KIND_OLD
# OFFER_INST_CD <- test$OFFER_INST_CD
# BULLET_DT <- test$BULLET_DT
# BULLET_NO_OLD <-　test$BULLET_NO_OLD
# tmp <- tempfile()

fn_login <- function() {
  login <- "https://www.fnguide.com/home/login"
  pgsession <<- session(login)
  pgform <- html_form(pgsession)[[1]]
  filled_form <- html_form_set(pgform, MemberID = "research1", PassWord = "research2")
  session_submit(pgsession, filled_form)
}


filter_new_report_no <- function()

save_pdf <- function(FILTER_BULLET_DT, 
                     BULLET_NO_OLD, 
                     ITEM_NM, 
                     BULLET_KIND_OLD, 
                     OFFER_INST_CD, 
                     BULLET_DT, 
                     row_number,
                     ...) {
  
  tmp <- tempfile()
  
  res <- session_jump_to(
    pgsession,
    url = 'https://file.fnguide.com/upload1/SVR_pdfDownload.asp?',
    query = list(
      bukd = BULLET_KIND_OLD,
      ofincd = OFFER_INST_CD,
      btym = str_sub(BULLET_DT, 1, 6),
      buno = BULLET_NO_OLD
    ),
    write_disk(tmp, overwrite = TRUE)
  )
  
  res <- tibble(BULLET_NO_OLD = BULLET_NO_OLD,
                PDF_RAW = pdf_text(tmp) %>% str_c(collapse = " "))
  
  # dbWriteTable(con, "FN_PDF", res, append = TRUE)
  write_table_to_database(res, "FN_PDF", append = TRUE)
  
  print(str_glue("{FILTER_BULLET_DT} {BULLET_NO_OLD} done."))
  
}


fn_login()

con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
fn_list <- tbl(con, "FN_COMP_LIST") %>% collect()
bullet_vec <- tbl(con, "FN_PDF") %>% pull(BULLET_NO_OLD)

fn_list_new <- fn_list %>% 
  filter(!BULLET_NO_OLD %in% bullet_vec)

fn_list_new %>% 
  pwalk(save_pdf)

dbDisconnect(con)

# 
# # pdf로 다운받은거 DB에 저장하는 부분 
# file_path <- dir_ls("./01_pdf") %>% `[`(1)
# 
# save_pdf <- function(file_path) {
#   bullet_no_old <- file_path %>% str_split("_", simplify = TRUE) %>% `[`(1, 3)
#   
#   pdf_text_raw <- pdf_text(file_path) %>% 
#     str_c(collapse = " ") 
#   
#   res <- tibble(BULLET_NO_OLD = bullet_no_old,
#                 PDF_RAW = pdf_text_raw)
#   
#   dbWriteTable(con, "FN_PDF", res, append = TRUE)
#   
#   print(str_glue("{bullet_no_old} Done ... !"))
# }
# 
# dir_ls("./01_pdf") %>% 
#   walk(save_pdf)



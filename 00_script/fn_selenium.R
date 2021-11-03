# Scrape report list from fnguide
# need to modify start_date and end_date

library(RSelenium)
library(seleniumPipes)
library(rvest)
library(httr)
library(tidyverse)
library(RSQLite)
library(odbc)
library(lubridate)



initialize_browser <- function(chrome_ver, task_kill = FALSE) {
  # shut down task if port 4569 is already in use
  if(task_kill == TRUE) {
    system("taskkill /im java.exe /f", intern=FALSE, ignore.stdout=FALSE)
  }
  rD <- rsDriver(port= 4569L, browser = "chrome", chromever = chrome_ver)
  
  return(rD)
}

login_fn <- function(rD) {
  remDr <- rD$client
  
  remDr$navigate("https://www.fnguide.com/")
  Sys.sleep(2)
  id <- remDr$findElement(using = "name", "MemberID")
  id$sendKeysToElement(list("research1"))
  Sys.sleep(1)
  pwd <- remDr$findElement(using = "name", "PassWord")
  pwd$sendKeysToElement(list("research2"))
  Sys.sleep(1)
  btn <- remDr$findElement(using = "css selector", value = ".btn--login")
  btn$sendKeysToElement(list(key = "enter"))
  Sys.sleep(1)
  remDr$findElement("class name", "btn--back")$sendKeysToElement(list(key = "enter"))
}

search_report_list <- function(rD, year_month) {
  start_date <- str_c(year_month, "01")
  end_date <- (make_date(year = str_sub(year_month, 1, 4),
                        month = str_sub(year_month, 5, 6)) + months(1) - days(1)) %>% str_remove_all("-")
  
  
  url <- str_glue("https://www.fnguide.com/api/fgdr/SearchTotalDiquest?searchType=FnResearch&searchStart={start_date}&searchEnd={end_date}&searchPage=1&pageNumber=1&searchDisplay=10000&searchTab=R100&searchSort=date&searchRecom=A")
  remDr$navigate(url)
  Sys.sleep(3)
  
  res_json <- remDr$findElement(using = "css selector", "pre")$getElementText()
  
  tmp <- tempfile()
  write(res_json[[1]], file = file(tmp, encoding = "UTF-8"))
  
  fnlist_new <- read_json(tmp) %>% 
    purrr::pluck("resultSet", "result") %>% 
    purrr::pluck(1, 11) %>%
    map_dfr(as_tibble) %>%
    # 기업에서 기업분석만 필터링
    # R100이 아니라 searchTab을 R100.10으로 설정하면 됨...
    filter(BULLET_KIND == "R100,R100.10") %>%
    # IR협의회 기술분석 보고서 제외
    filter(NICK_NM != "") %>%
    # 영어 보고서 제외
    filter(ENG_YN == "N") %>%
    # 종목코드 없는 것 제외 (비상장 등)
    filter(ITEM_NM != "") %>%
    mutate(row_number = row_number())
  
  return(fnlist_new)
}

save_fnlist <- function(fnlist_new) {
  con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
  
  fnlist <- tbl(con, "FN_COMP_LIST")
  
  fnlist_new_filtered <- fnlist_new %>% 
    anti_join(fnlist, by = "BULLET_NO", copy = TRUE)
  
  dbWriteTable(con, "FN_COMP_LIST", fnlist_new_filtered, append =  TRUE)
  
  dbDisconnect(con)
}


# 코드 실행부분분
rD <- initialize_browser(chrome_ver = "95.0.4638.54", task_kill = TRUE)
# 한번 클릭해줘야 접속 되나?
login_fn(rD)

fnlist_new <- search_report_list(rD, year_month = "202111")

save_fnlist(fnlist_new)




# # Parameters ----
# chrome_ver <- "95.0.4638.54"
# start_date <- "20210801"
# end_date <- "20210930"
# 
# # Crawler ---- 

# # run this line in case port 4569 is already in use
# system("taskkill /im java.exe /f", intern=FALSE, ignore.stdout=FALSE)
# 
# 
# rD <- rsDriver(port= 4569L, browser = "chrome", chromever = chrome_ver)
# 
# remDr <- rD$client
# 
# remDr$navigate("https://www.fnguide.com/")
# Sys.sleep(5)
# id <- remDr$findElement(using = "name", "MemberID")
# id$sendKeysToElement(list("research1"))
# Sys.sleep(1)
# pwd <- remDr$findElement(using = "name", "PassWord")
# pwd$sendKeysToElement(list("research2"))
# Sys.sleep(1)
# btn <- remDr$findElement(using = "css selector", value = ".btn--login")
# btn$sendKeysToElement(list(key = "enter"))
# Sys.sleep(1)
# remDr$findElement("class name", "btn--back")$sendKeysToElement(list(key = "enter"))
# 

# url <- str_glue("https://www.fnguide.com/api/fgdr/SearchTotalDiquest?searchType=FnResearch&searchStart={start_date}&searchEnd={end_date}&searchPage=1&pageNumber=1&searchDisplay=10000&searchTab=R100&searchSort=date&searchRecom=A")
# remDr$navigate(url)
# Sys.sleep(3)
# 
# res_json <- remDr$findElement(using = "css selector", "pre")$getElementText()
# 
# tmp <- tempfile()
# write(res_json[[1]], file = file(tmp, encoding = "UTF-8"))
# 
# fnlist_new <- read_json(tmp) %>% 
#   purrr::pluck("resultSet", "result") %>% 
#   purrr::pluck(1, 11) %>%
#   map_dfr(as_tibble) %>%
#   # 기업에서 기업분석만 필터링
#   # R100이 아니라 searchTab을 R100.10으로 설정하면 됨...
#   filter(BULLET_KIND == "R100,R100.10") %>%
#   # IR협의회 기술분석 보고서 제외
#   filter(NICK_NM != "") %>%
#   # 영어 보고서 제외
#   filter(ENG_YN == "N") %>%
#   # 종목코드 없는 것 제외 (비상장 등)
#   filter(ITEM_NM != "") %>%
#   mutate(row_number = row_number())
# 
# con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
# fnlist <- tbl(con, "FN_COMP_LIST")
# 
# 
# fnlist_new_filtered <- fnlist_new %>% 
#   anti_join(fnlist, by = "BULLET_NO", copy = TRUE)
# 
# dbWriteTable(con, "FN_COMP_LIST", fnlist_new_filtered, append =  TRUE)
# 
# dbDisconnect(con)

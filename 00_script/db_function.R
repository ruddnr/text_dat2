library(odbc)
library(dbplyr)

write_table_to_database <- function(dat, table_name, append = TRUE) {
  con <- dbConnect(RSQLite::SQLite(), "TextDat.sqlite")
  
  dbWriteTable(con, table_name, dat, append = append)
  
  dbDisconnect(con)
}
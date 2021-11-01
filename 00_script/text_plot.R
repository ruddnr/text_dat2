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
library(RColorBrewer)

# LOAD data ----
dfm_list <- readRDS("./dfm_list.RDS")

report_dfm <- dfm_list[["report_dfm"]]
news_dfm <- dfm_list[["news_dfm"]]
filings_dfm <- dfm_list[["filings_dfm"]]

# Calculate Cosine Similarity ----

# Calc indv similarity
calc_simil <- function(source_dfm, type, query_word, method) {
  
  query_word_dfm <- dfm(query_word, tolower = FALSE) %>% 
    dfm_match(features = featnames(source_dfm))
  
  if (method == "simple") {
    res <- source_dfm %*% t(query_word_dfm) %>% 
      tidy() %>% 
      as_tibble() 
  } else {
    res <- textstat_simil(source_dfm, 
                          query_word_dfm, 
                          margin = "documents", 
                          method = method) %>% 
      as_tibble() 
  }
  
  res <- res %>%   
    select(name = 1, value = 3) %>% 
    arrange(desc(value)) %>% 
    
    # Min-max scaling
    mutate(value = (value - min(value)) / (max(value) - min(value))) %>%
    
    mutate(type = type) %>% 
    
    slice(1:50)
  
  return(res)
}

# Calc Simlilarity for three difference sources
calc_simil_tot <- function(query_word) {
  
  tribble(
    ~source_dfm, ~type, ~method,
    report_dfm, "report", "simple",
    news_dfm, "news", "simple",
    filings_dfm, "filings", "cosine"
  ) %>% 
    pmap_dfr(.f = calc_simil, query_word = query_word)
  
}



# res <- calc_simil_tot("친환경")


plot_indv_sources <- function(res) {
  
  # my_colors <- colorRampPalette(brewer.pal(11, "Spectral"))(150) %>% rev()
  
  g <- res %>% 
    mutate(name = reorder_within(name, value, type)) %>% 
    mutate(name2 = str_split(name, "___", simplify = TRUE)[,1],
           text = str_glue("{name2} : {round(value, digits = 2)}")) %>% 
    ggplot(aes(name, value, fill = name, text = text)) + 
    geom_col() +
    coord_flip() + 
    theme_minimal()+
    facet_wrap(~type, nrow = 1, scales = "free_y") + 
    scale_x_reordered() + 
    theme(legend.position = "null") +
    labs(x = "", y = "") +
    # scale_fill_manual(values = my_colors) 
    scale_fill_viridis_d()
  
  ggplotly(g, tooltip = c("text")) %>% 
    hide_legend()
}


plot_votes <- function(res) {
  
  res_vote <- res %>% 
    group_by(name) %>% 
    summarise(score = sum(value), 
              vote = n()) %>% 
    ungroup() %>% 
    arrange(-vote, -score)
  
  # my_colors <- colorRampPalette(brewer.pal(11, "Spectral"))(90) %>% rev()
  
  g_vote <- res_vote %>% 
    pivot_longer(-name, names_to = "type", values_to = "value") %>% 
    
    group_by(type) %>% 
    slice(1:30) %>% 
    ungroup() %>% 
    
    mutate(name = reorder_within(name, value, type)) %>% 
    mutate(name2 = str_split(name, "___", simplify = TRUE)[,1],
           text = str_glue("{name2} : {round(value, digits = 2)}")) %>% 
    
    ggplot(aes(name, value, fill = name, text = text)) + 
    geom_col() +
    coord_flip() + 
    theme_minimal()+
    facet_wrap(~type, nrow = 1, scales = "free") + 
    scale_x_reordered() + 
    theme(legend.position = "null") +
    labs(x = "", y = "") +
    scale_fill_viridis_d()
  
  ggplotly(g_vote, tooltip = "text") %>% 
    hide_legend()
  
}

# dump(c("calc_simil", "calc_simil_tot", "plot_indv_sources", "plot_votes"), "ploting_functions.R")
# 
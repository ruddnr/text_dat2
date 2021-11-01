calc_simil <-
function(tfidf_dfm, type, query_word) {
  
  query_word_dfm <- dfm(query_word, tolower = FALSE) %>% 
    dfm_match(features = featnames(tfidf_dfm))
  
  res <- textstat_simil(tfidf_dfm, 
                 query_word_dfm, 
                 margin = "documents", 
                 method = "cosine") %>% 
    as_tibble() %>% 
    arrange(desc(cosine)) %>% 
    select(name = 1, value = 3) %>% 
    
    # Min-max scaling
    mutate(value = (value - min(value)) / (max(value) - min(value))) %>% 
    
    mutate(type = type) %>% 
    
    slice(1:50)
  
  return(res)
}
calc_simil_tot <-
function(query_word) {
  
  tribble(
    ~tfidf_dfm, ~type,
    report_tfidf, "report",
    news_tfidf, "news",
    filings_tfidf, "filings"
  ) %>% 
    pmap_dfr(.f = calc_simil, query_word = query_word)
  
}
plot_indv_sources <-
function(res) {
  
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
    # scale_fill_manual(values = my_colors) 
    scale_fill_viridis_d()
  
  ggplotly(g, tooltip = c("text")) %>% 
    hide_legend()
}
plot_votes <-
function(res) {
  
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

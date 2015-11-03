library(rvest)
lotr_project <- read_html("C:/Users/Juan De Dios Santos/Desktop/LOTR_html.txt") 
lotr_project%>% 
  html_nodes("#481") %>%
  html_text()

lotr_project %>% 
  html_nodes("#481") %>%
  html_attr("class")

# Last character is 952
library(rvest)
html_data <- read_html("~/Development/lotr-names-classification/lotr-names-html.html")
characters_data <- data.frame(name = character(0), race = character(0),
                              stringsAsFactors = FALSE)

for (i in 1:952){
  
  # Get the name
  name <- html_data %>%
    html_nodes(paste0('#', i)) %>%
    html_text()
  
  race_and_class <- strsplit(html_data %>%
    html_nodes(paste0('#', i)) %>%
    html_attr('class'), split = ' ')
  
  if (length(name) > 0) {
    characters_data[i,] <- list(name, race_and_class[[1]][length(race_and_class[[1]]) - 1])
  }
}
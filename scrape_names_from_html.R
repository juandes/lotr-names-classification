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

# Remove rows with NA
characters_data <- na.omit(characters_data)
# Remove rows where name is '?'
characters_data <- characters_data[grep('\\?', characters_data$name, invert = TRUE), ]
# Remove \n from the names
characters_data$name <- sub('\n', '', characters_data$name)
# Remove the prefix '1st', '2nd', etc.
characters_data$name <- sub('[0-9]?[0-9][a-z]{2}', '', characters_data$name)
table(characters_data$race)
# Subset the races that have a significant number of entries
characters_data <- characters_data[characters_data$race == 'Ainur' | 
                        characters_data$race == 'Dwarf' |
                        characters_data$race == 'Elf' |
                        characters_data$race == 'Half-elf' |
                        characters_data$race == 'Hobbit' |
                        characters_data$race == 'Man', ]
# Change the half-elves for elves (sorry Elrond)
characters_data$race[characters_data$race == 'Half-elf'] <- 'Elf'
# Remove trailing spaces
characters_data$name <- sub('[ \t]+$', '', characters_data$name)
# Remove an entry where the name is 'Others'
characters_data <- characters_data[characters_data$name != 'Others' & 
                                     characters_data$name != 'Master of La...', ]

# The names of the characters on this dataframe won't have any surnames or
# numbers on their name; we'll keep just the first name.
characters_no_surnames <- characters_data

# Regex to remove everything after the first whitespace
characters_no_surnames$name <- sub(' .*', '', characters_no_surnames$name)

write.csv(characters_no_surnames, file = 'characters_no_surnames.csv', row.names = FALSE)
write.csv(characters_data, file = 'characters_data.csv', row.names = FALSE)

require(dplyr)
characters_no_surnames <- read.csv("~/Development/lotr-names-classification/characters_no_surnames.csv")
characters.no.ainur <- filter(characters_no_surnames, race != "Ainur")
write.csv(characters.no.ainur, file = "characters_no_ainur.csv", row.names = FALSE)

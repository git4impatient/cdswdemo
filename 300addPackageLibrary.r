thisIsAString <- "Demonstrate how to install packages and use libraries in R"
print ( thisIsAString )
install.packages("ggplot2")
library ( ggplot2)
install.packages("datasets")
library (datasets)
library(MASS)
ggplot(geyser) + geom_histogram(aes(x = duration))

# these take a long time to install - please do not run on the demo cluster
#install.packages("maps")
#library(devtools)
#devtools::install_github("rstudio/sparklyr")

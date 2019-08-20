#
# once you install a package, it will be there the next time you work in the project
# even though you stop the sessoin, a new session will still have the packages ready and waiting for you
#
remove.packages("A3")
a <- available.packages()
#print (a)
head(a,n=5L,3)

# show failure to load package, go back, install A3, then it works
# install.packages("A3")
library ("A3")


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

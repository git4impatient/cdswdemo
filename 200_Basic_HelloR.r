thisIsAString <- "hello from R in cdsw"
print ( thisIsAString )

x <- seq(-pi,pi,0.1)
plot(x, sin(x))

# do not install sparklyr on the demo cluster 
# unless you have a specific
# use case for it
# it takes a long time to install
#
# to paralellize R 
# devtools::install_github("rstudio/sparklyr")

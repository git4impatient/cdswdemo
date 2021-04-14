# make sure there is no .git directory in the CDSW tree
# or v1.4.0 will throw error
# failed to initiate the model build
#
import sys
import cdsw

args = len(sys.argv) - 1  
sum = 0
x = 1

while (args >= x): 
    print ("Argument %i: %s" % (x, sys.argv[x]))
    sum = sum + int(sys.argv[x])
    x = x + 1
cdsw.track_metric("number of args", args ) 
cdsw.track_metric("sum of args is", sum)
print ("Sum of the numbers is: %i." % sum)


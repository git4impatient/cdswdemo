# make sure there is no .git directory in the CDSW tree
# or v1.4.0 will throw error
# failed to initiate the model build
#
def add(args):
  result = args["a"] + args["b"]
  return result
  

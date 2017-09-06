# demonstrate how to pip install
# from command line
# this will fail without
# pip install seaborn
# optional can use 
# !pip install seaborn
# in the workbench
#
# once you install a package, it will be there the next time you work in the project
# even though you stop the sessoin, a new session will still have the packages ready and waiting for you
#

import seaborn as sns; sns.set(style="ticks", color_codes=True)
iris = sns.load_dataset("iris")
g = sns.pairplot(iris ,  hue="species" )

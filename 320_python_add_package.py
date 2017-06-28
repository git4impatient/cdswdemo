# demonstrate how to pip install
# from command line
# this will fail without
# pip install seaborn
# optional can use 
# !pip install seaborn
# in the workbench
import seaborn as sns; sns.set(style="ticks", color_codes=True)
iris = sns.load_dataset("iris")
g = sns.pairplot(iris ,  hue="species" )

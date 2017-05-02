# demonstrate how to pip install
# from command line
# this will fail without pip install seaborn
import seaborn as sns; sns.set(style="ticks", color_codes=True)
iris = sns.load_dataset("iris",  hue="species" )
g = sns.pairplot(iris)

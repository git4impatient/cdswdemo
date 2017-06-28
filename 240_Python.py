# example from the python project template
# Setup
# -----

from pandas_datareader import data as web
import matplotlib.pyplot as plt

# Basic Data Manipulation
# -----------------------
#
# Read data from Yahoo stock API using [pandas](http://bit.ly/HHNQqn).

start = datetime.datetime(2014, 1, 1)
end = datetime.datetime(2014, 9, 1)
stocks = web.DataReader(['GOOG','AAPL','TSLA'], 'yahoo', start, end)
stocks['Open']['TSLA'].plot(title="The Rise of Tesla")

# Using IPython's Rich Display System
# -----------------------------------
#
# IPython has a [rich display system](bit.ly/HHPOac) for 
# interactive widgets.

from IPython.display import IFrame
from IPython.core.display import display

# Define a google maps function.
def gmaps(query):
  url = "https://maps.google.com/maps?q={0}&output=embed".format(query)
  display(IFrame(url, '700px', '450px'))

gmaps("Golden Gate Bridge")

# Worker Engines
# -----------------
#
# You can launch worker engines to distribute your work across a cluster. 
# Uncomment the following to launch two workers with 2 cpu cores and 0.5GB
# memory each.

# import cdsw
# workers = cdsw.launch_workers(n=2, cpu=0.2, memory=0.5, code="print 'Hello from a CDSW Worker'")


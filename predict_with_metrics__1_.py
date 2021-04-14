# Read the fitted model (fit.py) from the file model.pkl
# and define a function that uses the model to predict 
# petal width from petal length

# This version of the predict function is wrapped with the
# model_metrics decorator, enabling it to call track_metrics
# to store mathematical metrics associated with each
# prediction.

import pickle
import cdsw

model = pickle.load(open('model.pkl', 'rb'))

# The model_metrics decorator equips the predict function to
# call track_metrics. It also changes the return type. If the
# raw predict function returns a value "result", the wrapped
# function will return eg 
# {
#   "uuid": "612a0f17-33ad-4c41-8944-df15183ac5bd",
#   "prediction": "result"
# }
# The UUID can be used to query the stored metrics for this
# prediction later.
@cdsw.model_metrics
def predict(args):
  # Track the input.
  cdsw.track_metric("input", args)

  # If this model involved features, ie transformations of the
  # raw input, they could be tracked as well.
  # cdsw.track_metric("feature_vars", {"a":1,"b":23})

  petal_length = float(args.get('petal_length'))
  result = model.predict([[petal_length]])

  # Track the output.
  cdsw.track_metric("predict_result", result[0][0])
  return result[0][0]
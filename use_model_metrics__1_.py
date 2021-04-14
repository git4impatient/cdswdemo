import cdsw
import time
from sklearn import datasets
import numpy as np

# This script demonstrates the usage of several model metrics-
# related functions:
# - call_model: Calls a model deployed on CDSW as an HTTP endpoint.
# - read_metrics: Reads metrics tracked for all model predictions
#   made within a time window. This is useful for  doing analytics 
#   on the tracked metrics.
# - track_delayed_metrics: Adds metrics for a given prediction 
#   retrospectively, after the prediction has already been made.
#   Common examples of such metrics are ground truth and various
#   per-prediction accuracy metrics.
# - track_aggregate_metrics: Adds metrics for a set or batch of
#   predictions within a given time window, not an individual 
#   prediction. Common examples of such metrics are mean or 
#   median accuracy, and various measures of drift.

# This script can be used in a local development mode, or in
# deployment mode. To use it in deployment mode, please: 
# - Set dev = False
# - Create a model deployment from the function 'predict' in
#   predict_with_metrics.py 
# - Obtain the model deployment's CRN from the model's overview
#   page and the model's access key from its settings page and 
#   paste them below.
# - If you selected "Enable Authentication" when creating the
#   model, then create a model API key from your user settings 
#   page and paste it below as well.

dev = True

# Conditionally import the predict function only if we are in
# dev mode
if dev:
    from predict_with_metrics import predict

if dev:
    model_deployment_crn="crn:cdp:ml:USE YOUR CRN"
    
    
    # update modelDeploymentCrn
    model_access_key="muse your keyh"
else: 
    # The model deployment CRN can be obtained from the model overview
    # page.
    model_deployment_crn=None 
    if model_deployment_crn is None:
        raise ValueError("Please set a valid model deployment Crn")

    # The model access key can be obtained from the model settings page.
    model_access_key=None
    if model_access_key is None:
        raise ValueError("Please set the model's access key")

    # You can create a models API key from your user settings page.
    # Not required if you did not select "Enable Authentication"
    # when deploying the model. In that case, anyone with the
    # model's access key can call the model.
    user_api_key = None

# First, we use the call_model function to make predictions for 
# the held-out portion of the dataset in order to populate the 
# metrics database.
iris = datasets.load_iris()
test_size = 10

# This is the input data for which we want to make predictions.
# Ground truth is generally not yet known at prediction time.
score_x = iris.data[:test_size, 2].reshape(-1, 1) # Petal length

# Record the current time so we can retrieve the metrics
# tracked for these calls.
start_timestamp_ms=int(round(time.time() * 1000))

uuids = []
predictions = []
for i in range(len(score_x)):
    output = predict({"petal_length": score_x[i][0]})
    # Record the UUID of each prediction for correlation with ground truth.
    uuids.append(output["uuid"])
    predictions.append(output["prediction"])

# Record the current time.
end_timestamp_ms=int(round(time.time() * 1000))

# We can now use the read_metrics function to read the metrics we just
# generated into the current session, by querying by time window.
data = cdsw.read_metrics(model_deployment_crn=model_deployment_crn,
            start_timestamp_ms=start_timestamp_ms,
            end_timestamp_ms=end_timestamp_ms, dev=dev)
## Print out Model Serving Metrics
print(data)
data = data['metrics']

# Now, ground truth is known and we want to track the true value
# corresponding to each prediction above.
score_y = iris.data[:test_size, 3].reshape(-1, 1) # Observed petal width

# Track the true values alongside the corresponding predictions using
# track_delayed_metrics. At the same time, calculate the mean absolute
# prediction error.
mean_absolute_error = 0
n = len(score_y)
for i in range(n):
    ground_truth = score_x[i][0]
    cdsw.track_delayed_metrics({"actual_result":ground_truth}, uuids[i], dev=dev)

    absolute_error = np.abs(ground_truth - predictions[i])
    mean_absolute_error += absolute_error / n

# Use the track_aggregate_metrics function to record the mean absolute
# error within the time window where we made the model calls above.
cdsw.track_aggregate_metrics(
    {"mean_absolute_error": mean_absolute_error}, 
    start_timestamp_ms, 
    end_timestamp_ms, 
    model_deployment_crn=model_deployment_crn,
    dev=dev
)

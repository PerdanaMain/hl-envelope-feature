from requests import get # type: ignore
from model import *
from datetime import timedelta
from statsmodels.tsa.arima.model import ARIMA  # type: ignore
from model import *
from joblib import Parallel, delayed  # type: ignore # Untuk paralelisasi
import numpy as np # type: ignore
import numpy as np  # type: ignore
import itertools

def evaluate_arima_model(data, order):
    """Evaluasi model ARIMA untuk parameter tertentu dan mengembalikan AIC."""
    try:
        model = ARIMA(data, order=order)
        model_fit = model.fit()
        return model_fit.aic, order
    except:
        return float("inf"), order

def find_best_arima(data, p_range, d_range, q_range, n_jobs=-1):
    """Grid search untuk menemukan parameter ARIMA terbaik menggunakan paralelisasi."""
    pdq_combinations = list(itertools.product(p_range, d_range, q_range))
    results = Parallel(n_jobs=n_jobs)(
        delayed(evaluate_arima_model)(data, order) for order in pdq_combinations
    )
    best_result = min(results, key=lambda x: x[0])
    return best_result[1]


def train_arima_model(data, order):
    """
    Train ARIMA model with specified order
    
    Args:
        data: Training data
        order: ARIMA order (p,d,q)
    
    Returns:
        Fitted ARIMA model
    """
    model = ARIMA(data, order=order)
    return model.fit()

def execute_arima(part_id, features_id):
    data = get_feature_values(part_id, features_id)
    if len(data) == 0:
        print(f"No data found for part_id: {part_id}, features_id: {features_id}")
        return

    # Ekstrak value dan timestamp
    raw_values = [val[3] for val in data] 
    timestamps = pd.to_datetime([val[2] for val in data])

    # Konversi nilai ke float
    X = []
    for value in raw_values:
        try:
            X.append(float(value))
        except (ValueError, TypeError):
            X.append(0.0)

    if len(X) < 10:
        print(f"Insufficient data for ARIMA: {len(X)} records found.")
        return

    # Buat DataFrame dengan timestamp sebagai index
    df = pd.DataFrame({
        'value': X,
        'timestamp': timestamps
    }).set_index('timestamp')

    # Resampling data ke format per jam
    df_hourly = df.resample('h').mean().fillna(method='ffill')

    # Gunakan 50% data awal untuk pencarian parameter ARIMA
    subset = df_hourly['value'].iloc[:int(len(df_hourly) * 0.5)]
    best_order = find_best_arima(
        subset.values, 
        p_range=range(0, 3), 
        d_range=range(0, 2), 
        q_range=range(0, 3)
    )
    print(f"Best ARIMA order for part_id {part_id}, features_id {features_id}: {best_order}")

    # Membagi data menjadi pelatihan dan pengujian
    split_index = int(len(df_hourly) * 0.66)
    train = df_hourly['value'].iloc[:split_index]

    # Train model
    model_fit = train_arima_model(train.values, best_order)

    # Generate future dates and times
    jakarta_tz = pytz.timezone("Asia/Jakarta")
    last_timestamp = df_hourly.index[-1]
    
    # Ensure we start from the next hour
    start_forecast = last_timestamp + timedelta(hours=1)
    future_hours = pd.date_range(
        start=start_forecast,
        periods=24 * 7,  # 7 days * 24 hours
        freq='h'
    )

    # Predict next 7 days hourly
    future_forecast = model_fit.forecast(steps=len(future_hours))

    # Format timestamps for database
    future_timestamps = [ts.strftime("%Y-%m-%d %H:00:00") for ts in future_hours]

    # Delete old predictions
    delete_predicts(part_id, features_id)

    # Save new predictions
    create_predict(part_id, features_id, future_forecast, future_timestamps)

    print(f"ARIMA prediction completed for part_id: {part_id}")
    print(f"Forecast period: {future_timestamps[0]} to {future_timestamps[-1]}")
    print(f"Number of hourly predictions: {len(future_forecast)}")

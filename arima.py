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


def train_arima_model(train_data, order):
    """Melatih model ARIMA dengan parameter yang diberikan."""
    model = ARIMA(train_data, order=order)
    return model.fit()

def execute_arima(equipment_id, features_id):
    data = get_feature_values(equipment_id, features_id)
    if len(data) == 0:
        print(f"No data found for equipment_id: {equipment_id}, features_id: {features_id}")
        return

    # Ekstrak value dan timestamp
    raw_values = [val[3] for val in data] 
    timestamps = [val[2] for val in data] 

    X = []
    for value in raw_values:
        try:
            X.append(float(value))
        except (ValueError, TypeError):
            # Jika konversi gagal, masukkan nilai default (0) atau nilai lainnya
            X.append(0.0)

    if len(X) < 10:  # Minimum data untuk ARIMA
        print(f"Insufficient data for ARIMA: {len(X)} records found.")
        return

    # Gunakan 50% data awal untuk pencarian parameter ARIMA
    subset = X[: int(len(X) * 0.5)]
    best_order = find_best_arima(
        subset, p_range=range(0, 3), d_range=range(0, 2), q_range=range(0, 3)
    )
    print(f"Best ARIMA order for equipment_id {equipment_id}, features_id {features_id}: {best_order}")

    # Membagi data menjadi pelatihan dan pengujian
    split_index = int(len(X) * 0.66)
    train, test = X[:split_index], X[split_index:]

    model_fit = train_arima_model(train, best_order)

    n_days = 7 * 24 # 7 hari ke depan per jam
    future_forecast = model_fit.forecast(steps=n_days)

    # Membuat timestamp untuk 7 hari ke depan
    last_date = timestamps[-1]
    future_timestamps = [(last_date + timedelta(days=i + 1)).strftime("%Y-%m-%d") for i in range(n_days)]
    
    # # delete old prediction
    delete_predicts(equipment_id, features_id)

    # # Simpan hasil prediksi
    create_predict(equipment_id, features_id, future_forecast, future_timestamps)

    print(f"ARIMA prediction for equipment_id: {equipment_id} finished.")
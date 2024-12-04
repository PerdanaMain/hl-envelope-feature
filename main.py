import matplotlib.pyplot as plt # type: ignore
import numpy as np # type: ignore
import pandas as pd # type: ignore
from datetime import datetime, timedelta


def find_signal_envelopes(signal, chunk_size=1, split_at_mean=False):
    """
    Find high and low envelopes of a signal.
    """
    diff_signal = np.diff(np.sign(np.diff(signal)))
    min_indices = np.nonzero(diff_signal > 0)[0] + 1
    max_indices = np.nonzero(diff_signal < 0)[0] + 1

    if split_at_mean:
        signal_mean = np.mean(signal)
        min_indices = min_indices[signal[min_indices] < signal_mean]
        max_indices = max_indices[signal[max_indices] > signal_mean]

    min_indices = min_indices[
        [
            i + np.argmin(signal[min_indices[i : i + chunk_size]])
            for i in range(0, len(min_indices), chunk_size)
        ]
    ]
    max_indices = max_indices[
        [
            i + np.argmax(signal[max_indices[i : i + chunk_size]])
            for i in range(0, len(max_indices), chunk_size)
        ]
    ]

    return min_indices, max_indices


def generate_datetime_data():
    """
    Generate data dari 21 Oktober 2024 sampai hari ini dengan 24 record per hari
    """
    start_date = datetime(2024, 10, 21)
    end_date = datetime(2024, 12, 4)

    datetimes = []
    signals = []

    current_date = start_date
    while current_date <= end_date:
        for hour in range(24):
            current_datetime = current_date + timedelta(hours=hour)
            datetimes.append(current_datetime)
            # Membuat signal yang lebih realistis dengan komponen periodik
            t = len(signals) / 24  # waktu dalam hari
            signal_value = 5 + 2 * np.sin(2 * np.pi * t / 7) + np.random.normal(0, 0.5)
            signals.append(signal_value)
        current_date += timedelta(days=1)

    return pd.DataFrame({"datetime": datetimes, "signal": signals})


def plot_signals_with_envelopes(df):
    """
    Plot signal data dengan high dan low envelopes
    """
    signal_values = df["signal"].values
    min_indices, max_indices = find_signal_envelopes(signal_values)

    plt.figure(figsize=(15, 7))

    # Plot signal asli
    plt.plot(df["datetime"], signal_values, "b-", alpha=0.5, label="Signal")

    # Plot envelopes
    plt.plot(
        df["datetime"].iloc[max_indices],
        signal_values[max_indices],
        "g.",
        label="High Envelope",
        markersize=10,
    )
    plt.plot(
        df["datetime"].iloc[min_indices],
        signal_values[min_indices],
        "r.",
        label="Low Envelope",
        markersize=10,
    )

    plt.title("Signal Data dengan High/Low Envelopes (21 Oktober - 4 Desember 2024)")
    plt.xlabel("Tanggal dan Waktu")
    plt.ylabel("Nilai Signal")
    plt.grid(True, alpha=0.3)

    # Format x-axis
    plt.gcf().autofmt_xdate()

    plt.legend()
    plt.tight_layout()
    plt.show()


def main():
    # Generate data
    df = generate_datetime_data()

    # Tampilkan informasi data
    print("Info Dataset:")
    print(f"Jumlah total record: {len(df)}")
    print(
        f"Rentang waktu: {df['datetime'].min().strftime('%Y-%m-%d %H:%M')} sampai {df['datetime'].max().strftime('%Y-%m-%d %H:%M')}"
    )
    print("\nStatistik Signal:")
    print(df["signal"].describe())

    # Plot signal dengan envelopes
    # plot_signals_with_envelopes(df)


if __name__ == "__main__":
    main()

import matplotlib.pyplot as plt  # type: ignore
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from datetime import datetime, timedelta
from model import *


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


def plot_signals_with_envelopes(df, part_name):
    """
    Plot signal data dengan high dan low envelopes
    """
    signal_values = df["value"].values
    min_indices, max_indices = find_signal_envelopes(signal_values)

    plt.figure(figsize=(15, 7))

    # Plot signal asli
    plt.plot(df["datetime"], signal_values, "b-", alpha=0.5, label="Signal")

    # Plot envelopes
    plt.plot(
        df["datetime"].iloc[max_indices],
        signal_values[max_indices],
        "g-",
        label="High Envelope",
        markersize=10,
    )
    plt.plot(
        df["datetime"].iloc[min_indices],
        signal_values[min_indices],
        "r-",
        label="Low Envelope",
        markersize=10,
    )

    plt.title(f"Signal Data dengan High/Low Envelopes: {part_name}")
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
    data = get_envelope_values(part_id="0f6c87be-65a7-480f-8121-f2bcc05ec407")
    part = get_part(part_id="0f6c87be-65a7-480f-8121-f2bcc05ec407")

    print(part)

    # Tampilkan informasi data
    if not data:
        print("No data found")
        return

    # Convert to DataFrame if not already
    df = pd.DataFrame(data, columns=["value", "datetime"])

    # Tampilkan informasi data
    print("Info Dataset:")
    print(f"Jumlah total record: {len(df)}")
    print(f"5 data teratas:\n{df.head()}")
    print(f"Rentang waktu: {df['datetime'].min()} sampai {df['datetime'].max()}")

    # Plot signal dengan envelopes
    plot_signals_with_envelopes(df, part[0][1])


if __name__ == "__main__":
    main()

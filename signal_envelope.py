import numpy as np  # type: ignore


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
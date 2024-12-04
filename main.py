import matplotlib.pyplot as plt
import numpy as np


def hl_envelopes_idx(s, dmin=1, dmax=1, split=False):
    """
    Input :
    s: 1d-array, data signal from which to extract high and low envelopes
    dmin, dmax: int, optional, size of chunks, use this if the size of the input signal is too big
    split: bool, optional, if True, split the signal in half along its mean, might help to generate the envelope in some cases
    Output :
    lmin,lmax : high/low envelope idx of input signal s
    """

    # locals min
    lmin = (np.diff(np.sign(np.diff(s))) > 0).nonzero()[0] + 1
    # locals max
    lmax = (np.diff(np.sign(np.diff(s))) < 0).nonzero()[0] + 1

    if split:
        # s_mid is zero if s centered around x-axis or more generally mean of signal
        s_mid = np.mean(s)
        # pre-sorting of locals min based on relative position with respect to s_mid
        lmin = lmin[s[lmin] < s_mid]
        # pre-sorting of local max based on relative position with respect to s_mid
        lmax = lmax[s[lmax] > s_mid]

    # global min of dmin-chunks of locals min
    lmin = lmin[
        [i + np.argmin(s[lmin[i : i + dmin]]) for i in range(0, len(lmin), dmin)]
    ]
    # global max of dmax-chunks of locals max
    lmax = lmax[
        [i + np.argmax(s[lmax[i : i + dmax]]) for i in range(0, len(lmax), dmax)]
    ]

    return lmin, lmax


def main():
    t = np.linspace(0, 2 * np.pi, 100)
    s = 5 * np.cos(5 * t) * np.exp(-t) + np.random.rand(len(t))

    print("signal: ", s)
    print("time: ", t)

    print("time length: ", len(t))
    print("signal length: ", len(s))

    lmin, lmax = hl_envelopes_idx(s)

    # plot
    plt.plot(t, s, label="signal")
    plt.plot(t[lmin], s[lmin], "r", label="low")
    plt.plot(t[lmax], s[lmax], "g", label="high")
    plt.show()


if __name__ == "__main__":
    main()

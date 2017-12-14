import os
from dask import dataframe
import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
import seaborn as sns

import datetime

import dask

def  read_ddf(ddir, fbase):
    pass

def clip(ser, nstds=2):

    mn = ser.mean()
    std = ser.std()
    res = ser
    clip_min = mn - nstds*std
    res[res < clip_min] = clip_min

    clip_max = mn + nstds*std
    res[res > clip_max] = clip_max

    print ("%f.3  , %f.3 " % (res.min(), res.max()))
    print (res.mean())
    return res

def standardize(ser):
    mn = ser.mean()
    std = ser.std()
    #ser[np.isnan(ser)] = mn
    res = (ser - mn) / std
    return res

def main():
    tdir = "test"
    fbase = "model"
    fpath = os.path.join(tdir, fbase+".csv")
    df_orig = pd.DataFrame.from_csv(fpath)
    print (df_orig.head())
    print (df_orig.shape)
    if "is_churn" in df_orig.columns:
        del df_orig["is_churn"]
    ncols = [c for c in df_orig.columns
             if c != "msno" ]
    df_std = df_orig
    df_std_clip = df_orig
    for c in ncols:
        print (c)
        #df_orig.sort_values(by=c)
        #sns.kdeplot(df_orig[c], bw=0.5)
        if len(df_orig[c].unique()) > 2 and df_orig[c].max() > 1:
            df_std[c] = standardize(df_orig[c])
            if sum(df_std[c].isnull()) >= 1:
                miss_c = "%s_missing" % (c,)
                df_std[miss_c] = 0
                df_std[miss_c][df_std[c].isnull()] = 1
            df_std[c][df_std[c].isnull()] = 0
            df_std_clip[c] = clip(df_std[c])

        #sns.kdeplot(df_norm[c], bw=0.5)
        #plt.show()
        print ("done with %s" % (c,))

    fpath = os.path.join(tdir, "df_std.csv")
    df_std.to_csv(fpath, index=False)

    fpath = os.path.join(tdir, "df_std_clip.csv")
    df_std_clip.to_csv(fpath, index=False)

if __name__ == "__main__":
    print("__main__")
    main()
import os
from dask import dataframe
import pandas as pd
import numpy as np

import datetime

import dask

print(dask.__version__)


def csv2ddf(ddir, fname):
    fpath = os.path.join(ddir, fname)
    res = dataframe.read_csv(fpath)
    return res


def ddf2csv(ddir, fbase, ddf):
    fpath = os.path.join(ddir, "%s_*.csv" % (fbase,))
    temp = ddf
    temp.repartition(npartitions=1)
    temp.to_csv(fpath)
    return None


def df2csv(ddir, fbase, df):
    fpath = os.path.join(ddir, "%s.csv" % (fbase,))
    df.to_csv(fpath)
    return None


def days_before(x, default=datetime.date(2017, 3, 31)):
    res = -1
    if isinstance(x, datetime.date):
        td = default - x
        res = td.days
    return res


def months_before(x, default=datetime.date(2017, 3, 31)):
    res = -1
    if isinstance(x, datetime.date):
        td = default - x
        d = td.days
        res = d // 30
    return res


def int2date(x, default=datetime.date(2017, 3, 31)):
    res = default
    if x is not None and not np.isnan(x):
        try:
            day = int(x % 100)
            month = int((x // 100) % 100)
            year = int(x // 10000)
            res = datetime.date(year, month, day)
        except:
            print("bad date= %s" % (str(x)))
    return res


def fixup_gender(x):
    res = 0
    if x.lower() == "female":
        res = -1
    if x.lower() == "male":
        res = 1
    return res


def is_male(x):
    res = int(str(x) == "male")
    return res


def is_female(x):
    res = int(str(x) == "female")
    return res


def is_unknown_gender(x):
    res = int(str(x) not in ("male", "female"))
    return res


def is_reg_type_9(x):
    res = int(x == 9)
    return res


def is_reg_type_4(x):
    res = int(x == 4)
    return res


def is_reg_type_3(x):
    res = int(x == 3)
    return res


def is_reg_type_7(x):
    res = int(x == 7)
    return res


def is_reg_type_13(x):
    res = int(x == 13)
    return res


def is_reg_type(x, rt=13):
    res = int(x == rt)
    return res


def is_reg_type_unk(x):
    res = int(x not in (3, 4, 7, 9, 13))
    return res


def fixup_bd(x):
    res = x
    if x is None or np.isnan(x):
        res = 25
    elif x < 18:
        res = 18
    elif x > 70:
        res = 70
    return res


def fixup_trans_count(x, cap=25):
    res = x
    if x is None or np.isnan(x):
        res = -1
    elif x > cap:
        res = cap
    return res


def is_eq(x, eq=41):
    res = int(x == eq)
    return res


def not_eq(x, neq=41):
    res = int(x != neq)
    return res


def fixup_log_count(x):
    res = x
    if x is None or np.isnan(x):
        res = -1
    elif x > 25:
        res = 25
    return res

def do_members(base, rdir, mdir):
    # Members
    memv3 = csv2ddf(ddir=rdir, fname="members_v3.csv")
    mcols = ["msno", "bd", "gender", "registered_via", "registration_init_time"]
    mems = base.merge(memv3[mcols], how="left", on='msno')

    mems["bd"] = mems["bd"].apply(fixup_bd)

    mems["gen_male"] = mems["gender"].apply(is_male)
    mems["gen_female"] = mems["gender"].apply(is_female)
    mems["gen_unk"] = mems["gender"].apply(is_unknown_gender)

    mems["reg_via_3"] = mems["registered_via"].apply(is_reg_type, rt=3)
    mems["reg_via_4"] = mems["registered_via"].apply(is_reg_type, rt=4)
    mems["reg_via_7"] = mems["registered_via"].apply(is_reg_type, rt=7)
    mems["reg_via_9"] = mems["registered_via"].apply(is_reg_type, rt=9)
    mems["reg_via_13"] = mems["registered_via"].apply(is_reg_type, rt=13)

    mems["reg_via_unk"] = mems["registered_via"].apply(is_reg_type_unk)

    mems['regdate'] = mems["registration_init_time"].apply(int2date)
    mems["days_since_reg"] = mems['regdate'].apply(days_before)
    mems["months_since_reg"] = mems['regdate'].apply(months_before)

    ddf2csv(ddir=mdir, fbase="mems", ddf=mems)
    memdf = mems.compute()
    df2csv(ddir=mdir, fbase="mems", df=memdf)
    del memdf
    return mems

def do_trans_last(base):
    pass


def main():
    rdir = "raw_data"
    mdir = "test"
    if not os.path.isdir(mdir):
        os.makedirs(mdir)

    base = csv2ddf(ddir=rdir, fname="sample_submission_v2.csv")
    ddf2csv(ddir=mdir, fbase="base", ddf=base)


    # init model

    mems = do_members(base=base, rdir=rdir, mdir=mdir)
    msg = "status: Done with Members"
    print (msg)
    # add to model
    model = base
    cols = ["msno", "bd", "gen_male", "gen_female", "gen_unk", "months_since_reg", "reg_via_unk"] # "registered_via", "months_since_reg"]
    model = model.merge(mems[cols], how="left", on="msno")
    del mems

    # transactions
    transv2 = csv2ddf(ddir=rdir, fname="transactions_v2.csv")

    trans = base.merge(transv2, how="left", on="msno")

    gbcols = ["msno"]
    datacols = ["transaction_date"]
    trans_max_date = trans[gbcols + datacols].groupby(by=gbcols).max().reset_index()
    trans_max_date['last_tdate'] = trans_max_date["transaction_date"].apply(int2date)
    # trans_max_date[trans_max_date.last_date is None] = datetime.date(2017, 3, 31)
    trans_max_date["days_since_last_trans"] = trans_max_date['last_tdate'].apply(days_before)
    ddf2csv(ddir=mdir, fbase="trans_max_date", ddf=trans_max_date)

    datacols = ["transaction_date"]
    trans_count = trans[gbcols + datacols].groupby(by=gbcols).count().reset_index()
    trans_count = trans_count.rename(columns={"transaction_date": "trans_count"})
    trans_count.fillna(-1)
    trans_count["tcount"] = trans_count["trans_count"].apply(fixup_trans_count)

    trans_count.fillna(-1)
    trans_count['no_trans'] = trans_count.tcount > 0
    ddf2csv(ddir=mdir, fbase="trans_count", ddf=trans_count)
    del trans_count

    msg = "status: Done with Trans count"
    print (msg)

    datacols = ["actual_amount_paid", "is_cancel"]
    trans_sum = trans[gbcols + datacols].groupby(by=gbcols).sum().reset_index()
    trans_sum.fillna(-1)
    ddf2csv(ddir=mdir, fbase="trans_sum", ddf=trans_sum)
    del trans_sum

    msg = "status: Done with Trans sum"
    print (msg)

    join_cols = ["msno", "transaction_date"]
    cols = ["msno",  "payment_method_id", "payment_plan_days", "plan_list_price",
            "is_auto_renew", "membership_expire_date"]
    trans_last = trans_max_date.merge(trans, how="inner", on=join_cols)
    trans_last.fillna(-1)

    trans_last['pmethod_41'] = trans_last.payment_method_id.apply(is_eq, eq=41)
    trans_last['pmethod_oth'] = trans_last.payment_method_id.apply(not_eq, neq=41)

    trans_last['plan_days_30'] = trans_last.payment_plan_days.apply(is_eq, eq=30)
    trans_last['pmethod_oth'] = trans_last.payment_plan_days.apply(not_eq, neq=30)
    ddf2csv(ddir=mdir, fbase="trans_last", ddf=trans_last)

    msg = "status: Done with Trans last"
    print (msg)

    # add to model
    cols = ["msno", "days_since_last_trans", "payment_plan_days",
            "actual_amount_paid"] # "payment_method_id"]
    model = model.merge(trans_last[cols], how="left", on="msno")
    del trans_last

    msg = "status: Done with Trans"
    print (msg)
    if False:
        trans_lastdf = trans_last.compute()
        print(trans_lastdf.head())
        df2csv(ddir=mdir, fbase="trans_last", df=trans_lastdf)
        del trans_last
        del trans_lastdf

    # user logs
    ulogsv2 = csv2ddf(ddir=rdir, fname="user_logs_v2.csv")
    ulogs = base.merge(ulogsv2, how="left", on="msno")
    # ddf2csv(ddir=mdir, fbase="ulogs", ddf=ulogs)

    gbcols = ["msno", ]
    datacols = ["date"]
    ulogs_max_date = ulogs[gbcols + datacols].groupby(by=gbcols).max().reset_index()
    ulogs_max_date['last_udate'] = ulogs_max_date["date"].apply(int2date)
    ulogs_max_date["days_since_last_log"] = ulogs_max_date['last_udate'].apply(days_before)
    ddf2csv(ddir=mdir, fbase="ulogs_max_date", ddf=ulogs_max_date)

    msg = "status: Done with ulogs max date"
    print (msg)

    gbcols = ["msno", ]
    datacols = ["date"]
    ulogs_count = ulogs[gbcols + datacols].groupby(by=gbcols).count().reset_index()
    ulogs_count = ulogs_count.rename(columns={"date": "ulogs_count"})
    ulogs_count.fillna(-1)

    ulogs_count['lcount'] = ulogs_count["ulogs_count"].apply(fixup_log_count)
    cols = ["msno", "lcount"]
    model = model.merge(ulogs_count[cols], how="left", on="msno")

    ddf2csv(ddir=mdir, fbase="ulogs_count", ddf=ulogs_count)

    msg = "status: Done with ulogs count"
    print (msg)

    gbcols = ["msno", ]
    datacols = ["num_985", "num_unq", "total_secs"]
    ulogs_sum = ulogs[gbcols + datacols].groupby(by=gbcols).sum().reset_index()
    ulogs_sum.fillna(-1)

    ddf2csv(ddir=mdir, fbase="ulogs_sum", ddf=ulogs_sum)

    msg = "status: Done with ulogs sum"
    print (msg)

    # add to model
    cols = ["msno",  "num_unq", "total_secs"]
    model = model.merge(ulogs_sum[cols], how="left", on="msno")
    ddf2csv(ddir=mdir, fbase="model", ddf=model)

    modeldf = model.compute()
    modeldf.drop_duplicates(subset="msno", inplace=True)
    df2csv(ddir=mdir, fbase="model", df=modeldf)

    print("done")


if __name__ == "__main__":
    print("__main__")
    main()

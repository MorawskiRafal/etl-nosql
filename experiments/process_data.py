import numpy as np
import pandas as pd



df_cass = pd.read_csv('run_cassandra.csv', names=['UDF', 'RK', 'RD', 'PC', 'czas'])
df_mongo = pd.read_csv('run_mongodb.csv', names=['UDF', 'RK', 'RD', 'PC', 'czas'])
df_postgres = pd.read_csv('run_postgres.csv', names=['UDF', 'RK', 'RD', 'PC', 'czas'])



def get_mean(df):
    df_data = []
    dfg = df.groupby(['UDF', 'RK', 'RD', 'PC'])

    for name, group in dfg:
        m = np.sort(list(group['czas']))[1:-1]
        mean = np.mean(m)
        df_data.append([*name, mean])

    return pd.DataFrame(df_data, columns=['UDF', 'RK', 'RD', 'PC', 'srednia'])


df_cass = get_mean(df_cass)
df_cass.PC = df_cass.PC / 1000
df_mongo = get_mean(df_mongo)
df_postgres = get_mean(df_postgres)
df_postgres.PC = df_postgres.PC / 1000

df_cass.insert(0, 'baza_danych', ['cassandra']*len(df_cass))
df_mongo.insert(0, 'baza_danych', ['mongodb']*len(df_mongo))
df_postgres.insert(0, 'baza_danych', ['postgresql']*len(df_postgres))

dfs_db = [df_cass, df_mongo, df_postgres]
db_merged = pd.concat(dfs_db, ignore_index=True)


def get_pushdown_decision(df, binary_ths=0.0):
    dfg = df.groupby(['baza_danych', 'RK', 'RD', 'PC'])
    df_data = []
    for name, group in dfg:
        udfs = [udf[0:-3] for udf in group['UDF']]
        udfs = list(set(udfs))
        for udf in udfs:
            this_udf = group['UDF'].str.startswith(udf)
            is_np = group[this_udf]['UDF'].str.endswith('np')
            np_ = group[this_udf][is_np]
            pp_ = group[this_udf][~is_np]

            if binary_ths > 0:
                pushdown_val = (pp_['srednia'].iloc[0] - np_['srednia'].iloc[0]) / np_['srednia'].iloc[0]
                pushdown = pushdown_val < -binary_ths
            else:
                pushdown = pp_['srednia'].iloc[0] / np_['srednia'].iloc[0]

            df_data.append([*name, udf, pushdown])

    return pd.DataFrame(df_data,
                        columns=['baza_danych', 'RK', 'PC', 'RD', 'UDF', 'etykieta'])


merge_b = get_pushdown_decision(db_merged, binary_ths=0.05)
merge_b4 = get_pushdown_decision(db_merged, binary_ths=0.4)
merge_r = get_pushdown_decision(db_merged)

merge_b.to_csv('data_binary.csv', index=False)
merge_b4.to_csv('data_binary_ths04.csv', index=False)
merge_r.to_csv('data_regression.csv', index=False)

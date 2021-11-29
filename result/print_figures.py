from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
import pandas as pd
import re


def fig_bars(names, np_, pp_, RK, RD, PC):
    fig = go.Figure(data=[
        go.Bar(name='UDF - bez push-down', x=names, y=np_['mean'], error_y=dict(
            type='data',
            array=np_['std'],
            visible=True)),
        go.Bar(name='UDF - z push-down', x=names, y=pp_['mean'], error_y=dict(
            type='data',
            array=pp_['std'],
            visible=True))
    ])
    fig.update_layout(xaxis_tickangle=-45)
    fig.update_layout(
        title=f'Eksperyment - rozmiar klastra: {RK},<br>'
              f'{"&#160;" * 14}rozmiar danych: {RD}[GB],<br>'
              f'{"&#160;" * 14}przyznana pamięć cache: {PC}[GB]',
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis_title='Czas[s]',
        xaxis_title='UDF',
        barmode='group',
        font=dict(
            family="Courier New",
            size=10,
            color="black",
        )
    )
    return fig


def create_bars(df, path, db):
    dfg2 = df.groupby(['RK', 'PC', 'RD'])

    img_id = -1
    for name, group in dfg2:
        is_np = group['UDF'].str.endswith('np')
        np_ = group[is_np]
        pp_ = group[~is_np]
        names = group['UDF'].apply(lambda x: x[:-3]).unique()
        params = group.iloc[0]
        RK, PC, RD = params['RK'], params['PC'], params['RD']

        img_id += 1
        img_id_bar = f"{db}_RK{RK}_PC{PC}_RD{RD}_bar"

        f_bar = fig_bars(names, np_, pp_, RK, RD, PC)
        f_bar.write_image(f"{path}/{img_id_bar}.pdf")
        group.to_csv(f"{path}/{img_id_bar}.csv", index=False)


c2p = {'RD': "RD", 'PC': 'PC', 'RK': 'RK'}
c2p2 = {'RD': "rozmiar danych", 'PC': 'pamięć cache', 'RK': 'rozmiar klastra'}
c2s = {'RD': "[GB]", 'PC': '[GB]', 'RK': ''}


def create_dd(df_c, c1, c2, c3, path, db):
    df_udf = df_c.groupby('type')
    gr_col_1 = c1
    gr_col_2 = c2
    gr_col_3 = c3
    img_id = -1
    for name, group in df_udf:
        group_RD = group.groupby([gr_col_1])

        for name1, group1 in group_RD:
            traces = dict()
            row = 1
            group_RD_ro = group1.groupby([gr_col_2])
            y_max = -float('inf')
            x_data = group1[gr_col_3]
            colors = ["red", "green", "blue", "#00B5F7", "goldenrod"]
            for name2, group2 in group_RD_ro:
                df_np = group2[group2.is_np]
                df_pp = group2[~group2.is_np]

                name_np = f"Brak push-down -<br> {c2p2[gr_col_2]}={name2}{c2s[gr_col_2]}"
                name_pp = f"Z push-down -<br> {c2p2[gr_col_2]}={name2}{c2s[gr_col_2]}"
                y_max = np.max([y_max, df_np['mean'].max(), df_np['mean'].min()])
                c = colors[0]
                colors = colors[1:] + colors[0:1]
                np_gos = go.Scatter(y=df_np['mean'], x=df_np[gr_col_3],
                                    name=name_np, marker_color=c,
                                    error_y=dict(
                                        type='data',
                                        array=df_np['std'],
                                        visible=True))
                pp_gos = go.Scatter(y=df_pp['mean'], x=df_pp[gr_col_3],
                                    name=name_pp, marker_color=c,
                                    error_y=dict(
                                        type='data',
                                        array=df_pp['std'],
                                        visible=True),
                                    line=dict(dash='dash'))
                traces[row] = [np_gos, pp_gos]
                row += 1

            fig = make_subplots(rows=1, cols=len(traces))

            for tk in traces.keys():
                for f in traces[tk]:
                    fig.add_trace(f, row=1, col=tk)
                    fig.update_xaxes(title_text=f"{c2p2[gr_col_3].capitalize()}{c2s[gr_col_3]}",
                                     row=1, col=tk, showgrid=True, tickvals=x_data, linecolor='black',
                                     gridcolor='rgba(230,230,230,250)')
                fig.update_yaxes(row=1, col=tk, range=[0, y_max * 1.1], showgrid=True,
                                 linecolor='black', gridcolor='rgba(230,230,230,250)')

            fig.update_layout(
                title=f'Eksperyment - UDF: {name},<br>'
                      f'{"&#160;" * 14}{c2p2[gr_col_1].capitalize()}: {name1}{c2s[gr_col_1]}',
                plot_bgcolor='rgba(0,0,0,0)',
                yaxis_title='Czas[s]',
                font=dict(
                    family="Courier New",
                    size=10,
                    color="black",
                )
            )

            img_id += 1
            img_name = f"{path}/{db}_{c1}{group1[c1].iloc[0]}_{c2}_{c3}_{name}"
            fig.write_image(img_name + ".pdf")
            group1.copy().drop(['type', 'is_np'], axis=1).sort_values([c1, c2, c3]).to_csv(img_name + ".csv", index=False)

def create_bb(df_c, c1, c2, c3, path, db):
    df_udf = df_c.groupby('type')
    gr_col_1 = c1
    gr_col_2 = c2
    gr_col_3 = c3


    for name, group in df_udf:
        group_RD = group.groupby([gr_col_1])
        for name1, group1 in group_RD:
            group_RD_ro = group1.groupby([gr_col_2])
            y_max = -float('inf')
            x_data = group1[gr_col_3]
            fig = make_subplots(rows=1, cols=1)
            colors = ["red", "green", "blue", "#00B5F7", "goldenrod"]
            colors_p = ["rgb(251,180,174)", "rgb(204,235,197)", "rgb(179,205,227)", "#87dfff", "rgb(255,255,204)"]

            for name2, group2 in group_RD_ro:
                df_np = group2[group2.is_np]
                df_pp = group2[~group2.is_np]

                name_np = f"Brak push-down -<br> {c2p2[gr_col_2]}={name2}{c2s[gr_col_2]}"
                name_pp = f"Z push-down -<br> {c2p2[gr_col_2]}={name2}{c2s[gr_col_2]}"
                y_max = np.max([y_max, df_np['mean'].max(), df_np['mean'].min()])
                c = colors[0]
                colors = colors[1:] + colors[0:1]
                cp = colors_p[0]
                colors_p = colors_p[1:] + colors_p[0:1]
                np_gos = go.Bar(y=df_np['mean'], x=df_np[gr_col_3],
                                name=name_np, marker_color=c,
                                error_y=dict(
                                    type='data',
                                    array=df_np['std'],
                                    visible=True))
                pp_gos = go.Bar(y=df_pp['mean'], x=df_pp[gr_col_3],
                                name=name_pp, marker_color=cp,
                                error_y=dict(
                                    type='data',
                                    array=df_pp['std'],
                                    visible=True))
                fig.add_trace(np_gos)
                fig.add_trace(pp_gos)

            fig.update_xaxes(title_text=f"{c2p2[gr_col_3].capitalize()}{c2s[gr_col_3]}",
                             showgrid=True, tickvals=x_data, linecolor='black',
                             gridcolor='rgba(230,230,230,250)')
            fig.update_yaxes(range=[0, y_max * 1.1], showgrid=True,
                             linecolor='black', gridcolor='rgba(230,230,230,250)')

            fig.update_layout(
                title=f'Eksperyment - UDF: {name},<br>'
                      f'{"&#160;" * 14}{c2p2[gr_col_1].capitalize()}: {name1}{c2s[gr_col_1]}',
                plot_bgcolor='rgba(0,0,0,0)',
                yaxis_title='Czas[s]',
                font=dict(
                    family="Courier New",
                    size=10,
                    color="black",
                )
            )

            img_name = f"{path}/{db}_{c1}{group1[c1].iloc[0]}_{c2}_{c3}_{name}"
            fig.write_image(img_name + ".pdf")
            group1.copy().drop(['type', 'is_np'], axis=1).sort_values([c1, c2, c3]).to_csv(img_name + ".csv", index=False)

def create_d(df, path, db):
    create_dd(df, 'RK', 'PC', 'RD', path, db)


def create_b(df, path, db):
    create_bb(df, 'RD', 'RK', 'PC', path, db)
    create_bb(df, 'RD', 'PC', 'RK', path, db)


if __name__ == '__main__':
    pd.set_option('display.max_colwidth', None)

    df_cass = pd.read_csv('run_cassandra.csv', names=['UDF', 'RK', 'RD', 'PC', 'czas'])
    df_mongo = pd.read_csv('run_mongodb.csv', names=['UDF', 'RK', 'RD', 'PC', 'czas'])
    df_postgres = pd.read_csv('run_postgres.csv', names=['UDF', 'RK', 'RD', 'PC', 'czas'])
    df_cass['PC'] = df_cass.PC.apply(lambda x: x / 1000)
    df_postgres['PC'] = df_postgres.PC.apply(lambda x: x / 1000)
    df_mongo['PC'] = df_mongo.PC.apply(lambda x: x * 3)

    for db in [{"db": "cassandra", "df": df_cass},
               {"db": "mongodb", "df": df_mongo},
               {"db": "postgres", "df": df_postgres}
               ]:
        df_all = db["df"]
        df_all_c = list()
        dfg = df_all.groupby(['UDF', 'RK', 'PC', 'RD'])
        for name, group in dfg:
            m = np.sort(list(group['czas']))[1:-1]
            mean = np.mean(m)
            std = np.std(m)
            q1 = np.quantile(m, 0.25)
            q3 = np.quantile(m, 0.75)
            median = np.median(m)
            type = name[0][:-3]
            is_np = True if name[0].endswith('np') else False
            df_all_c.append([*name, type, is_np, mean, std, q1, median, q3])

        df_cass = pd.DataFrame(df_all_c,
                               columns=['UDF', 'RK', 'PC', 'RD', 'type', 'is_np', 'mean', 'std', 'q1', 'median', 'q3'])

        """
          ####### UDF=~var, RK=3, PC=3, RD=9
        """
        df_udf = df_cass[df_cass.RK == 3]
        df_udf = df_udf[df_udf.PC == 3.0]
        df_udf = df_udf[df_udf.RD == 9]
        create_bars(df_udf, f'images/{db["db"]}', db["db"])
        create_bars(df_udf, f'images/{db["db"]}', db["db"])
    
        """
          ####### UDF=10_intersect, RK=3, PC=3, RD=~var
        """
        df_RD = df_cass[df_cass.UDF.str.contains("10_intersect")]
        df_RD = df_RD[df_RD.RK == 3]
    
        create_dd(df_RD, 'RK', 'PC', 'RD', f'images/{db["db"]}', db["db"])
    
        """
          ####### UDF=10_intersect, RK=~, PC=3, RD=9
        """
        df_RK = df_cass[df_cass.UDF.str.contains("10_intersect")]
        df_RK = df_RK[df_RK.RD == 9]
    
        create_bb(df_RK, 'RD', 'PC', 'RK', f'images/{db["db"]}', db["db"])
    
        """
          ####### UDF=10_intersect, RK=3, PC=~var, RD=9
        """
        df_RK = df_cass[df_cass.UDF.str.contains("10_intersect")]
        df_RK = df_RK[df_RK.RD == 9]

        create_bb(df_RK, 'RD', 'RK', 'PC', f'images/{db["db"]}', db["db"])
        """
        PRINT ALL
        """

        create_bars(df_cass, f'images/{db["db"]}', db["db"])
        create_d(df_cass, f'images/{db["db"]}', db["db"])
        create_b(df_cass, f'images/{db["db"]}', db["db"])

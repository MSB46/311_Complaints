from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 3,
}

with (DAG(
        dag_id='311_noise_forecast_pipeline',
        default_args=default_args,
        schedule='0 0 * * *',
        catchup=False,
        tags=['311_data'],
        max_active_runs=1)) as dag:
    import pandas as pd
    import requests

    @task()
    def initialize_database():
        import numpy as np
        import sqlite3
        URL = 'https://data.cityofnewyork.us/resource/erm2-nwe9.json?$select=unique_key,created_date,complaint_type,descriptor,borough,location_type,street_name,Latitude,Longitude&$where=complaint_type%20LIKE%20%22%25Noise%25%22&$order=created_date%20DESC'

        RECORD_LIMIT = 7_500_000
        SPLIT_LIMIT = 10
        force=False

        iters_ = np.arange(0, int(RECORD_LIMIT * (1 + 1 / SPLIT_LIMIT)), RECORD_LIMIT / SPLIT_LIMIT).astype(int)
        split_urls = [URL + f'&$limit={b - a}&$offset={a}' for (a, b) in zip(iters_[:-1], iters_[1:])]

        db_path = "noise_records.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        def fetch_url(url):
            request = requests.get(url)
            request.raise_for_status()
            return request.json()

        records_exists = cursor.execute('''SELECT count(*) FROM sqlite_master WHERE type='table' AND name='complaints';''').fetchall()[0][0] > 0

        if not records_exists or force:
            from concurrent.futures import ThreadPoolExecutor
            from concurrent.futures import as_completed
            print("Creating table for noise records ...")
            cursor.execute("DROP TABLE IF EXISTS complaints")
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS complaints (
                    unique_key TEXT,
                    created_date TEXT,
                    complaint_type TEXT,
                    descriptor TEXT,
                    borough TEXT,
                    street_name TEXT,
                    latitude TEXT,
                    longitude TEXT)
                '''
            )
            print("Inserting records into table ...")
            with ThreadPoolExecutor() as executor:
                future_to_url = {executor.submit(fetch_url, url): url for url in split_urls}
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    print(f"Inserting records from: {url}")
                    try:
                        cur_data = future.result()
                        cursor.executemany("INSERT INTO complaints VALUES(?,?,?,?,?,?,?,?)",
                                           ((row.get("unique_key"),
                                             row.get("created_date"),
                                             row.get("complaint_type", 'Noise'),
                                             row.get("descriptor", 'Other'),
                                             row.get("borough", 'UNKNOWN'),
                                             row.get('street_name', 'UNKNOWN'),
                                             row.get("Latitude", ''),
                                             row.get("Longitude", '')
                                             ) for row in cur_data))
                    except Exception as exc:
                        print(f'{url} generated an exception: {exc}')
                    else:
                        print(f"Finished records from {url}")
                        conn.commit()
            print("Done!")

        else:
            print("Records already created")

        conn.close()
        return

    @task()
    def extract_data():
        import sqlite3

        def fetch_url(url):
            import requests
            try:
                request = requests.get(url)
                request.raise_for_status()
                return request.json()
            except (ConnectionError, ConnectionRefusedError, TypeError) as e:
                print("Connection error. Can't connect at the moment.")
                return {}

        db_path = "noise_records.db"  # For SQLite
        conn = sqlite3.connect(db_path)
        with conn:
            cursor = conn.cursor()

            last_record = pd.read_sql_query("SELECT created_date FROM complaints ORDER BY created_date DESC LIMIT 1",conn)
            last_record['created_date'] = pd.to_datetime(last_record['created_date'])
            last_dt = last_record.iloc[0]['created_date']
            last_day, last_month, last_year = f'{last_dt.day:02d}', f'{last_dt.month:02d}', f'{last_dt.year}'
            last_hour, last_minute, last_second = f'{last_dt.hour:02d}', f'{last_dt.minute:02d}', f'{last_dt.second:02d}'

            url_new_records = f"https://data.cityofnewyork.us/resource/erm2-nwe9.json?$query=SELECT%0A%20%20%60unique_key%60%2C%0A%20%20%60created_date%60%2C%0A%20%20%60complaint_type%60%2C%0A%20%20%60descriptor%60%2C%0A%20%20%60street_name%60%2C%0A%20%20%60borough%60%2C%0A%20%20%60Latitude%60%2C%0A%20%20%60Longitude%60%0AWHERE%0A%20%20(%60created_date%60%20%3E%20%22{last_year}-{last_month}-{last_day}T{last_hour}%3A{last_minute}%3A{last_second}%22%20%3A%3A%20floating_timestamp)%0A%20%20AND%20caseless_contains(%60complaint_type%60%2C%20%22Noise%22)%0AORDER%20BY%20%60created_date%60%20ASC%20NULL%20LAST%20LIMIT%201000000"
            new_data = fetch_url(url_new_records)
            if new_data:
                print(f"Fetched {len(new_data)} new records")
                cursor.executemany("INSERT INTO complaints VALUES(?,?,?,?,?,?,?,?)",
                                   ((row.get("unique_key"),
                                     row.get("created_date"),
                                     row.get("complaint_type", 'Noise'),
                                     row.get("descriptor", 'Other'),
                                     row.get("borough", 'UNKNOWN'),
                                     row.get('street_name', 'UNKNOWN'),
                                     row.get("Latitude", ''),
                                     row.get("Longitude", '')) for row in new_data))
                conn.commit()

        records = pd.read_sql_query("SELECT * FROM complaints ORDER BY created_date", conn)
        to_path = r"/opt/airflow/dags/complaint_records.parquet"
        records.to_parquet(to_path, index=True)

        return to_path



    @task(multiple_outputs=True)
    def transform_load_data(orig_df_path):
        import re
        # Code to preprocess the ingested data
        df_ = pd.read_parquet(orig_df_path)

        df_['created_date'] = pd.to_datetime(df_['created_date'])
        df_.set_index('created_date', inplace=True)

        df_['latitude'] = pd.to_numeric(df_['latitude'], errors='coerce')
        df_['longitude'] = pd.to_numeric(df_['longitude'], errors='coerce')

        df_['complaint_type'] = df_['complaint_type'].apply(
            lambda x: 'General' if x == 'Noise' else re.sub(r"^Noise\s+?[\.\-]\s+?", '', x))

        df_['complaint_type'] = df_['complaint_type'].str.upper()

        # Removal of records with vague 'Noise survey' value
        df_ = df_[df_['complaint_type'] != 'NOISE SURVEY']

        # Generalization of various values
        df_.loc[df_['complaint_type'] == 'COLLECTION TRUCK NOISE', 'complaint_type'] = 'VEHICLE'
        df_.loc[df_['complaint_type'] == 'HELICOPTER', 'descriptor'] = 'HELICOPTER'
        df_.loc[df_['complaint_type'] == 'HELICOPTER', 'complaint_type'] = 'VEHICLE'

        # Remove "NOISE" prefix in descriptor
        df_['descriptor'] = df_['descriptor'].str.upper()
        df_['descriptor'] = df_['descriptor'].apply(
            lambda x: 'OTHER' if x == 'NOISE' else re.sub(r"^NOISE[\s\.\-\,:]+", '', x))

        # Unite air conditioning descriptors
        df_.loc[df_['descriptor'].str.contains(
            "AIR CONDITION/VENTILATION"), 'descriptor'] = 'AIR CONDITION/VENTILATION'

        # Unite 'Other' descriptors
        df_.loc[df_['descriptor'].str.contains("OTHER"), 'descriptor'] = 'OTHER'

        # Unite 'Loud Music' descriptors
        df_.loc[df_['descriptor'].str.contains("LOUD MUSIC"), 'descriptor'] = 'LOUD MUSIC/PARTY'

        # Unite 'Boat' descriptors
        df_.loc[df_['descriptor'].str.contains("BOAT"), 'descriptor'] = 'BOAT'

        # Remove parenthesis and its content in descriptor
        df_['descriptor'] = df_['descriptor'].apply(lambda x: re.sub(r"\(.+\)", "", x))

        df_['descriptor'] = df_['descriptor'].str.strip()

        df_ = df_[df_['borough'].str.lower().isin(['queens', 'bronx', 'brooklyn', 'manhattan', 'staten island'])]

        df_["day_of_week"] = df_.index.dayofweek  # Monday = 0, Sunday = 6
        df_["month"] = df_.index.month
        df_["quarter"] = df_.index.quarter
        df_["is_weekend"] = (df_["day_of_week"] >= 5).astype(int)
        df_['hour'] = df_.index.hour

        df_resample = df_.resample('h').count().rename(columns={'unique_key': 'num_complaints'})[['num_complaints']]

        # Load data
        export_path_rs = r"/opt/airflow/dags/complaints_hourly.parquet"
        export_path_df = r"/opt/airflow/dags/complaint_records.parquet"

        df_resample.to_parquet(export_path_rs, index=True)
        df_.to_parquet(export_path_df, index=True)
        return {'rs_path': export_path_rs,
                'df_path': export_path_df}


    # @task()
    # def load_data(df_resample, df):
    #     export_path_rs = r"/opt/airflow/dags/complaints_hourly.parquet"
    #     export_path_df = r"/opt/airflow/dags/complaint_records.parquet"
    #     df_resample.to_parquet(export_path_rs, index=True)
    #     df.to_parquet(export_path_df, index=True)
    #
    #     return {'rs_path': export_path_rs, 'df_path': export_path_df}


    @task()
    def visualize(record_path):
        import plotly.express as px
        import plotly.graph_objects as go
        from plotly import subplots
        records = pd.read_parquet(record_path)
        borough_color_map = {
            'BRONX': '#3471eb',
            'BROOKLYN': '#d40614',
            'MANHATTAN': '#54ff52',
            'QUEENS': '#a539f7',
            'STATEN ISLAND': '#ed6e13'
        }

        def viz_noise_by_borough(df):
            df_nct_bor = ((df.groupby(['complaint_type', 'borough'])['unique_key']
                           .count())
                          .to_frame()
                          .sort_values(by=['unique_key', 'borough'], ascending=[False, True])
                          .reset_index())
            df_nct_bor = df_nct_bor.rename(columns={'unique_key': 'num_complaints'})
            noise_type_color_map = {a: b for b, a in zip(px.colors.sequential.matter_r, df_nct_bor["complaint_type"].unique())}
            def save_type_pie():
                fig = go.Figure()
                fig.add_trace(
                    go.Pie(
                        labels=df_nct_bor["complaint_type"],
                        values=df_nct_bor["num_complaints"],
                        textinfo='label+percent',
                        marker=dict(colors=df_nct_bor["complaint_type"]),
                        marker_colors=df_nct_bor["complaint_type"].map(noise_type_color_map),
                    ),
                )

                fig.update_layout(
                    title=dict(text="Breakdown of Noise Complaints by Type",
                               x=0.5,
                               y=0.97,
                               font=dict(size=18)
                               ),
                    font=dict(
                        size=14,
                        color="white"
                    ),
                    template='plotly_dark',
                    paper_bgcolor='rgba(0, 0, 6, .8)',
                    plot_bgcolor='rgba(0, 0, 0, 0)',
                    margin=dict(l=20, r=20, t=50, b=0),
                    hoverlabel=dict(
                        font_size=14,
                    ),
                    uniformtext_minsize=12, uniformtext_mode='hide',
                    showlegend=False
                )
                fig.update_traces(textposition='inside')
                fig.write_html(r"/opt/airflow/shared/noise_type_pie.html")
            def save_type_donut():
                temp = df_nct_bor.groupby('borough', as_index=False)['num_complaints'].sum()
                temp['complaint_type'] = 'TOTAL'
                df_nct_bor_with_total = pd.concat((df_nct_bor, temp))

                fig = px.pie(df_nct_bor_with_total[df_nct_bor_with_total['complaint_type'] != 'General'],
                              values='num_complaints',
                              names='borough',
                              facet_col="complaint_type",
                              facet_col_wrap=4,
                              facet_row_spacing=0.34,
                              facet_col_spacing=0.,
                              hole=0.56,
                              color='borough',
                              color_discrete_map=borough_color_map,
                              custom_data='num_complaints',
                              )

                fig.update_layout(
                    title=dict(text="Breakdown of Noise Complaints by Place and Borough",
                               x=0.5,
                               y=0.97,
                               font=dict(size=16)
                               ),
                    legend=dict(
                        orientation="h"
                    ),
                    font=dict(
                        size=14,
                        color="white"
                    ),
                    template='plotly_dark',
                    paper_bgcolor='rgba(0, 0, 6, .8)',
                    plot_bgcolor='rgba(0, 0, 0, 0)',
                    margin=dict(l=10, r=10, t=70, b=10),
                    hoverlabel=dict(
                        font_size=12,
                    ),
                )

                fig.for_each_annotation(
                    lambda a: a.update(text=a.text.replace("complaint_type=", ""), showarrow=False))

                fig.update_traces(hovertemplate=None, textposition='inside')
                fig.write_html(r"/opt/airflow/shared/noise_type_donut.html")

            save_type_pie()
            save_type_donut()

        def viz_noise_by_time_day(df):
            def hour_to_ampm(hour):
                if hour == 0:
                    return "12:00 AM"
                elif hour < 12:
                    return f"{hour}:00 AM"
                elif hour == 12:
                    return "12:00 PM"
                else:
                    return f"{hour - 12}:00 PM"
            df_day_hour = df.groupby(['day_of_week', 'hour'])['unique_key'].count().rename(
                'num_complaints').reset_index()
            df_day_hour['day_of_week'] = df_day_hour['day_of_week'].map(
                {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'})
            df_day_hour['weekend'] = df_day_hour['day_of_week'].isin(['Saturday', 'Sunday']).astype(int)
            df_day_hour['hour_ampm'] = df_day_hour['hour'].map(hour_to_ampm)

            df_day = df_day_hour.groupby('day_of_week')['num_complaints'].agg(['sum', 'mean'])
            df_day['mean'] = df_day['mean'].round(3)
            df_day.sort_values(['mean', 'sum'], ascending=[False, False], inplace=True)
            df_day['weekend'] = df_day.index.isin(['Saturday', 'Sunday'])

            df_dayofweek = df.resample('d')['unique_key'].count().rename('count').reset_index()
            df_dayofweek['dayofweek'] = df_dayofweek['created_date'].dt.dayofweek
            df_dayofweek = df_dayofweek.groupby('dayofweek', as_index=False)['count'].mean().round(2)
            df_dayofweek['dayofweek'] = df_dayofweek['dayofweek'].map(
                {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'})

            df_dayofweek['weekend'] = df_dayofweek['dayofweek'].isin(['Saturday', 'Sunday'])
            df_dayofweek.rename(columns={'count': 'average_complaints'}, inplace=True)

            df_hour = df.resample('h')['unique_key'].count().reset_index().rename(
                columns={'unique_key': 'count'})
            df_hour['hour'] = df_hour['created_date'].dt.hour
            df_hour = df_hour.groupby('hour', as_index=False)['count'].mean().round(2)
            df_hour['hour_ampm'] = df_hour['hour'].map(hour_to_ampm)
            df_hour.rename(columns={'count': 'average_complaints', 'hour': 'Hour'}, inplace=True)

            def save_hour_bar_subplots():
                fig = subplots.make_subplots(rows=1, cols=7, shared_yaxes=True,
                                              subplot_titles=df_day_hour['day_of_week'].unique())
                for i, d in enumerate(df_day_hour['day_of_week'].unique(), start=1):
                    cur_df = df_day_hour[df_day_hour['day_of_week'] == d]
                    mean_complaints, total_complaints = df_day.loc[d, 'mean'], df_day.loc[d, 'sum']
                    fig.add_trace(go.Bar(x=cur_df['hour'],
                                          y=cur_df['num_complaints'],
                                          marker=dict(
                                              color='rgba(255, 30, 30, 0.75)' if i >= 6 else 'rgba(130, 130, 240, 0.75)'),
                                          customdata=cur_df['hour_ampm'],
                                          hovertemplate='Hour: %{customdata}<extra></extra>' + '<br>Total Complaints: <b>%{y}</b>',
                                          showlegend=False),
                                   row=1, col=i)

                    # fig.add_hline(y=mean_complaints, line_dash="dot",
                    #                annotation_text=f"Mean: {mean_complaints}",
                    #                annotation_position="center",
                    #                annotation_font_size=12,
                    #                annotation_font_color="rgba(255, 255, 199, 0.85)",
                    #                row=1, col=i
                    #                )


                fig.update_xaxes(title_text='Hour', title_standoff=15, row=1, col=1)

                fig.update_layout(title='Total Noise Complaint Distribution by Hour and Day',
                                   yaxis_title='Complaints',
                                   font=dict(size=12, color="white"),
                                   template='plotly_dark',
                                   paper_bgcolor='rgba(0, 0, 6, .9)',
                                   plot_bgcolor='rgba(0, 0, 0, 0)',
                                   margin=dict(l=10, r=10, t=55, b=20),
                                   hoverlabel=dict(
                                       font_size=14,
                                   ),
                                   annotations=[
                                       dict(
                                           x=a['x'],
                                           y=a['y'] - 0.05,  # Adjust vertical position
                                           text=a['text'],
                                           showarrow=False,
                                           xanchor='center',
                                           yanchor='bottom'
                                       ) for a in fig.layout['annotations']
                                   ]
                                   )

                fig.write_html(r"/opt/airflow/shared/hour_subplot.html")
            def save_hour_avg_bar():
                fig = px.bar(
                        df_hour,
                        x='hour_ampm',
                        y='average_complaints',
                        color='Hour',
                        color_continuous_scale=['darkblue', 'rgba(20, 0, 120, 1)', 'orange', 'yellow', 'orange', 'rgba(20, 0, 120, 1)', 'darkblue'],
                    )

                fig.update_layout(title="Average Daily Complaints by Hour",
                    xaxis_title=None,
                    yaxis_title='Complaints',
                    font=dict(size=12,color="white"),
                    template='plotly_dark',
                    paper_bgcolor='rgba(0, 0, 6, .9)',
                    plot_bgcolor='rgba(0, 0, 0, 0)',
                    margin=dict(l=10, r=10, t=55, b=20),
                    showlegend=False,
                    xaxis_tickangle=45,
                    hoverlabel=dict(bgcolor='black',font_size=14)
                   )

                fig.update_traces(hovertemplate='<b>%{x}</b><br>Average: %{y}<extra></extra>',)
                fig.write_html(r"/opt/airflow/shared/hour_avg_bar.html")
            def save_dayofweek_avg_bar():
                fig = px.bar(
                    df_dayofweek,
                    x='dayofweek',
                    y='average_complaints',
                    color='weekend',
                    color_discrete_map={True:'rgba(255, 30, 30, 0.75)', False:'rgba(130, 130, 240, 0.75)'},
                    # text='hour_ampm',
                )

                fig.update_layout(title="Average Complaints by Day of Week",
                    xaxis_title=None,
                    yaxis_title='Complaints',
                    font=dict(size=12,color="white"),
                    template='plotly_dark',
                    paper_bgcolor='rgba(0, 0, 6, .9)',
                    plot_bgcolor='rgba(0, 0, 0, 0)',
                    margin=dict(l=10, r=10, t=55, b=20),
                    xaxis={
                        'categoryorder': 'array',
                        'categoryarray': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']},
                    showlegend=False,
                    xaxis_tickangle=0,
                    hoverlabel=dict(font_size=14)
                   )

                fig.update_traces(hovertemplate='<b>%{x}</b><br>Average: %{y}<extra></extra>')
                fig.write_html(r"/opt/airflow/shared/dayofweek_avg_bar.html")

            save_hour_bar_subplots()
            save_hour_avg_bar()
            save_dayofweek_avg_bar()

        def viz_noise_by_description(df):
            df_desc = (df.groupby(['descriptor', 'complaint_type']).count()['unique_key']
                       .sort_values(ascending=False)
                       .reset_index()
                       .rename(columns={'unique_key': 'num_complaints'}))
            df_desc = df_desc[df_desc['num_complaints'] > 10]

            N_TH = 10
            df_gr_desc = df_desc.groupby('descriptor')['num_complaints']
            df_desc['desc_total'] = df_gr_desc.transform(lambda x: sum(x))
            n_highest = df_gr_desc.sum().sort_values(ascending=False).iloc[N_TH - 1]
            common_noise_types = df_desc[df_desc['desc_total'] >= n_highest]['descriptor'].unique()
            def save_desc_hbar():
                fig = px.bar(df_desc[df_desc['descriptor'].isin(common_noise_types)],
                              x="num_complaints",
                              y="descriptor",
                              orientation='h',
                              custom_data=['complaint_type'],
                              color="complaint_type",
                              color_discrete_sequence=px.colors.sequential.matter_r,
                              )
                #
                fig.update_layout(title='Top 10 Frequent Complaints by Descriptions and Place',
                                   yaxis_title=None,
                                   xaxis_title='Complaints',
                                   xaxis_title_standoff=25,
                                   font=dict(size=12, color="white"),
                                   template='plotly_dark',
                                   paper_bgcolor='rgba(0, 0, 6, .9)',
                                   plot_bgcolor='rgba(0, 0, 0, 0)',
                                   margin=dict(l=20, r=20, t=50, b=0),
                                   legend_title_text='Complaint Type',
                                   legend=dict(
                                       orientation="v",
                                       yanchor="top",
                                       y=0.55),
                                   hoverlabel=dict(bgcolor="black", font_size=14),
                                   )
                fig.update_yaxes(ticksuffix="  ")
                fig.update_yaxes(categoryorder='total ascending')
                fig.update_traces(
                    hovertemplate='Type: <i>%{customdata[0]}</i><extra></extra>' + '<br>Count: <i>%{x:d}</i>')

                fig.write_html(r"/opt/airflow/shared/description_hbar.html")
            save_desc_hbar()

        def viz_noise_by_street(df):
            import re
            street_suffix_dict = {
                'AVE': 'AVENUE',
                'AV': 'AVENUE',
                'BCH': 'BEACH',
                'BLDG': 'BUILDING',
                'BRDG': 'BRIDGE',
                'BRG': 'BRIDGE',
                'BL': 'BOULEVARD',
                'BLVD': 'BOULEVARD',
                'BLV': 'BOULEVARD',
                'BR': 'BRANCH',
                'CIR': 'CIRCLE',
                'CP': 'CAMP',
                'CRES': 'CRESCENT',
                'CT': 'COURT',
                'CV': 'COVE',
                'CRT': 'COURT',
                'CTR': 'CENTER',
                'DR': 'DRIVE',
                'DRV': 'DRIVE',
                'ET': '',
                'EXD': '',
                'EXT': 'EXIT',
                'FLD': 'FIELD',
                'GDN': 'GARDEN',
                'GRN': 'GREEN',
                'HS': 'HIGH SCHOOL',
                'H S': 'HIGH SCHOOL',
                'HL': 'HILL',
                'HWY': 'HIGHWAY',
                'LA': 'LANE',
                'LN': 'LANE',
                'MS': 'MIDDLE SCHOOL',
                'MS/HS': 'MIDDLE/HIGH SCHOOL',
                'PCT': 'PRECINCT',
                'PS': 'PUBLIC SCHOOL',
                'P S': 'PUBLIC SCHOOL',
                'PS/IS': 'PUBLIC SCHOOL',
                'SR': 'STATE ROAD',
                'EXPY': 'EXPRESSWAY',
                'PK': 'PARK',
                'PRK': 'PARK',
                'PKWY': 'PARKWAY',
                'PG': 'PLAYGROUND',
                'PL': 'PLACE',
                'PLGD': 'PLAYGROUND',
                'PLZ': 'PLAZA',
                'PLZA': 'PLAZA',
                'PKW': 'PARKWAY',
                'PW': 'PARKWAY',
                'RIV': 'RIVER',
                'RD': 'ROAD',
                'RR': 'RAILROAD',
                'SQ': 'SQUARE',
                'WB': 'WESTBOUND',
                'SB': 'SOUTHBOUND',
                'NB': 'NORTHBOUND',
                'EB': 'EASTBOUND',
                'TER': 'TERRACE',
                'TERR': 'TERRACE',
                'TUNL': 'TUNNEL',
                'TNPK': 'TURNPIKE',
                'TPKE': 'TURNPIKE',
                'TRL': 'TRAIL',
                'N': 'NORTH ',
                'S': 'SOUTH ',
                'E': 'EAST ',
                'W': 'WEST ',
                'NW': 'NORTHWEST ',
                'SW': 'SOUTHWEST ',
                'NE': 'NORTHEAST ',
                'SE': 'SOUTHEAST ',
            }
            df_street = df.copy()
            df_street['street_name'] = df_street['street_name'].apply(lambda x: re.sub(r"([^a-zA-Z0-9\s]+)", ' ', x))
            df_street.loc[df_street['street_name'].str.startswith("ST "), 'street_name'] = df_street.loc[
                df_street['street_name'].str.startswith("ST "), 'street_name'].apply(
                lambda x: re.sub(r"^(ST\s?)", "SAINT ", x).strip())
            df_street.loc[df_street['street_name'].str.endswith("ST"), 'street_name'] = df_street.loc[
                df_street['street_name'].str.endswith("ST"), 'street_name'].apply(
                lambda x: re.sub(r"[\s\d](ST)$", " STREET", x).strip())

            df_street['street_name'] = df_street['street_name'].apply(lambda x: re.sub(
                r"(\d+)(ST|ND|RD|TH)",
                r"\1",
                " ".join([street_suffix_dict.get(w, w) for i, w in enumerate(x.upper().split())])))

            df_street['street_name'] = df_street['street_name'].apply(lambda x: re.sub(r"(^\s)|(\s$)|(\s\s+)", "", x))
            df_street = df_street[df_street['street_name'] != 'UNKNOWN']
            df_street_bor = df_street.groupby(['borough', 'street_name'], as_index=False)['unique_key'].count().rename(
                columns={'unique_key': 'num_complaints'})
            def save_street_scatter():
                fig = subplots.make_subplots(rows=5, cols=1, vertical_spacing=0.085)
                for i, b in enumerate(['BRONX', 'MANHATTAN', 'QUEENS', 'BROOKLYN', 'STATEN ISLAND'], start=1):
                    color = borough_color_map.get(b)
                    cur_df = df_street_bor[df_street_bor['borough'] == b].sort_values('num_complaints',
                                                                                      ascending=False).iloc[:6]
                    fig.add_trace(go.Scatter(y=cur_df["num_complaints"], mode='markers',
                                              marker=dict(
                                                  color=color,
                                                  size=cur_df["num_complaints"],
                                                  sizemode='area',
                                                  sizeref=2. * max(df_street_bor["num_complaints"]) / (60 ** 2),
                                                  sizemin=2,
                                                  line_width=2,
                                              ),
                                              x=cur_df["street_name"], name=b), row=i, col=1)

                fig.update_layout(
                    title=dict(text="Noisiest Streets by Borough",
                               x=0.5,
                               y=0.97,
                               font=dict(size=16)
                               ),
                    font=dict(color="white"),
                    template='plotly_dark',
                    paper_bgcolor='rgba(0, 0, 6, .9)',
                    plot_bgcolor='rgba(0, 0, 0, 0)',
                    margin=dict(l=10, r=10, t=50, b=10),
                    hoverlabel=dict(
                        bgcolor="black",
                        font_size=14,
                    ),
                    hovermode='x unified',
                    showlegend=False,

                )
                fig.update_xaxes(tickfont_size=9, tickangle=0)

                for i in range(1, 6):
                    fig.update_yaxes(range=[-2000, 2000], row=i, col=1, type='log', showticklabels=False)

                fig.update_traces(hovertemplate='<br>Total Complaints: <b>%{y}</b>')
                fig.write_html(r"/opt/airflow/shared/street_scatter.html")
            save_street_scatter()
            df_sbg = (df_street_bor.groupby('borough', as_index=False)['num_complaints']
                      .agg(['median', 'mean', 'sum'])
                      .sort_values(['median', 'mean', 'sum'], ascending=False)
                      .rename(
                columns={'borough': 'Borough', 'median': 'Median Complaints', 'mean': 'Average Complaints',
                         'sum': 'Total Complaints'}))

            df_sbg['Median Complaints'] = df_sbg['Median Complaints'].astype(int)
            df_sbg['Average Complaints'] = df_sbg['Average Complaints'].round(2)

            df_sbg.to_csv(r"/opt/airflow/shared/groupby_borough_noise.csv")

        viz_noise_by_borough(records)
        viz_noise_by_time_day(records)
        viz_noise_by_description(records)
        viz_noise_by_street(records)


    @task(multiple_outputs=True)
    def split_data(df, train_size, test_size):
        df_ = pd.read_parquet(df)
        # .set_index('created_date')

        train, test = df_.iloc[-(train_size + test_size):-test_size], df_.iloc[-test_size:]
        train.index = pd.DatetimeIndex(train.index).to_period('h')
        test.index = pd.DatetimeIndex(test.index).to_period('h')

        train_path = r"/opt/airflow/dags/train.parquet"
        test_path = r"/opt/airflow/dags/test.parquet"
        train.to_parquet(train_path, index=True)
        test.to_parquet(test_path, index=True)

        return {'train_records': train_path, 'test_records': test_path}

    @task()
    def find_best_model(train_path):
        from statsmodels.tsa.statespace.sarimax import SARIMAX
        from itertools import product
        import numpy as np
        import joblib

        train = pd.read_parquet(train_path)
        # .set_index('created_date')

        order_combs = list(product([1,2], [0], [1, 2]))
        season_order_combs = list(product([0], [1], [2], [24]))
        trend_combs = ['c', 'n']
        train_parameters = set([(a, b, c) for b in season_order_combs for a in order_combs for c in trend_combs])

        best_model = None
        best_aic = np.inf

        for i, (pdq, pdq_S, t) in enumerate(train_parameters):
            print(f'{i} / {len(train_parameters)} completed ...')
            try:
                cur_model = SARIMAX(train, order=pdq, seasonal_order=pdq_S, trend=t, enforce_stationarity=True,
                                    enforce_invertibility=True).fit()
            except Exception as e:
                print(f"Parameters {pdq} {pdq_S} {t} encountered an error: {e}")
                continue

            cur_aic = cur_model.aic
            if cur_aic < best_aic:
                best_aic = cur_aic
                best_model = cur_model
                # best_params = [pdq, pdq_S, t]

        model_path = r"/opt/airflow/dags/model.pkl"
        joblib.dump(best_model, model_path)
        return model_path

    @task()
    def generate_forecast(mdl_path, train_size, test_size):
        # Code to generate forecasts using the trained model and save forecast results
        import joblib
        model_ = joblib.load(mdl_path)
        pred_ = model_.predict(start=train_size, end=train_size+test_size+24)

        df_fc = pred_.reset_index().rename(columns={'index': 'created_date'})
        df_fc['created_date'] = df_fc['created_date'].astype(str)
        df_fc['created_date'] = pd.to_datetime(df_fc['created_date'])

        f_path = r"/opt/airflow/dags/forecast.csv"
        df_fc.to_csv(f_path, index=True)
        return f_path

    @task()
    def visualize_forecast(f_path, train_path, test_path):
        import plotly.graph_objects as go

        # Code to store or visualize the forecasted data
        df_fc = pd.read_csv(f_path, index_col=0)
        df_fc['created_date'] = df_fc['created_date'].astype(str)
        df_fc['created_date'] = pd.to_datetime(df_fc['created_date'])

        df_train = pd.read_parquet(train_path)
        NUM_DAYS_LOOK_BACK = 7
        START_DATE_CHECK = len(df_train) - 24 * NUM_DAYS_LOOK_BACK
        df_train = df_train.iloc[START_DATE_CHECK:]
        df_train = df_train.reset_index()
        df_train['created_date'] = df_train['created_date'].astype(str)
        df_train['created_date'] = pd.to_datetime(df_train['created_date'])

        df_test = pd.read_parquet(test_path)
        df_test = df_test.reset_index()
        df_test['created_date'] = df_test['created_date'].astype(str)
        df_test['created_date'] = pd.to_datetime(df_test['created_date'])

        # Creating the forecast plot
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_fc['created_date'], y=df_fc['predicted_mean'], name='Forecast',
                                  marker_color='rgba(255, 77, 151, .9)', text=df_fc['created_date'].dt.day_name()))
        fig.add_trace(go.Scatter(x=df_train['created_date'], y=df_train['num_complaints'], name='Observed',
                                  marker_color='rgba(244, 244, 255, .9)', text=df_train['created_date'].dt.day_name()))
        fig.add_trace(go.Scatter(x=df_test['created_date'], y=df_test['num_complaints'], name='Observed',
                                  marker_color='rgba(155, 152, 255, .9)', text=df_test['created_date'].dt.day_name()))

        fig.update_layout(
            title='Noise Complaint Forecast',
            legend=dict(
                orientation="v",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            title_x=0.5,
            xaxis_title=None,
            xaxis_title_standoff=15,
            yaxis_title='Complaints',
            yaxis_title_standoff=20,
            font=dict(
                size=12,
                color="white"
            ),
            template='plotly_dark',
            paper_bgcolor='rgba(0, 0, 6, .8)',
            plot_bgcolor='rgba(0, 0, 0, 0)',
            margin=dict(l=10, r=10, t=30, b=10),
            hovermode='x unified'
        )

        fig_path = r"/opt/airflow/shared/311_noise_forecast_plot.html"
        fig.write_html(fig_path)
        return fig_path


    with TaskGroup("extract_transform_load", tooltip="Extract, Transform, Load") as etl:
        init_db = initialize_database()
        extracted_data = extract_data()
        transformed_data = transform_load_data(extracted_data)
        rs_path, df_path = transformed_data["rs_path"], transformed_data["df_path"]
        create_visuals = visualize(df_path)

        with TaskGroup("train_model_forecast", tooltip="Training + Forecasting") as tmf:
            train_size = int(168 * 10)
            test_size = int(16.8 * 10)

            split_records = split_data(rs_path, train_size, test_size)
            train_records, test_records = split_records['train_records'], split_records['test_records']

            trained_model = find_best_model(train_records)
            forecast_path = generate_forecast(trained_model, train_size, test_size)
            make_forecast_plot = visualize_forecast(forecast_path, train_records, test_records)

            split_records >> trained_model >> forecast_path >> make_forecast_plot

        init_db >> extracted_data >> transformed_data >> [create_visuals, tmf]

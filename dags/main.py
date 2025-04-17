import pandas as pd
from datetime import datetime
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
}

with (DAG(dag_id='311_noise_forecast_pipeline',
         default_args=default_args,
         schedule='0 0 * * *',
         catchup=False,
         tags=['311_data']) as dag):

    @task()
    def extract_data():
        # Code to ingest 311 service request data
        URL_311 = 'https://data.cityofnewyork.us/resource/erm2-nwe9.json?$select=unique_key,created_date,complaint_type,descriptor,borough,location_type&$where=complaint_type%20LIKE%20%22%25Noise%25%22&$order=created_date%20DESC&$limit=1000000'
        response = requests.get(URL_311)

        assert response.status_code == 200, f"Couldn't fetch the data for some reason: {response.status_code}"
        pd_table = pd.read_json(URL_311)
        return pd_table


    @task()
    def transform_data(df):
        # Code to preprocess the ingested data
        df_ = df.copy()

        df_['complaint_type'] = df_['complaint_type'] \
            .apply(lambda x: 'General' if x == 'Noise' else x.replace('Noise - ', ''))

        df_.rename(columns={'complaint_type': 'noise_complaint_type'}, inplace=True)
        df_['created_date'] = pd.to_datetime(df_['created_date'])
        df_.set_index('created_date', inplace=True)

        df_resample = df_.resample('h').count().rename(columns={'unique_key': 'num_complaints'})[['num_complaints']]

        return df_resample


    @task()
    def load_data(df_resample):
        export_path = r"/opt/airflow/dags/complaints_hourly.csv"
        df_resample.to_csv(export_path, index=True)
        return export_path


    @task()
    def train_model(train_data_path):
        # Code to train the forecasting model
        from statsmodels.tsa.statespace.sarimax import SARIMAX
        import joblib

        train_data = pd.read_csv(
            train_data_path,
            parse_dates=['created_date']
        ).set_index('created_date')

        # Define SARIMA parameters
        p, d, q = 2, 0, 0
        s = 24  # Assuming hourly seasonality

        # Fit the SARIMA model
        new_model = SARIMAX(train_data, order=(p, d, q), seasonal_order=(p, d, q, s))
        model_fit = new_model.fit()

        model_path = r"/opt/airflow/dags/model.pkl"
        joblib.dump(model_fit, model_path)
        return model_path

    # @task()
    # def various_eda():
    #     pass

    @task()
    def generate_forecast(mdl_path):
        # Code to generate forecasts using the trained model and save forecast results
        import joblib
        model_ = joblib.load(mdl_path)
        f = model_.get_forecast(steps=24)

        f_df = f.predicted_mean

        f_path = r"/opt/airflow/dags/forecast.csv"
        f_df.to_csv(f_path, index=True)
        return f_path

    @task()
    def store_forecast(f_path, df_path):
        import plotly.graph_objects as go

        # Code to store or visualize the forecasted data
        df_forecast = pd.read_csv(f_path, index_col=0, parse_dates=True)
        df_ = pd.read_csv(df_path,
                          parse_dates=['created_date']).set_index('created_date')

        NUM_DAYS_LOOK_BACK = 3
        START_DATE_CHECK = len(df_) - 24 * NUM_DAYS_LOOK_BACK

        df_ = df_.iloc[START_DATE_CHECK:]
        df_ = df_.reset_index()

        df_forecast = df_forecast.reset_index().rename(columns={'index': 'created_date'})

        # Creating the forecast plot
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_forecast['created_date'], y=df_forecast['predicted_mean'], name='Forecast',
                                 marker_color='rgba(255, 100, 110, .9)', text=df_forecast['created_date'].dt.day_name()))
        fig.add_trace(go.Scatter(x=df_['created_date'], y=df_['num_complaints'], name='Observed',
                                 marker_color='rgba(140, 152, 255, .9)', text=df_['created_date'].dt.day_name()))
        fig.update_layout(
            title='311 Noise Complaints in NYC',
            legend=dict(
                orientation="v",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            title_x=0.5,
            xaxis_title='Date + Hour',
            xaxis_title_standoff=15,
            yaxis_title='Complaint Count',
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
        extracted_data = extract_data()
        transformed_data = transform_data(extracted_data)
        loaded_data_path = load_data(transformed_data)

        with TaskGroup("train_model_forecast", tooltip="Training + Forecasting") as tmf:
            trained_model = train_model(loaded_data_path)
            forecast_path = generate_forecast(trained_model)
            make_forecast_plot = store_forecast(forecast_path, loaded_data_path)

            trained_model >> forecast_path >> make_forecast_plot

        extracted_data >> transformed_data >> loaded_data_path >> tmf

from datetime import timedelta, datetime
import pendulum

import pandas as pd
import numpy as np

import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import matplotlib.pyplot as plt
import seaborn as sns

import telegram
import io


# Подключение к БД
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 
    'user': 'student',
    'database': 'simulator'
}
db = 'simulator_20240320'

local_tz = pendulum.timezone("Europe/Moscow")

# Параметры DAG
default_args = {
    'owner': 'v.grabchuk',
    #'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 5, 13, tzinfo=local_tz),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'


# Временной горизонт для отчёта
days = 8

points = days * 24 * 60 / 15  # Пересчёт в отсчёты
n_prev_points = points + 100  # Поправка на размер окна для расчётов


# Настройка графиков
sns.set(
    font_scale=1.1,  # Размер шрифта
    style='whitegrid',  # Стиль задника, осей
    rc={'figure.figsize': (10, 6)}  # Размер графика
)


# Параметры для поиска аномалий
mean_window_size = 3
quantile_window_size = 100
alpha = 0.04

time_template = '%Y-%m-%d %H:%M:%S'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=True)
def gvs_alerts():
    def query(query):
        # Запрос в БД
        return ph.read_clickhouse(query, connection=connection)
    
    @task
    def get_feed_data(n_prev_points):
        # Данные по ленте
        context = get_current_context()
        now = context['ts']
        now = datetime.fromisoformat(now)
        now = now.strftime(time_template)
        
        print(now)
        
        now = f"'{now}'"
        return query(f"""
            SELECT *
            FROM
            (
                SELECT
                      toStartOfFifteenMinutes(time) AS time_grain
                    , uniqExact(user_id) AS uniq_users
                    , countIf(action, action='view') AS views
                    , countIf(action, action='like') AS likes
                    , likes / views AS ctr
                FROM {db}.feed_actions
                WHERE time <= {now}
                GROUP BY time_grain
                ORDER BY time_grain DESC
                LIMIT {n_prev_points} + 1 + 1
            )
            ORDER BY time_grain ASC
        """).iloc[:-1].reset_index(drop=True)
    
    @task
    def get_messenger_data(n_prev_points):
        # Данные по мессенджеру
        context = get_current_context()
        now = context['ts']
        now = datetime.fromisoformat(now)
        now = now.strftime(time_template)
        now = f"'{now}'"
        return query(f"""
            SELECT *
            FROM
            (
                SELECT
                      toStartOfFifteenMinutes(time) AS time_grain
                    , uniqExact(user_id) AS uniq_users
                    , COUNT(receiver_id) AS messages
                FROM {db}.message_actions
                WHERE time <= {now}
                GROUP BY time_grain
                ORDER BY time_grain DESC
                LIMIT {n_prev_points} + 1 + 1
            )
            ORDER BY time_grain ASC
        """).iloc[:-1].reset_index(drop=True)
    
    def get_anomaly_info(
        df, 
        metric_col,
        mean_window_size,
        quantile_window_size,
        use_columns,
        alpha,
        delta=0
    ):
        '''
        Возвращает информацию об аномальности точки
        Метод: ДИ

        Parameters:
        -----------
        alpha: float
            Левый квантиль для построения ДИ
        delta: float
            Коэффициент для искусственного расширения ДИ

        Returns:
        --------
        df: pandas.DataFrame
            Структура c информацией по аномальности значений
            anomaly:
                1  - аномалия превышает верхний порог
                0  - аномалии нет
                -1 - аномалия меньше нижнего порога 
        '''
        mean = df[metric_col].rolling(mean_window_size).mean()

        high_freq = df[metric_col] - mean

        # CI
        ci_left = high_freq.rolling(quantile_window_size-1, closed='left').quantile(alpha)
        ci_right = high_freq.rolling(quantile_window_size-1, closed='left').quantile(1-alpha)
        # CI modification
        ci_left *= (1 + delta)
        ci_right *= (1 + delta)

        th_min = ci_left + mean
        th_max = ci_right + mean

        df = df[use_columns]

        return df.assign(
            mean=mean,
            high_freq=high_freq,
            ci_left=ci_left,
            ci_right=ci_right,
            th_min=th_min,
            th_max=th_max,
            anomaly=(df[metric_col] < th_min) * (-1) + (df[metric_col] > th_max) * 1
        )
    
    def get_lineplot(data, x, y, ths=None, title=None, xrotation=20, **kwargs):
        '''
        Возвращает модифицированный lineplot

        Parameters:
        -----------
            ths: [th_min, th_max]
                Пороги для отображения на графике
        '''

        ax = sns.lineplot(data=data, x=x, y=y, **kwargs)
        if ths is not None:
            th_min = ths[0]
            th_max = ths[1]
            sns.lineplot(data=data, x=x, y=th_min, color='r', linewidth=.5)
            sns.lineplot(data=data, x=x, y=th_max, color='r', linewidth=.5)

    #     ax.grid()
        if title is not None:
            ax.set_title(title)
        ax.tick_params(axis='x', labelrotation=xrotation)

        return ax
    
    def is_anomaly(df, anomaly_flag_col='anomaly'):
        return bool(df.iloc[-1][anomaly_flag_col])
    
    def report_message(bot, chat_id, text):
        # Отправка сообщения
        bot.sendMessage(chat_id=chat_id, text=text)
        
    def report_image(bot, chat_id, plot):
        # Отправка изображения
        buffer = io.BytesIO()
        plot.figure.savefig(buffer, format='png', bbox_inches='tight')
        buffer.seek(0)
        bot.sendPhoto(chat_id=chat_id, photo=buffer)
        plt.close()
        
    @task
    def check_metric(df, metric_col, time_col, delta=2, round_digits=2, plot_title=None): 
        context = get_current_context()
        now = context['ts']
        now = datetime.fromisoformat(now)
        time = now - timedelta(minutes=15)
        time = time.strftime(time_template)

        df = get_anomaly_info(
            df=df, 
            metric_col=metric_col, 
            mean_window_size=mean_window_size,
            quantile_window_size=quantile_window_size,
            alpha=alpha,
            use_columns=[time_col, metric_col],
            delta=delta
        )

        df = df.iloc[quantile_window_size:]

        if is_anomaly(df):
            text = f'''ANOMALY ALERT! 
{time}
----------------------------------------------------
Метрика: {plot_title}

Текущее значение: {round(df[metric_col].iloc[-1], round_digits)}
rolling_mean: {round(df['mean'].iloc[-1], round_digits)}
Отклонение: {round(df['high_freq'].iloc[-1], round_digits)}
----------------------------------------------------
Дашборд: https://superset.lab.karpov.courses/superset/dashboard/5423/

@boss
            '''

            plot = get_lineplot(
                data=df, 
                x=time_col, 
                y=df['high_freq'], 
                ths=[df['ci_left'], df['ci_right']], 
                title=plot_title
            );

            report_message(bot, chat_id, text)
            report_image(bot, chat_id, plot)

        return df

    
    # Данные
    feed_data_df = get_feed_data(n_prev_points)
    messenger_data_df = get_messenger_data(n_prev_points)
    # Настройка бота
    my_token =
    bot = telegram.Bot(token=my_token)
    chat_id = 
    # Поиск аномалий
    time_col = 'time_grain'
    # feed
    deltas = (
        2.5,  # uniq_users
        2.2,  # views
        1.8,  # likes
        1.6   # ctr
        
    )
    for metric_col, delta in zip(['uniq_users', 'views', 'likes', 'ctr'], deltas):
        _ = check_metric(
            df=feed_data_df, 
            metric_col=metric_col,
            time_col=time_col,
            delta=delta,
            plot_title=f'feed {metric_col}'
        )
    # messenger
    deltas = (
        2.0,  # uniq_users
        1.6   # messages
    )
    for metric_col, delta in zip(['uniq_users', 'messages'], deltas):
        _ = check_metric(
            df=messenger_data_df, 
            metric_col=metric_col,
            time_col=time_col,
            delta=delta,
            plot_title=f'messenger {metric_col}'
        )


gvs_alerts = gvs_alerts()


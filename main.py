import psycopg
import pandas as pd
import mlflow
import os

connection = {"sslmode": "require", "target_session_attrs": "read-write"}
postgres_credentials = {
    "host": "rc1b-uh7kdmcx67eomesf.mdb.yandexcloud.net", 
    "port": "6432",
    "dbname": "playground_mle_20250822_5fbea5500a",
    "user": "mle_20250822_5fbea5500a_freetrack",
    "password": "9ad242eec151435baaffc8c8573a8d7b",
}
assert all([var_value != "" for var_value in list(postgres_credentials.values())])

connection.update(postgres_credentials)

# определим название таблицы, в которой хранятся наши данные.
TABLE_NAME = "users_churn"

# эта конструкция создаёт контекстное управление для соединения с базой данных 
# оператор with гарантирует, что соединение будет корректно закрыто после выполнения всех операций 
# закрыто оно будет даже в случае ошибки, чтобы не допустить "утечку памяти"
with psycopg.connect(**connection) as conn:

# создаёт объект курсора для выполнения запросов к базе данных
# с помощью метода execute() выполняется SQL-запрос для выборки данных из таблицы TABLE_NAME
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {TABLE_NAME}")
                
                # извлекаем все строки, полученные в результате выполнения запроса
        data = cur.fetchall()

                # получает список имён столбцов из объекта курсора
        columns = [col[0] for col in cur.description]

# создаёт объект DataFrame из полученных данных и имён столбцов. 
# это позволяет удобно работать с данными в Python, используя библиотеку Pandas.
df = pd.DataFrame(data, columns=columns)

with open("columns.txt", "w", encoding="utf-8") as fio:
    fio.write(",".join(df.columns))
    counts_columns = [
    "type", "paperless_billing", "internet_service", "online_security", "online_backup",
    "device_protection", "tech_support", "streaming_tv", "streaming_movies", "gender",
    "senior_citizen", "partner", "dependents", "multiple_lines", "target"
]

stats = {}

# Частоты категориальных признаков (без NaN/None!)
for col in counts_columns:
    vc = df[col].value_counts()  # dropna=True по умолчанию
    for val, cnt in vc.items():
        stats[f"{col}_{val}"] = int(cnt)

# Размер датасета
stats["data_length"] = int(len(df))

# Числовые метрики
for num_col in ["monthly_charges", "total_charges"]:
    s = pd.to_numeric(df[num_col], errors="coerce")
    stats[f"{num_col}_min"] = float(s.min())
    stats[f"{num_col}_max"] = float(s.max())
    stats[f"{num_col}_mean"] = float(s.mean())
    stats[f"{num_col}_median"] = float(s.median())

# Уникальные клиенты и число пропусков в end_date
stats["unique_customers_number"] = int(df["customer_id"].nunique())
stats["end_date_nan"] = int(df["end_date"].isna().sum())
df.to_csv("users_churn.csv", index=False)

# задаём название эксперимента и имя запуска для логирования в MLflow
EXPERIMENT_NAME = "churn_fio"
RUN_NAME = "data_check"

# директория для артефактов
ARTIFACT_DIR = "dataframe"

# создаём папку для артефактов, если её нет
os.makedirs(ARTIFACT_DIR, exist_ok=True)

# создаём новый эксперимент в MLflow с указанным названием 
existing = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
experiment_id = (
    existing.experiment_id
    if existing is not None
    else mlflow.create_experiment(EXPERIMENT_NAME, artifact_location=ARTIFACT_DIR)
)

# запускаем run внутри нужного эксперимента
with mlflow.start_run(experiment_id=experiment_id, run_name=RUN_NAME) as run:
    run_id = run.info.run_id

    # логируем метрики эксперимента
    mlflow.log_metrics(stats)

    # логируем файлы как артефакты в директорию dataframe
    mlflow.log_artifact("columns.txt", artifact_path="dataframe")
    mlflow.log_artifact("users_churn.csv", artifact_path="dataframe")

# получаем данные о запуске
run = mlflow.get_run(run_id)

# проверяем успешное завершение
assert run.info.status == "FINISHED"

# удаляем файлы
os.remove("columns.txt")
os.remove("users_churn.csv")
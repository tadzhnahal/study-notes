from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

BUCKET = "nyctaxi"
MINIO_ENDPOINT = "http://172.17.0.1:9000"
MINIO_ACCESS_KEY = "tadzhnahal"
MINIO_SECRET_KEY = "tadzhnahal123"

def build_spark():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Unified Schema")
        .master("spark://localhost:7077")
        .config("spark.driver.host", "172.17.0.1")  # Указал адрес хоста, который видят контейнеры
        .config("spark.driver.bindAddress", "0.0.0.0")  # Разрешил драйверу слушать все интерфейсы
        .config("spark.driver.port", "4050")
        .config("spark.blockManager.port", "4051")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)  # Направил S3A-запросы в MinIO
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")  # Включил path-style для MinIO
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # Отключил SSL для http-подключения
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    return spark

def add_missing_columns(df, columns_with_types):
    for col_name, col_type in columns_with_types.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(col_type))  # Добавил отсутствующие поля как NULL
    return df

def cast_common_types(df):
    timestamp_cols = [
        "pickup_datetime",
        "dropoff_datetime",
    ]

    double_cols = [
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
    ]

    int_cols = [
        "passenger_count",
        "payment_type",
        "year",
        "month",
    ]

    for c in timestamp_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.coalesce(
                    F.try_to_timestamp(F.col(c), F.lit("yyyy-MM-dd HH:mm:ss")),
                    F.try_to_timestamp(F.col(c), F.lit("MM/dd/yyyy hh:mm:ss a")),
                    F.try_to_timestamp(F.col(c), F.lit("M/d/yyyy h:mm:ss a")),
                )
            )  # Добавил разбор нескольких форматов даты

    for c in double_cols:
        if c in df.columns:
            df = df.withColumn(
                c,
                F.regexp_replace(F.col(c), ",", "").cast(DoubleType())
            )  # Убрал запятые в больших числах перед приведением к double

    for c in int_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(IntegerType()))

    return df

def prepare_yellow(df):
    rename_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "VendorID": "vendor_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    df = df.withColumn("taxi_type", F.lit("yellow"))
    return df

def prepare_green(df):
    rename_map = {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "RatecodeID": "rate_code_id",
        "VendorID": "vendor_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    df = df.withColumn("taxi_type", F.lit("green"))
    return df

def prepare_fhv(df):
    rename_map = {
        "Pickup_DateTime": "pickup_datetime",
        "DropOff_datetime": "dropoff_datetime",
        "dispatching_base_num": "vendor_id",
        "PUlocationID": "pickup_location_id",
        "DOlocationID": "dropoff_location_id",
        "SR_Flag": "sr_flag",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    df = df.withColumn("taxi_type", F.lit("fhv"))
    return df

def add_year_month(df):
    df = df.withColumn("year", F.year("pickup_datetime"))
    df = df.withColumn("month", F.month("pickup_datetime"))
    return df

def main():
    spark = build_spark()

    yellow = spark.read.option("header", True).csv(f"s3a://{BUCKET}/yellow/*.csv")
    green = spark.read.option("header", True).csv(f"s3a://{BUCKET}/green/*.csv")
    fhv = spark.read.option("header", True).csv(f"s3a://{BUCKET}/fhv/*.csv")

    yellow = prepare_yellow(yellow)
    green = prepare_green(green)
    fhv = prepare_fhv(fhv)

    target_columns = {
        "vendor_id": "string",
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "passenger_count": "int",
        "trip_distance": "double",
        "rate_code_id": "string",
        "pickup_location_id": "string",
        "dropoff_location_id": "string",
        "payment_type": "int",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "sr_flag": "string",
        "taxi_type": "string",
        "year": "int",
        "month": "int",
    }

    yellow = add_missing_columns(yellow, target_columns)
    green = add_missing_columns(green, target_columns)
    fhv = add_missing_columns(fhv, target_columns)

    yellow = cast_common_types(yellow)
    green = cast_common_types(green)
    fhv = cast_common_types(fhv)

    yellow = add_year_month(yellow)
    green = add_year_month(green)
    fhv = add_year_month(fhv)

    final_columns = list(target_columns.keys())

    yellow = yellow.select(final_columns)
    green = green.select(final_columns)
    fhv = fhv.select(final_columns)

    df_all = yellow.unionByName(green).unionByName(fhv)

    valid_condition = (
        (F.col("trip_distance").isNull() | (F.col("trip_distance") > 0)) &
        (F.col("fare_amount").isNull() | (F.col("fare_amount") > 0)) &
        (
            F.col("pickup_datetime").isNull() |
            F.col("dropoff_datetime").isNull() |
            (F.col("pickup_datetime") < F.col("dropoff_datetime"))
        )
    )  # Добавил правила валидации для дистанции, стоимости и времени поездки

    df_valid = df_all.filter(valid_condition)
    df_invalid = df_all.filter(~valid_condition)

    df_valid.printSchema()
    df_valid.show(10, truncate=False)

    print("All rows:", df_all.count())
    print("Valid rows:", df_valid.count())
    print("Invalid rows:", df_invalid.count())

    df_clean = df_valid # Выделил очищенный датафрейм после валидации
    df_analytics = df_clean.filter(F.col("year").isin(2017, 2018, 2019)) # Ограничил аналитику корректными годами

    trips_by_year = (
        df_analytics
        .groupBy("year")
        .count()
        .orderBy("year")
    )

    print("\nTrips by year:")
    trips_by_year.show()

    trips_by_year_taxi = (
        df_analytics
        .groupBy("year", "taxi_type")
        .count()
        .orderBy("year", "taxi_type")
    )

    print("\nTrips by year and taxi type:")
    trips_by_year_taxi.show()

    year_window = Window.orderBy("year")  # Добавил окно для сравнения года с предыдущим

    trips_by_year_growth = (
        trips_by_year
        .withColumn("prev_count", F.lag("count").over(year_window))
        .withColumn(
            "growth_pct",
            F.round(
                ((F.col("count") - F.col("prev_count")) / F.col("prev_count")) * 100,
                2
            )
        )
    )

    print("\nYear-over-year growth:")
    trips_by_year_growth.show()

    summary_stats = df_analytics.select(
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.expr("percentile_approx(fare_amount, 0.5)").alias("median_fare_amount"),  # Добавил медиану через percentile_approx
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.expr("percentile_approx(trip_distance, 0.5)").alias("median_trip_distance"),  # Добавил медиану через percentile_approx
        F.avg("total_amount").alias("avg_total_amount"),
        F.expr("percentile_approx(total_amount, 0.5)").alias("median_total_amount"),  # Добавил медиану через percentile_approx
    )

    print("\nAverage and median values:")
    summary_stats.show(truncate=False)

    period_compare = (
        df_analytics
        .filter(F.col("year").isin(2017, 2018))
        .groupBy("year")
        .agg(
            F.count("*").alias("trips"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
        )
        .orderBy("year")
    )

    print("\nComparison of 2017 and 2018:")
    period_compare.show(truncate=False)

    tips_by_taxi = (
        df_analytics
        .groupBy("taxi_type")
        .agg(
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg(
                F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount"))
            ).alias("avg_tip_share")  # Добавил долю чаевых только для строк с положительной стоимостью
        )
        .orderBy("taxi_type")
    )

    print("\nDistance, tips and tip share by taxi type:")
    tips_by_taxi.show(truncate=False)

    passenger_distance = (
        df_analytics
        .filter(F.col("passenger_count").isNotNull())
        .groupBy("passenger_count")
        .agg(
            F.count("*").alias("trips"),
            F.avg("trip_distance").alias("avg_trip_distance")
        )
        .orderBy("passenger_count")
    )

    print("\nPassenger count and average distance:")
    passenger_distance.show()

    # df_invalid.show(10, truncate=False)

    # Сохраняем очищенный датафрейм целиком без партиционирования
    df_clean.write \
        .mode("overwrite") \
        .parquet(f"s3a://{BUCKET}/prepared/nyctaxi_parquet_full")

    # Сохраняем очищенный датафрейм с партиционированием по году и типу такси
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("year", "taxi_type") \
        .parquet(f"s3a://{BUCKET}/prepared/nyctaxi_parquet_partitioned")

    print("\nParquet write completed.")

    spark.stop()

if __name__ == "__main__":
    main()
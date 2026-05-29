from pyspark.sql.functions import *
import time

def generate_orders(batch_size=50000):

    df = (
        spark.range(batch_size)

        .withColumn("order_id", expr("uuid()"))

        .withColumn(
            "customer_id",
            (rand() * 100000).cast("int")
        )

        .withColumn(
            "product_id",
            (rand() * 5000).cast("int")
        )

        .withColumn(
            "quantity",
            (rand() * 5 + 1).cast("int")
        )

        .withColumn(
            "price",
            round(rand() * 500 + 10, 2)
        )

        .withColumn(
            "payment_type",
            when(rand() < 0.33, "Credit Card")
            .when(rand() < 0.66, "PayPal")
            .otherwise("Apple Pay")
        )

        .withColumn(
            "shipping_state",
            when(rand() < 0.2, "CA")
            .when(rand() < 0.4, "TX")
            .when(rand() < 0.6, "FL")
            .otherwise("NY")
        )

        .withColumn(
            "order_timestamp",
            current_timestamp()
        )

        .drop("id")
    )

    timestamp = int(time.time())

    output_path = (
        f"/FileStore/raw/orders/orders_{timestamp}"
    )

    (
        df.write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    print(f"Wrote batch to {output_path}")
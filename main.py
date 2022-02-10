from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def load_csv_file(filepath):
    return spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(filepath)


def convert_to_usd():
    join = company_df.join(currency_convert_df, company_df["currency"] == currency_convert_df["ISO_4217"], "inner")

    return join.withColumn("Dollar_Price", join.Price / join.Dollar_To_Curr_Ratio) \
        .select("Model_name", "Country", "Dollar_Price")


def calcul_average_price():
    model_us_df = company_df_usd \
        .where(col("Country") == "United States") \
        .withColumnRenamed("Dollar_Price", "United_States_price").select("Model_name", "United_States_price")

    calcul = company_df_usd \
        .join(model_us_df, company_df_usd["Model_name"] == model_us_df["Model_name"], "inner") \
        .groupBy("Country") \
        .agg(avg("Dollar_Price"), avg("United_States_price")) \
        .withColumnRenamed("avg(Dollar_Price)", "avg_price") \
        .withColumnRenamed("avg(United_States_price)", "avg_usa_price")

    return calcul.withColumn("Ecart", ((calcul.avg_price - calcul.avg_usa_price) / calcul.avg_usa_price) * 100) \
        .orderBy(desc("Ecart"))


def create_csv(df, name):
    df.write.mode("append").format("csv").option("header", "true").save("assets/" + name)


def total_cost_by_country():
    return company_df_usd.select("Country", "Dollar_Price").groupBy("Country").sum("Dollar_Price").coalesce(1)


def get_available_products():
    return company_df.select("Model_name").coalesce(1).distinct()


def get_cheaper_country(model):
    return company_df_usd.select("Country", "Dollar_Price").where(col("Model_name") == model).orderBy(
        asc("Dollar_Price")).coalesce(1)


if __name__ == '__main__':
    print("init SparkSession...\n")

    spark = SparkSession.builder \
        .master("local") \
        .getOrCreate()

    print("Loading => " + sys.argv[1] + "\n")
    company_df = load_csv_file(sys.argv[1])
    print("Company CSV loaded.\n")

    print("Loading => " + sys.argv[2] + "\n")
    currency_convert_df = load_csv_file(sys.argv[2])
    print("Currency converter CSV loaded.\n")

    print("Convert price to USD...\n")
    company_df_usd = convert_to_usd()
    company_df_usd.show()

    print("Create average price CSV...\n")
    create_csv(calcul_average_price(), "moyennePrix.csv")
    print("Write under 'assets/moyennePrix.csv'\n")

    print("Calcul cost of all products by country...\n")
    create_csv(total_cost_by_country(), "coutTotal.csv")
    print("Write under 'assets/coutTotal.csv'\n")

    print("Calcul cost of all products by country...\n")
    create_csv(get_available_products(), "listeProduit.csv")
    print("Write under 'assets/listeProduit.csv'\n")

    print("Find country with cheaper Airpods Pro price \n")
    create_csv(get_cheaper_country("AirPods Pro"), "airpodsPro.csv")
    print("Write under 'assets/airpodsPro.csv'\n")

    print("==== END SCRIPT ====")

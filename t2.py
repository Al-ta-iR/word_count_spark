from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split


# Создаем SparkSession
spark = SparkSession.builder.appName("WordCountDataFrame").getOrCreate()

# Загружаем текстовый файл
text_file = spark.read.text("text.txt")

# Разбиваем строки на слова
words = text_file.select(
    explode(split(text_file.value, " ")).alias("word")
)

# Подсчитываем количество слов
word_counts = words.groupBy("word").count()

# Выводим результат
word_counts.show()

# Останавливаем SparkSession
spark.stop()

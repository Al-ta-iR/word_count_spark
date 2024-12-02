from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode, count, avg, desc


# Создаем SparkSession
spark = SparkSession.builder.appName("WordStats").getOrCreate()

# Загружаем текстовый файл
text_file = spark.read.text("text.txt")

# Очистка текста и приведение к нижнему регистру
cleaned_text = text_file.select(
    regexp_replace(lower(col("value")), r"[^\w\s]", "").alias("line")
)

# Разбиваем строки на слова, убирая лишние пробелы
words = cleaned_text.select(
    explode(split(col("line"), r"\s+")).alias("word")
).filter(col("word") != "")  # Убираем пустые строки

# Подсчитываем количество слов
word_counts = words.groupBy("word").count()

# Сортируем по убыванию частоты для топ-10
top_10_words = word_counts.orderBy(col("count").desc()).limit(10)

# Дополнительная статистика
total_words = word_counts.agg(count("*").alias("unique_words")).collect()[0]["unique_words"]
most_common = word_counts.orderBy(col("count").desc()).first()
least_common = word_counts.orderBy(col("count").asc()).first()
average_count = word_counts.agg(avg(col("count")).alias("avg_count")).collect()[0]["avg_count"]
total_word_count = word_counts.agg(count(col("count")).alias("total_words")).collect()[0]["total_words"]

# Вывод результатов
print("Топ-10 самых частых слов:")
top_10_words.show()

print(f"Самое частое слово: {most_common['word']} с количеством {most_common['count']}")
print(f"Самое редкое слово: {least_common['word']} с количеством {least_common['count']}")
print(f"Средняя встречаемость слов: {average_count:.2f}")
print(f"Общее число уникальных слов: {total_words}")
print(f"Общее число всех слов: {total_word_count}")

# Останавливаем SparkSession
spark.stop()

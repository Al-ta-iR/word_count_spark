from pyspark import SparkContext


# Создаем контекст Spark
sc = SparkContext("local", "WordCount")

# Загружаем текстовый файл
text_file = sc.textFile("text.txt")

# Реализация MapReduce
word_counts = (
    text_file
    .flatMap(lambda line: line.split(" "))  # Разбиваем строки на слова
    .map(lambda word: (word, 1))  # Каждое слово преобразуем в пару (word, 1)
    .reduceByKey(lambda a, b: a + b)  # Суммируем количество повторений слов
)

# Сохраняем результат в файл или выводим на экран
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Останавливаем контекст Spark
sc.stop()

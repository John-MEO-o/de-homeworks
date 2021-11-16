#!/usr/bin/env python
# coding: utf-8

# импорт
import sys, os
from pyspark.sql import SparkSession
from pyspark.sql import Column
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dateutil.parser import parse

# подгружаем файлы
link_one = 'sets/crime.csv'
link_two = 'sets/offense_codes.csv'

# инициализация сессии
spark = SparkSession.builder\
    .master("local[*]")\
    .appName("crimes-report")\
    .getOrCreate()



# сверка первой таблицы, вывод её схемы
df = spark.read.option("header", True).csv(link_one)
df_amount = df.count()
df_unique = df.distinct().count()
if(df_amount > df_unique):
    removedEntriesCount = str(df_amount - df_unique)
    dfa = df.dropDuplicates()
    dfa.cache()
    print('Удаляем дубли...')
    print(f"Очищено {removedEntriesCount} записи(ей)." )
    print('В 1 кадре находится '+str(df_unique)+' записи(ей)\nВ следующей структуре:\n')
else:
    df.cache()
    print('Кадр данных №1 не имеет дублей.')
    print('В его составе находится '+str(df_amount)+' записи(ей)\nВ следующей структуре:\n')
df.printSchema()

# сверка второй таблицы, вывод её схемы
df2 = spark.read.option("header", True).csv(link_two)
df2_amount = df2.count()
df2_unique = df2.distinct().count()
if(df2_amount > df2_unique):
    removedEntriesCount2 = str(df2_amount - df2_unique)
    df2a = df2.dropDuplicates()
    df2a.cache()
    print('Удаляем дубли...')
    print(f"Очищено {removedEntriesCount2} записи(ей)." )
    print('В его составе находится '+str(df2_unique)+' записи(ей)\nВ следующей структуре:\n')
else:
    df2.cache()
    print('Кадр данных №1 не имеет дублей.')
    print('В его составе находится '+str(df2_amount)+' записи(ей)\nВ следующей структуре:\n')
df2.printSchema()



# определяем базовые константы
districts = df.na.drop(how="any")\
    .select(col('district'))\
    .distinct()\
    .orderBy('district')\
    .rdd.flatMap(lambda x: x)\
    .collect()

def median(numbers):
    asclist = sorted(numbers)
    i = int(len(asclist) / 2)
    if(len(asclist) % 2 != 0):
        res = asclist[i]
    else:
        res = (asclist[i] + asclist[i - 1]) / 2.0
    return res



'''
создаём 2 витрины:

1. общая статистика, (ДЗ п.3.1-3.3)
колонки: "район", 
         "общее число правонарушений",
         "годы",
         "медиана правонарушений, по месяцам",
         "наиболее частые правонарушения"

2. правонарушения по координатам (ДЗ п.3.4)
колонки: "район",
         "тип правонарушения", 
         "средняя широта",
         "средняя долгота"
'''

# создаём кадр данных, для первой витрины (ДЗ п.3.1-3.3)
data = spark.sparkContext.emptyRDD()
cols = StructType([StructField('district',
                                  StringType(), True),
                   StructField('crimes_total',
                                StringType(), True),
                   StructField('years',
                                StringType(), True),
                   StructField('median-per-months',
                                StringType(), True),
                   StructField('top-frequent-crimes',
                                StringType(), True)])
report_df = spark.createDataFrame(data=data, schema=cols)
report_df.cache()



# удаляем хвосты с дефисами, в значениях 'name', таблицы правонарушений
df2b = df2a.withColumn('name', split(df2a['name'], '\-')[0])

# создаём кадр данных, для второй витрины (ДЗ п.3.4)
cols_coord_df = [df['district'], df['offense_code'], df2b['code'], df2b['name'], df['occurred_on_date'], df['lat'], df['long']]
dfcc_condition = ((df['offense_code'] == df2b['code'])) | (df['offense_code'].startswith('00') & df['offense_code'].endswith(df2b['code']))
dfcc = df.join(df2b, dfcc_condition)\
    .select(cols_coord_df)\
    .withColumnRenamed('name', 'crime_type')\
    .sort(df['district'], df['occurred_on_date'], df['offense_code'])

dfca = dfcc.where((dfcc['lat'] == -1.0) & (dfcc['long'] == -1.0) & (dfcc['district'].isNull()))\
    .na.drop('any', subset=['lat', 'long'])

dfcb = dfcc.where(dfcc['lat'].isNull() & dfcc['long'].isNull() & dfcc['district'].isNull()).drop()

dfc = dfcc.exceptAll(dfca)
dfc = dfc.exceptAll(dfcb)
dfc = dfc.na.fill("0", ['lat'])\
        .na.fill("0", ['long'])
dfc.cache()



# итерируем районы
for d in districts:

    # для вычисления медианы правонарушений по месяцам, находим годы
    years = df.where(col('district') == d)\
        .select('year')\
        .distinct()\
        .sort('year')\
        .rdd.flatMap(lambda x: x)\
        .collect()

    # вычисляем общее число правонарушений, пишем в список
    crimes_total = df.where(df['district'] == d).count()

    district = []
    district.insert(0, d)
    district.insert(1, crimes_total)

    # вычисляем наиболее частые 3 типа правонарушения
    frequent_crimes = df.where(df['district'] == d)\
        .groupBy(df['offense_description'])\
        .count()\
        .sort(col('count').desc())\
        .head(3)
    crimes_chart = []
    for c in frequent_crimes:
        crime = c[0].lower()
        crimes_chart.append(crime)

    # вычисляем средние значения координат, заполняя вторую витрину
    dfcf = dfc.where(dfc['district'] == d)\
        .groupBy(dfc['district'], dfc['crime_type'])\
        .agg(avg('lat').alias('avg_lat'), avg('long').alias('avg_long'))

    # итерируем годы
    for y in years:
        # для вычисления медианы правонарушений по месяцам, находим месяцы
        months = df.where((col('district') == d) & (col('year') == y))\
            .select('month')\
            .distinct()\
            .rdd.flatMap(lambda x: x)\
            .collect()
        listOfMonths = []

        # итерируем месяцы
        for m in months:
            # вычисляем число правонарушений в месяц, пишем в список
            crimesPerMonth = df.where((col('district') == d) & (col('year') == y) & (col('month') == m)).count()
            listOfMonths.append(crimesPerMonth)
            
        # сортируем правонарушения по месяцам в порядке возрастания и вычисляем медиану
        listOfMonths.sort()
        medValue = str(median(listOfMonths))

        # пишем значения в список
        district.insert(2, str(y))
        district.insert(3, str(medValue))
        district.insert(4, str(crimes_chart))
        
        # заполняем первую витрину значениями
        if y == years[0]:
            districtOverYear = spark.createDataFrame([(district[0], district[1], district[2], district[3], district[4])], cols)
            report_df = report_df.union(districtOverYear)
        else:
            districtOverYear = spark.createDataFrame([("", "", district[2], district[3], "")], cols)
            report_df = report_df.union(districtOverYear)


    # подготавливаем соответствующие папки для отчётов
    path='output/'+str(d)
    os.makedirs(path, exist_ok=True)
    
    # сохраняем отчёты
    report_df.write.parquet(path+'/crime-statistics.parquet')
    dfcf.write.parquet(path+'/crimes-over-coordinates.parquet')

'''
РЕЗУЛЬТАТ: в папке output, получаем перечень районов
в каждой папке района два parquet файла
статистика и координаты
'''
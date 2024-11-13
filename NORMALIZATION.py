from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MySparkApp").getOrCreate()


df = spark.read.text("/home/Korouhanba/Downloads/individual+household+electric+power+consumption/household_power_consumption.txt")

from pyspark.sql.functions import split, explode, col

# Assuming the first row contains the header
header = df.first()[0]
header_list = header.split(';')

# Remove the first row (header)
df_without_header = df.filter(df.value != header)


# Split the data into columns
df_split = df_without_header.withColumn('split_value', split(df.value, ';'))

# Create a new dataframe with columns
new_df = df_split
for i, column_name in enumerate(header_list):
    new_df = new_df.withColumn(column_name, col('split_value').getItem(i))


# Drop the original 'value' column and the temporary 'split_value' column
new_df = new_df.drop('value').drop('split_value')
new_df.show(5)

from pyspark.sql.functions import min, max, count, when

for column_name in ['Global_active_power', 'Global_reactive_power', 'Voltage', 'Global_intensity']:
  new_df = new_df.withColumn(column_name, when(new_df[column_name] == '?', None).otherwise(new_df[column_name]))


new_df = new_df.na.drop(subset=['Global_active_power', 'Global_reactive_power', 'Voltage', 'Global_intensity'])

# Convert the columns to numeric type
new_df = new_df.withColumn('Global_active_power', new_df['Global_active_power'].cast('double'))
new_df = new_df.withColumn('Global_reactive_power', new_df['Global_reactive_power'].cast('double'))
new_df = new_df.withColumn('Voltage', new_df['Voltage'].cast('double'))
new_df = new_df.withColumn('Global_intensity', new_df['Global_intensity'].cast('double'))


# Calculate minimum, maximum, and count for specified columns
new_df.agg(
    min('Global_active_power').alias('Min Global Active Power'),
    max('Global_active_power').alias('Max Global Active Power'),
    count('Global_active_power').alias('Count Global Active Power'),
    min('Global_reactive_power').alias('Min Global Reactive Power'),
    max('Global_reactive_power').alias('Max Global Reactive Power'),
    count('Global_reactive_power').alias('Count Global Reactive Power'),
    min('Voltage').alias('Min Voltage'),
    max('Voltage').alias('Max Voltage'),
    count('Voltage').alias('Count Voltage'),
    min('Global_intensity').alias('Min Global Intensity'),
    max('Global_intensity').alias('Max Global Intensity'),
    count('Global_intensity').alias('Count Global Intensity')
).show()

from pyspark.sql.functions import mean, stddev

# Calculate the mean and standard deviation for specified columns
new_df.select(
    mean('Global_active_power').alias('Mean Global Active Power'),
    stddev('Global_active_power').alias('StdDev Global Active Power'),
    mean('Global_reactive_power').alias('Mean Global Reactive Power'),
    stddev('Global_reactive_power').alias('StdDev Global Reactive Power'),
    mean('Voltage').alias('Mean Voltage'),
    stddev('Voltage').alias('StdDev Voltage'),
    mean('Global_intensity').alias('Mean Global Intensity'),
    stddev('Global_intensity').alias('StdDev Global Intensity')
).show()


from pyspark.sql.functions import min as spark_min, max as spark_max, col

# Calculate the minimum and maximum values for the specified columns
min_max_values = new_df.select(
    spark_min('Global_active_power').alias('min_Global_active_power'),
    spark_max('Global_active_power').alias('max_Global_active_power'),
    spark_min('Global_reactive_power').alias('min_Global_reactive_power'),
    spark_max('Global_reactive_power').alias('max_Global_reactive_power'),
    spark_min('Voltage').alias('min_Voltage'),
    spark_max('Voltage').alias('max_Voltage'),
    spark_min('Global_intensity').alias('min_Global_intensity'),
    spark_max('Global_intensity').alias('max_Global_intensity')
).collect()[0]

# Define a function for min-max normalization
def min_max_normalize(value, min_value, max_value):
  if max_value - min_value == 0:
    return 0  # Handle cases where the range is zero to avoid division by zero
  return (value - min_value) / (max_value - min_value)


# Apply the normalization to the specified columns using the calculated min and max values
normalized_df = new_df.withColumn(
    'normalized_Global_active_power',
    min_max_normalize(col('Global_active_power'), min_max_values.min_Global_active_power, min_max_values.max_Global_active_power)
).withColumn(
    'normalized_Global_reactive_power',
    min_max_normalize(col('Global_reactive_power'), min_max_values.min_Global_reactive_power, min_max_values.max_Global_reactive_power)
).withColumn(
    'normalized_Voltage',
    min_max_normalize(col('Voltage'), min_max_values.min_Voltage, min_max_values.max_Voltage)
).withColumn(
    'normalized_Global_intensity',
    min_max_normalize(col('Global_intensity'), min_max_values.min_Global_intensity, min_max_values.max_Global_intensity)
)


normalized_df.select(
    'Global_active_power', 'normalized_Global_active_power',
    'Global_reactive_power', 'normalized_Global_reactive_power',
    'Voltage', 'normalized_Voltage',
    'Global_intensity', 'normalized_Global_intensity'
).show(5)



# Save the normalized data to a single CSV file
normalized_df.coalesce(1).write.csv('/home/Korouhanba/Downloads/normalized_data.csv', header=True, mode='overwrite')

normalized_df.show()

spark.stop()

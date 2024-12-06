# NORMALIZATION_IN_SPARK

This repo contains program for implementing normalization function on numerical data using Spark.

<h2>Dataset:</h2>

<p>The data is presented in a single text file, with each line representing a unique record. Each record is meticulously structured using semicolons to separate nine informative attributes:</p>

Date: Captures the specific day the measurement was taken.
Time: Provides the time within the day for the measurement.
Power Measurements:
global_active_power: Household's overall active power usage (kilowatts) averaged over a minute.
global_reactive_power: Household's overall reactive power usage (kilowatts) averaged over a minute.
voltage: Minute-averaged voltage (volts).
global_intensity: Minute-averaged current intensity (amperes).
Sub-metering Values:
sub_metering_1, sub_metering_2, sub_metering_3: Energy consumption readings (watt-hours) from specific appliances or areas within the household (details not provided).

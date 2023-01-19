// Downloading the necessary libraries
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, TimestampType,DoubleType, BooleanType, IntegerType, DateType}
import org.apache.spark.sql.RelationalGroupedDataset

// Creating SparkSession
val spark = SparkSession
  .builder()
  .appName("Taxi")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

// Creating a database structure
val dataSchema = new StructType ()
    .add(name="Vendor_ID", IntegerType)
    .add(name="Trep_pickup_datetime", DateType)
    .add(name="Trep_dropoff_datetime", DateType)
    .add(name="Passanger_count", IntegerType)
    .add(name="Trip_distance", DoubleType)
    .add(name="Ratecodeid", IntegerType)
    .add(name="Store_and_fwd_flag",BooleanType)
    .add(name="PulocationId",IntegerType) 
    .add(name="Dolocationid",IntegerType)
    .add(name="Payment_type",IntegerType) 
    .add(name="Fare_amount", DoubleType)
    .add(name="Extra", DoubleType)
    .add(name="Mta_tax",DoubleType) 
    .add(name="Tip_amount", DoubleType)
    .add(name="Tools_amount",DoubleType) 
    .add(name="Improvement_surchange", DoubleType)
    .add(name="Total_amount", DoubleType)
    .add(name="Congestion_surchange",DoubleType) 

// Uploading data from an external file to the database
val path = "hdfs://localhost:9099/user/puhalev_igor/yellow_tripdata_2020-01.csv"
val df = spark.read.format("csv").schema(dataSchema).load(path)

// Deleting unnecessary columns
val df_drop=df.drop("Vendor_ID")
            .drop("Trep_dropoff_datetime")
            .drop("Ratecodeid")
            .drop("Store_and_fwd_flag")
            .drop("PulocationId")
            .drop("Dolocationid")
            .drop("Payment_type")
            .drop("Fare_amount")
            .drop("Mta_tax")
            .drop("Tools_amount")
            .drop("Improvement_surchange")
            .drop("Congestion_surchange")
            .drop("Extra")

// We select values from the range of one month - January 2020 and delete the lines,
// where the tip, the cost and the length of the trip is less than 0
val df_january = df_drop.filter ("Trep_pickup_datetime  BETWEEN '2020-01-01' AND '2020-01-31' ")
                        .where("Total_amount>0")
                        .where("Tip_amount >= 0")
                        .where("Trip_distance>0")

//Creating a frame grouped by days for all trips
val df_trips_all = df_january.groupBy ("Trep_pickup_datetime")
                            .agg(collect_list("Trep_pickup_datetime").as("Trips_list"),count("*").as("all_trips"))
                            .select($"Trep_pickup_datetime", $"all_trips") 

//1 We create a frame for trips without passengers and calculate the min. and max. the cost of the trip
val df_trips_0_pass_min_max = df_january.select("Trep_pickup_datetime","Passanger_count", "Total_amount")
                            .where("Passanger_count=0") 
                            .groupBy ("Trep_pickup_datetime")
                            .agg (max("Total_amount"), min("Total_amount"))

//2 Creating a frame with the number of trips without passengers for each day
val df_trips_0_pass_count = df_january.select("Trep_pickup_datetime","Passanger_count", "Total_amount")
                            .where("Passanger_count=0") 
                            .groupBy ("Trep_pickup_datetime")
                            .agg(collect_list("Passanger_count").as("Pass_list"),count("*").as("total_count"))
                            .select($"Trep_pickup_datetime", $"total_count")

//3 We combine three frames into one to calculate the percentage of trips without passengers from the total number
val df_trips_0_pass_un = df_trips_all.join(df_trips_0_pass_min_max, Seq("Trep_pickup_datetime"))
                                    .join(df_trips_0_pass_count, Seq("Trep_pickup_datetime"))

//4 Creating a final frame for trips without passengers
val df_trips_0_pass_res = df_trips_0_pass_un.select($"Trep_pickup_datetime".as ("Date"),
                                ($"total_count"*100/$"all_trips").cast("NUMERIC(10,2)").as("%_0_p"),
                                $"max(Total_amount)".as ("max$-0p"),
                                $"min(Total_amount)".as ("min$-0p"))
//df_trips_0_pass_res show()

//Repeat steps 1-4 for trips with 1, 2, 3 and more than 3 passengers
val df_trips_1_pass_min_max = df_january.select("Trep_pickup_datetime", "Passanger_count", "Total_amount")
                            .where("Passanger_count=1")
                            .groupBy ("Trep_pickup_datetime")
                            .agg (max("Total_amount"), min("Total_amount")) 
val df_trips_1_pass_count = df_january.select("Trep_pickup_datetime","Passanger_count", "Total_amount")
                            .where("Passanger_count=1") 
                            .groupBy ("Trep_pickup_datetime")
                            .agg(collect_list("Passanger_count").as("Pass_list"),count("*").as("total_count"))
                            .select($"Trep_pickup_datetime", $"total_count")   
val df_trips_1_pass_un = df_trips_all.join(df_trips_1_pass_min_max, Seq("Trep_pickup_datetime"))
                                    .join(df_trips_1_pass_count, Seq("Trep_pickup_datetime"))
val df_trips_1_pass_res = df_trips_1_pass_un.select($"Trep_pickup_datetime".as ("Date"),
                                ($"total_count"*100/$"all_trips").cast("NUMERIC(10,2)").as("%_1_p"),
                                $"max(Total_amount)".as ("max$-1p"),
                                $"min(Total_amount)".as ("min$-1p"))
//df_trips_1_pass_min_max show()
//df_trips_1_pass_count show()
//df_trips_1_pass_un show()
//df_trips_1_pass_res show()

val df_trips_2_pass_min_max = df_january.select("Trep_pickup_datetime", "Passanger_count", "Total_amount")
                            .where("Passanger_count=2")
                            .groupBy ("Trep_pickup_datetime")
                            .agg (max("Total_amount"), min("Total_amount")) 
val df_trips_2_pass_count = df_january.select("Trep_pickup_datetime","Passanger_count", "Total_amount")
                            .where("Passanger_count=2") 
                            .groupBy ("Trep_pickup_datetime")
                            .agg(collect_list("Passanger_count").as("Pass_list"),count("*").as("total_count"))
                            .select($"Trep_pickup_datetime", $"total_count")   
val df_trips_2_pass_un = df_trips_all.join(df_trips_2_pass_min_max, Seq("Trep_pickup_datetime"))
                                    .join(df_trips_2_pass_count, Seq("Trep_pickup_datetime"))
val df_trips_2_pass_res = df_trips_2_pass_un.select($"Trep_pickup_datetime".as ("Date"),
                                ($"total_count"*100/$"all_trips").cast("NUMERIC(10,2)").as("%_2_p"),
                                $"max(Total_amount)".as ("max$-2p"),
                                $"min(Total_amount)".as ("min$-2p"))

val df_trips_3_pass_min_max = df_january.select("Trep_pickup_datetime", "Passanger_count", "Total_amount")
                            .where("Passanger_count=3")
                            .groupBy ("Trep_pickup_datetime")
                            .agg (max("Total_amount"), min("Total_amount")) 
val df_trips_3_pass_count = df_january.select("Trep_pickup_datetime","Passanger_count", "Total_amount")
                            .where("Passanger_count=3") 
                            .groupBy ("Trep_pickup_datetime")
                            .agg(collect_list("Passanger_count").as("Pass_list"),count("*").as("total_count"))
                            .select($"Trep_pickup_datetime", $"total_count")   
val df_trips_3_pass_un = df_trips_all.join(df_trips_3_pass_min_max, Seq("Trep_pickup_datetime"))
                                    .join(df_trips_3_pass_count, Seq("Trep_pickup_datetime"))
val df_trips_3_pass_res = df_trips_3_pass_un.select($"Trep_pickup_datetime".as ("Date"),
                                ($"total_count"*100/$"all_trips").cast("NUMERIC(10,2)").as("%_3_p"),
                                $"max(Total_amount)".as ("max$-3p"),
                                $"min(Total_amount)".as ("min$-3p"))

val df_trips_4_pass_min_max = df_january.select("Trep_pickup_datetime", "Passanger_count", "Total_amount")
                            .where("Passanger_count>3")
                            .groupBy ("Trep_pickup_datetime")
                            .agg (max("Total_amount"), min("Total_amount")) 
val df_trips_4_pass_count = df_january.select("Trep_pickup_datetime","Passanger_count", "Total_amount")
                            .where("Passanger_count>3") 
                            .groupBy ("Trep_pickup_datetime")
                            .agg(collect_list("Passanger_count").as("Pass_list"),count("*").as("total_count"))
                            .select($"Trep_pickup_datetime", $"total_count")   
val df_trips_4_pass_un = df_trips_all.join(df_trips_4_pass_min_max, Seq("Trep_pickup_datetime"))
                                    .join(df_trips_4_pass_count, Seq("Trep_pickup_datetime"))
val df_trips_4_pass_res = df_trips_4_pass_un.select($"Trep_pickup_datetime".as ("Date"),
                                ($"total_count"*100/$"all_trips").cast("NUMERIC(10,2)").as("%_4_p"),
                                $"max(Total_amount)".as ("max$-4p"),
                                $"min(Total_amount)".as ("min$-4p"))

//Creating the final showcase
val df_trips_result = df_trips_0_pass_res.join(df_trips_1_pass_res, Seq("Date"))
                                    .join(df_trips_2_pass_res, Seq("Date"))
                                    .join(df_trips_3_pass_res, Seq("Date"))
                                    .join(df_trips_4_pass_res, Seq("Date"))

df_trips_result show(31, false)


from schema import BRONZE_SCHEMAS, SILVER_SCHEMAS, GOLD_SCHEMAS
from session import create_spark_session

spark = create_spark_session()

medallion_mappings = {
    "bronze": BRONZE_SCHEMAS,
    "silver": SILVER_SCHEMAS,
    "gold": GOLD_SCHEMAS
}

for layer_name, schema_dict in medallion_mappings.items():
    print(f"\n--- Provisioning tables for {layer_name.upper()} layer ---")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{layer_name}")
    
    for table_name, schema_obj in schema_dict.items():
        full_table_path = f"spark_catalog.{layer_name}.{table_name}"
        
        try:
            empty_df = spark.createDataFrame([], schema=schema_obj)
            
            empty_df.writeTo(full_table_path).create()
            
            print(f"✅ Created: {full_table_path}")

                
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg:
                print(f"⚠️ Skipped (Already exists): {full_table_path}")
            else:
                print(f"❌ Failed to create {full_table_path}")
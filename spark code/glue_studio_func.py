def MyTransform (glueContext, dfc) -> DynamicFrameCollection:
	selected = dfc.select(list(dfc.keys())[0]).toDF()
	selected.createOrReplaceTempView("ticketcount")
	totals = spark.sql("select court_location as location,infraction_description as infraction,count(infraction_code) as total from ticketcount group by court_location,infraction_description order by court_location asc")
	final_result = DynamicFrame.fromDF(totals,glueContext,"final_result_df")
	return DynamicFrameCollection({'results' : final_result}, glueContext)

create external table if not exists liutuozhen.airdelay (
	Year int,                                                                                                                                        
	Month int,                                                                                                                                       
	DayofMonth int,                                                                                                                                  
	DayofWeek int,                                                                                                                                   
	DepTime  int,                                                                                                                                    
	CRSDepTime  int,                                                                                                                                 
	ArrTime  int,                                                                                                                                    
	CRSArrTime  int,                                                                                                                                 
	UniqueCarrier String,                                                                                                                              
	FlightNum int,                                                                                                                                   
	TailNum String,                                                                                                                                    
	ActualElapsedTime int,                                                                                                                           
	CRSElapsedTime int,                                                                                                                              
	AirTime int,                                                                                                                                     
	ArrDelay int,                                                                                                                                    
	DepDelay int,                                                                                                                                    
	Origin String,                                                                                                                                     
	Dest String,                                                                                                                                       
	Distance int,                                                                                                                                    
	TaxiIn int,                                                                                                                                      
	TaxiOut int,                                                                                                                                     
	Cancelled int,                                                                                                                                   
	CancellationCode int,                                                                                                                            
	Diverted int,                                                                                                                                    
	CarrierDelay int,                                                                                                                                
	WeatherDelay int,                                                                                                                                
	NASDelay int,                                                                                                                                    
	SecurityDelay int,                                                                                                                               
	LateAircraftDelay int)                                                                                                                          
row format delimited fields terminated by ','                                                                                                          
location '/lifeng/student/liutuozhen/airdelay'
TBLPROPERTIES ('skip.header.line.count'='1');
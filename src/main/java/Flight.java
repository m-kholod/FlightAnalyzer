import java.io.Serializable;

public class Flight implements Serializable {
    int Year;               //1987-2008
    int Month;	            //1-12
    int DayOfMonth;	        //1-31
    int DayOfWeek;	        //1 (Monday) - 7 (Sunday)
    int DepTime;            //actual departure time (local, hhmm)
    int CRSDepTime;         //scheduled departure time (local, hhmm)
    int ArrTime;            //actual arrival time (local, hhmm)
    int CRSArrTime;         //scheduled arrival time (local, hhmm)
    String UniqueCarrier;   //unique carrier code
    String FlightNum;       //flight number
    String TailNum;         //plane tail number
    int ActualElapsedTime;  //in minutes
    int CRSElapsedTime;     //in minutes
    int AirTime;            //in minutes
    int ArrDelay;           //arrival delay, in minutes
    int DepDelay;           //departure delay, in minutes
    String Origin;          //origin IATA airport code
    String Dest;            //destination IATA airport code
    int Distance;           //in miles
    int TaxiIn;             //taxi in time, in minutes
    int TaxiOut;            //taxi out time in minutes
    boolean Cancelled;      //was the flight cancelled?
    char CancellationCode;  //reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
    boolean Diverted;       //1 = yes, 0 = no
    int CarrierDelay;       //in minutes
    int WeatherDelay;       //in minutes
    int NASDelay;           //in minutes
    int SecurityDelay;      //in minutes
    int LateAircraftDelay;  //in minutes
}

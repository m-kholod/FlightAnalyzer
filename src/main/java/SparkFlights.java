import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkFlights {
    public static void main(String[] args) {

        if (args.length != 4) {
            System.err.println("Usage: SparkFlights <flights path> <carriers path> <airports path> <output path>");
            System.exit(-1);
        }

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkFlights")
                .getOrCreate();

        Dataset<Flight> flights = sparkSession.read()
                .option("header", "true")
                .csv(args[0])
                .as(Encoders.bean(Flight.class));

        Dataset<Carrier> carriers = sparkSession.read()
                .option("header", "true")
                .csv(args[1])
                .as(Encoders.bean(Carrier.class));

        Dataset<Airport> airports = sparkSession.read()
                .option("header", "true")
                .csv(args[2])
                .as(Encoders.bean(Airport.class));

        String outputFolder = args[3];

        //Count total number of flights per carrier in 2007
        int year = 2007;
        String totalOutputFile = "/total_flights_per_carrier.tsv";

        saveToFile(FlightCalculations.flightsPerCarrier(flights, carriers, year),
                outputFolder + totalOutputFile);

        //Find total number of flights served in Jun 2007 by NYC (all airports, use join with Airports data)
        String city = "New York";
        String state = "NY";
        String country = "USA";
        int month = 6;
        String junNycFlights = "/nyc_june_flights.tsv";

        saveToFile(FlightCalculations.flightsByDateAndCity(flights, airports, city, state, country, month, year),
                outputFolder + junNycFlights);

        //Find five most busy airports in US during Jun 01 - Aug 31.
        int startMonth = 6;
        int endMonth = 8;
        int limit = 5;
        String busiestAirports = "/busiest_airports.tsv";

        saveToFile(FlightCalculations.busiestAirports(flights, airports, country, startMonth, endMonth, year, limit),
                outputFolder + busiestAirports);

        //Find the carrier who served the biggest number of flights
        String carrierWithBiggestNumberOfFlights = "/carrier_with_biggest_number_of_flights.tsv";

        saveToFile(FlightCalculations.carrierWithBiggestNumberOfFlights(flights, carriers),
                outputFolder + carrierWithBiggestNumberOfFlights);
    }

    static void saveToFile(Dataset<Row> data, String outputFile) {
        data.repartition(1)
                .write()
                .option("header", "true")
                .csv(outputFile);
    }
}

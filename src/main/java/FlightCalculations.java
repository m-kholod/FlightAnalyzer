import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.desc;

public class FlightCalculations {

    static Dataset<Row> flightsPerCarrier(Dataset<Flight> flights, Dataset<Carrier> carriers, int year) {
        return flights.join(carriers, flights.col("UniqueCarrier").equalTo(carriers.col("Code")), "left_outer")
                .where("Year == " + year)
                .groupBy("Description")
                .count()
                .sort(desc("count"));
    }

    static Dataset<Row> flightsByDateAndCity(Dataset<Flight> flights, Dataset<Airport> airports, String city, String state, String country, int month, int year) {
        Dataset<Row> filteredAirports = airports.select("iata")
                .where(String.format("city == \"%s\" and state == \"%s\" and country == \"%s\"", city, state, country));

        return flights.join(filteredAirports, flights.col("Origin").equalTo(airports.col("iata")), "left_semi")
                .union(flights.join(filteredAirports, flights.col("Dest")
                        .equalTo(airports.col("iata")), "left_semi"))
                .where(String.format("Year == %d and Month == %d", year, month))
                .groupBy()
                .count();
    }

    static Dataset<Row> busiestAirports(Dataset<Flight> flights, Dataset<Airport> airports, String country, int startMonth, int endMonth, int year, int limit) {
        Dataset<Row> filteredAirports = airports.select("iata", "airport")
                .where(String.format("country == \"%s\"", country));

        return flights.join(filteredAirports, flights.col("Origin").equalTo(airports.col("iata")), "inner")
                .union(flights.join(filteredAirports, flights.col("Dest")
                        .equalTo(airports.col("iata")), "inner"))
                .where(String.format("Year == %d and Month >= %d and Month <= %d", year, startMonth, endMonth))
                .groupBy("airport")
                .count()
                .sort(desc("count"))
                .limit(limit);
    }

    static  Dataset<Row> carrierWithBiggestNumberOfFlights(Dataset<Flight> flights, Dataset<Carrier> carriers) {
        return flights.join(carriers, flights.col("UniqueCarrier").equalTo(carriers.col("Code")), "inner")
                .groupBy("Description")
                .count()
                .sort(desc("count"))
                .limit(1);
    }
}

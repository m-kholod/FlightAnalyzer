import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class FlightCalculationsTest {
    static Dataset<Flight> flights;
    static Dataset<Carrier> carriers;
    static Dataset<Airport> airports;

    @BeforeClass
    static public void setUp() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("SparkFlights")
                .getOrCreate();

        flights = sparkSession.read()
                .option("header", "true")
                .csv("flights/2007.csv")
                .as(Encoders.bean(Flight.class));

        carriers = sparkSession.read()
                .option("header", "true")
                .csv("carriers/carriers.csv")
                .as(Encoders.bean(Carrier.class));

        airports = sparkSession.read()
                .option("header", "true")
                .csv("airports/airports.csv")
                .as(Encoders.bean(Airport.class));
    }

    @Test
    public void testFlightsPerCarrier() {
        int year = 2007;
        Dataset<Row> result = FlightCalculations.flightsPerCarrier(flights, carriers, year);
        Assert.assertEquals(20, result.count());
        Assert.assertEquals("Southwest Airlines Co.", result.first().getAs("Description"));
        Assert.assertEquals(1168871, (long)result.first().getAs("count"));
    }

    @Test
    public void testFlightsByDateAndCity() {
        String city = "New York";
        String state = "NY";
        String country = "USA";
        int month = 6;
        int year = 2007;
        Dataset<Row> result = FlightCalculations.flightsByDateAndCity(flights, airports, city, state, country, month, year);
        Assert.assertEquals(41280, (long)result.first().getAs("count"));
    }

    @Test
    public void testBusiestAirports() {
        String country = "USA";
        int startMonth = 6;
        int endMonth = 8;
        int year = 2007;
        int limit = 1;
        Dataset<Row> result = FlightCalculations.busiestAirports(flights, airports, country, startMonth, endMonth, year, limit);
        Assert.assertEquals("William B Hartsfield-Atlanta Intl", result.first().getAs("airport"));
        Assert.assertEquals(220506, (long)result.first().getAs("count"));
    }

    @Test
    public void testCarrierWithBiggestNumberOfFlights() {
        Dataset<Row> result = FlightCalculations.carrierWithBiggestNumberOfFlights(flights, carriers);
        Assert.assertEquals("Southwest Airlines Co.", result.first().getAs("Description"));
        Assert.assertEquals(1168871, (long)result.first().getAs("count"));
    }
}

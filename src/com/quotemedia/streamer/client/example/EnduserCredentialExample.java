package com.quotemedia.streamer.client.example;

import com.quotemedia.streamer.client.Datatype;
import com.quotemedia.streamer.client.OnClose;
import com.quotemedia.streamer.client.OnError;
import com.quotemedia.streamer.client.OnMessage;
import com.quotemedia.streamer.client.OnResponse;
import com.quotemedia.streamer.client.Stream;
import com.quotemedia.streamer.client.StreamAtmoFactory;
import com.quotemedia.streamer.client.StreamCfg;
import com.quotemedia.streamer.client.StreamException;
import com.quotemedia.streamer.client.StreamFactory;
import com.quotemedia.streamer.client.Subscription;
import com.quotemedia.streamer.client.auth.AuthClient;
import com.quotemedia.streamer.client.auth.AuthClientFactory;
import com.quotemedia.streamer.client.auth.cfg.AuthCfg;
import com.quotemedia.streamer.client.auth.cfg.AuthCfgBuilder;
import com.quotemedia.streamer.client.cfg.Config;
import com.quotemedia.streamer.client.cfg.ConfigProgrammatic;
import com.quotemedia.streamer.messages.MimeTypes;
import com.quotemedia.streamer.messages.control.BaseResponse;
import com.quotemedia.streamer.messages.control.ConnectResponse;
import com.quotemedia.streamer.messages.control.ResponseCodes;
import com.quotemedia.streamer.messages.control.StatsResponse;
import com.quotemedia.streamer.messages.control.SubscribeResponse;
import com.quotemedia.streamer.messages.control.UnsubscribeResponse;
import com.quotemedia.streamer.messages.market.DataMessage;
import com.quotemedia.streamer.messages.market.PriceData;
import com.quotemedia.streamer.messages.market.Quote;
import com.quotemedia.streamer.messages.market.Trade;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
//import org.slf4j.LoggerFactory;


//import ch.qos.logback.classic.Level;
//import ch.qos.logback.classic.Logger;

import java.util.logging.Level;

import java.util.logging.Logger;

import java.util.logging.SimpleFormatter;

import java.util.logging.FileHandler;

import java.util.logging.LogRecord;

/**
 * Demonstrates how to:<br>
 * <br>
 * - Configure and create a market data stream<br>
 * - Connect the market data stream to the streaming service using enduser credentials<br>
 * - Subscribe and unsubscribe for Level 1 market data<br>
 * - Retrieve number of available connections and symbols, current number of opened connections and subscribed symbols<br>
 * - Close the stream when no longer needed<br>
 */
public class EnduserCredentialExample{
    /**
     * Adjust authentication credentials to your account.
     */
    public static final String AUTH_WMID = "103680";
    public static final String AUTH_USERNAME = "VSenthil1";
    public static final String AUTH_PASSWORD = "Senthil2021";

    public static final String[] SYMBOL_LIST = new String[] {"$USDJPY", "$AUDUSD", "$NZDUSD", "$EURUSD", "$GBPUSD", "$NOKJPY", "$SEKJPY",
            "/VXM24", "/VXN24", "/VXQ24", "/VXU24", "/VXV24", "/VXX24", "/VXZ24",
            "/ESM4", "/SIN4:CMX",
//            "@QQQ   240515P00432000", "@QQQ   240515P00447000", "@QQQ   240515P00448000",
//           "/ESU4", "/ESZ4", "/CLM4:NMX", "/CL", "/HG", "/ZW", "/NQ", "/ZC", "/ZS", "/BZ", "/YM", "/LC", "/LH", "/GC", "/SI", "/ES",
            "QQQ", "SPY", "IWM", "XLK", "GDXJ", "GLD", "GDX", "USO", "UCO"};
//            "UNG", "BOIL", "SPY", "SQQQ",
//            "DAKT", "KOPN", "AMZN", "AAPL", "TSLA", "META", "MSFT", "GOOG", "NDXP", "GOOGL", "AVGO", "NVDA",
//            "FIZZ", "CNX", "LANC", "AOS", "PDCE", "CDNS", "PAR", "AXP", "SIG", "DVN", "HIBB", "CCI", "HOLX", "KLAC",
//            "IPG", "JPM", "DXC", "ALK", "MA", "RVTY", "AFL", "BGFV", "PXD", "MUR", "CAG", "SGC", "KIM", "FIS", "PEG",
//            "FITB", "ITW", "VZ", "EQC", "ROST", "AEP", "REG", "EOG", "WMB", "SLB", "ED", "T", "MU", "NKE", "BXMT",
//            "CLX", "EMR", "NOC", "ANSS", "IP", "CPB", "OVV", "NEE", "MDT", "CMA", "FWRD", "LNC", "ROK", "INTU", "APA",
//            "KMB", "CPT", "FLR", "MAS", "CHD", "LIN", "PNC", "ON", "OXY", "UMH", "AMT", "PDCO", "BAC", "TT",
//            "B", "CTAS", "AME", "SITC", "A", , "OII", "ACN", "GL", "RTX", "VFC", "NHI", "LMT", "DUK", "AJG",
//            "ANIK", "APD", "AMWD", "HSY", "SBAC", "CL", "GIII", "JNJ", "AIZ", "APOG", "MYE", "ADBE", "KLIC", "HP",
//            "LEN", "EIX", "AXGN", "LXP", "ETN", "LUV", "VRE", "C", "KO", "EQR", "AMAT", "CXW", "VRTX", "TXT", "ADSK",
//            "IBCP", "MRK", "WMT", "DIOD", "GILD", "CPRT", "REGN", "ROP", "PVH", "TJX", "OI", "DTE", "K", "BA", "NHC",
//            "HAL", "ABT", "SWN", "SYK", "BSX", "DFS", "EW", "HRL", "MMC", "CHS", "WM", "LHX", "ITRI", "DLR", "GE",
//            "ETR", "PEP", "MAA", "FSS", "COST", "SO", "ADI", "MBI", "ROL", "BIIB", "GIS", "CAKE", "OMC", "GBX", "PPL",
//            "SCVL", "SWKS", "GD", "ZION", "AVY"};
//            "AAPL", "MUR", "BG", "TPR", "EQT", "SO", "CBRE", "NCLH", "KR", "MKC", "BEN", "LYV", "PFE", "DIOD",
//            "VRTX", "PPL", "PAR", "CHS", "AVB", "WBD", "CAKE", "BXMT", "ORCL", "AKAM", "EQC", "WRB", "COP",
//            "GM", "MCK", "ALLE", "NDAQ", "CVS", "L", "XEL", "ROK", "TT", "ETR", "AJG", "AMWD", "AMD", "FIZZ",
//            "REGN", "ICE", "EFX", "PAYC", "UHS", "K", "HWM", "SWN", "PEAK", "LANC", "EMR", "JBHT", "AOS", "SITC",
//            "TDY", "TMUS", "COO", "MYE", "PARA", "STT", "MAA", "FANG", "TRGP", "APOG", "SNA", "GS", "PODD", "KHC",
//            "SPG", "CTLT", "DD", "CMG", "MSI", "DLTR", "WM", "PLD", "MDT", "NWS", "PFG", "HAS", "CE", "SHW", "GLW",
//            "GOOG", "JNPR", "MMM", "JKHY", "PYPL", "CHRW", "APH", "C", "MTCH", "AAL", "MHK", "XRAY", "EXPD", "TYL",
//            "LW", "RSG", "STZ", "WELL", "KLIC", "MAS", "WEC", "CINF", "ZBRA", "ABT", "MRK", "CME", "KDP", "NFLX",
//            "OTIS", "LYB", "EBAY", "PEP", "KLAC", "IP", "GEHC", "SYF", "RF", "MTB", "UAL", "SBUX", "NDSN", "USB",
//            "TFX", "GE", "ABBV", "INVH", "HBAN", "JCI", "HPQ", "EG", "RJF", "EOG", "WDC", "DUK", "NTAP", "DRI",
//            "WFC", "RL", "ADM", "MU", "PDCO", "BLK", "CSCO", "PSA", "APA", "DOW", "ATVI", "TAP", "RVTY",
//            "NUE", "MTD", "CDAY", "AWK", "PWR", "CHD", "AVY", "B", "EA", "PSX", "CPRT", "PNR", "VZ", "DVN", "STX",
//            "LMT", "LIN", "ARE", "CSGP", "PRU", "HSY", "MPC", "SWKS", "NEM", "CMS", "ADBE", "CLX", "GIII", "CAT",
//            "MAR", "UNH", "MCHP", "SBAC", "CMCSA", "PCAR", "FICO", "BBY", "INTU", "AIG", "LXP", "KMB", "FAST", "DPZ",
//            "ANET", "INCY", "PNC", "DHI", "INTC", "ALK", "IDXX", "CTAS", "QCOM", "TGT", "FWRD", "STE", "IRM",
//            "KMX", "EXC", "MMC", "NOW", "CCL", "ZTS", "NVDA", "KMI", "CCI", "FITB", "TER", "GPC", "FFIV", "CL",
//            "KOPN", "PANW", "CSX", "COR", "WAB", "NKE", "NI", "HD", "XYL", "HIG", "GNRC", "BKNG", "VFC", "VICI",
//            "TDG", "VTRS", "ITW", "DE", "ALB", "LOW", "CXW", "WMB", "FOX", "SIG", "DIS", "RHI", "VRSN", "APD",
//            "ILMN", "CVX", "PTC", "CAH", "O", "ROST", "MOH", "GIS", "PPG", "GILD", "FIS", "FSLR", "BR", "NVR",
//            "IVZ", "FMC", "ACN", "PEG", "PH", "DFS", "FDX", "GBX", "KO", "SLB", "ZBH", "VLO", "WYNN", "AIZ",
//            "TROW", "CNC", "EXR", "TMO", "HRL", "EW", "DXC", "NOC", "PCG", "MCD", "HSIC", "AFL", "LVS", "LNC",
//            "IEX", "AMZN", "MGM", "ADI", "CARR", "RCL", "EXPE", "NXPI", "MA", "BALL", "LDOS", "TSLA", "EL", "EIX",
//            "MPWR", "GOOGL", "ADP", "ALL", "WBA", "ED", "MET", "OXY", "LEN", "CMI", "SNPS", "CRL", "TRV", "ACGL",
//            "IQV", "MBI", "MOS", "BBWI", "CZR", "AEE", "CPT", "VMC", "DVA", "FTNT", "RTX", "BWA", "NRG", "A",
//            "ULTA", "CAG", "PNW", "SEDG", "CTVA", "BMY", "LLY", "BKR", "CDNS", "ROP", "DAKT", "LNT", "JPM", "HAL",
//            "ANSS", "ANIK", "ECL", "BRO", "NTRS", "IT", "LKQ", "CTRA", "D", "NWSA", "FTV", "PKG", "AEP", "ELV",
//            "VTR", "HES", "ROL", "TSCO", "GL", "GWW", "TFC", "GPN", "AZO", "EMN", "CTSH", "SEE", "MCO", "OII",
//            "KIM", "IPG", "PGR", "IBCP", "XOM", "BA", "ESS", "DTE", "NSC", "WTW", "BIO", "ODFL", "WMT", "IR", "WAT",
//            "AME", "MKTX", "UMH", "BF.B", "RMD", "YUM", "HIBB", "WY", "TJX", "ALGN", "COST", "T", "BSX", "STLD",
//            "MDLZ", "CB", "ITRI", "CMA", "SRE", "JNJ", "TXN", "BDX", "QRVO", "LUV", "KVUE", "CFG", "BGFV", "FRT",
//            "OVV", "DGX", "MSCI", "URI", "MS", "WST", "ATO", "TSN", "EVRG", "ES", "ADSK", "SCHW", "PXD", "EQR",
//            "HON", "HLT", "DXCM", "UNP", "FI", "ORLY", "NEE", "DLR", "GRMN", "ZION", "MO", "META", "LH", "FTRE",
//            "SYK", "CDW", "BXP", "CNP", "MSFT", "IBM", "SJM", "GD", "PHM", "OMC", "CRM", "HCA", "CF", "CI", "POOL",
//            "AON", "MRNA", "REG", "BAX", "AMGN", "AMP", "DAL", "DG", "AVGO", "CEG", "MNST", "VRE", "FLR", "FCX",
//            "FE", "TRMB", "TECH", "SGC", "BK", "FOXA", "AXON", "FLT", "UPS", "LRCX", "CPB", "AXGN", "V", "LHX",
//            "EQIX", "NWL", "GEN", "CNX", "DHR", "NHI", "CHTR", "WRK", "HPE", "HII", "BAC", "HOLX", "TEL", "J",
//            "CBOE", "SWK", "PM", "ISRG", "HST", "ETSY", "APTV", "AMCR", "HP", "OKE", "AMAT", "MRO", "F", "PAYX",
//            "ENPH", "EPAM", "KEY", "IFF", "HUM", "DOV", "SPGI", "AMT", "UDR", "SCVL", "PVH", "BRK.B", "WHR",
//            "TTWO", "OGN", "AES", "VRSK", "SYY", "KEYS", "NHC", "FDS", "AXP", "FSS", "COF", "ETN", "OI", "TXT",
//            "ON", "PG", "BIIB", "MLM"};

    private StreamFactory factory;
    private Stream stream;

    String bootstrapServers= "13.214.14.60:9092";//"13.214.14.60:9092";//"127.0.0.1:9092";
    Properties properties = new Properties();


    Logger qm_logger = Logger.getLogger("MyLog");

    FileHandler fh;

    /**
     * Demonstrates example streaming-service api usage.
     *
     * @throws Exception if something goes wrong
     */
    private void run() throws Exception {

        final AuthClient auth = new AuthClientFactory().create(Cfg.AuthCfg());

        fh = new FileHandler("/tmp/java_qm_kafka.log");

        SimpleFormatter formatter = new SimpleFormatter() {

            private static final String format = "%1$tF %1$tT [%2$-7s] %3$s %n";

            @Override

            public synchronized String format(LogRecord lr) {

                return String.format(format,

                        new java.util.Date(lr.getMillis()),

                        lr.getLevel().getLocalizedName(),

                        lr.getMessage()

                );

            }

        };
        qm_logger.setLevel(Level.INFO);
        fh.setFormatter(formatter);
        qm_logger.addHandler(fh);

        /* Step one: Setup stream */
        // Create factory with configuration
        this.factory = new StreamAtmoFactory(Cfg.Config(Cfg.AuthCfg()));

        // Use factory to create stream
        this.stream = this.factory.create();

        final String authtoken = auth.auth(
                AUTH_WMID,
                AUTH_USERNAME,
                AUTH_PASSWORD);

        final String Sid_val = new StreamCfg.AuthSid(authtoken).sid();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating producer
        final KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(properties);

        // Register message callback
        this.stream.onmessage(new OnMessage<DataMessage>() {
            @Override
            public void message(final DataMessage msg) {
                // This code is called asynchronously when new data is available
//                onmessage(msg);
//                System.out.println(Sid_val);

                String msg_sid = msg.toString() + ", " + Sid_val;

                System.out.println(msg_sid);

                qm_logger.info(msg.toString());

                //Creating producer record
                ProducerRecord<String, String> record=new ProducerRecord<String, String>("QM-DATA", msg_sid);

                //Sending the data
                first_producer.send(record);

//                first_producer.flush();
            }
        });
        // Register error callback
        this.stream.onerror(new OnError() {
            @Override
            public void error(final StreamException e) {
                // This code is called asynchronously if the stream breaks
                onerror(e);
            }
        });

        /* Step two: Connect stream and subscribe/un-subscribe for data */
        // Connect stream to server
//        final ConnectResponse connected = this.stream.open(Cfg.StreamCfg(
//                Cfg.AuthCredentials(
//                        AUTH_WMID,
//                        AUTH_USERNAME,
//                        AUTH_PASSWORD
//                )));
        final ConnectResponse connected = this.stream.open(Cfg.StreamCfg(
                new StreamCfg.AuthSid(authtoken)
        ));
        check(connected);

        // As well as per connection, there is ability to specify conflation per subscription request.
        // If conflation is not specified, default conflation value is used.
        // Please note. A matching conflation must be supplied when unsubscribing.
        final Subscription sub = new Subscription.Builder()
                .symbols(SYMBOL_LIST)
                .types(Datatype.QUOTE, Datatype.PRICEDATA, Datatype.TRADE)
                .conflation(Stream.CONFLATION.DEFAULT)
                .build();
        // Subscribe for market data
        final SubscribeResponse subscribed = this.subscribe(sub);
        check(subscribed);

        // Retrieve stats
        final StatsResponse stats = this.getSessionStats();
        check(stats);
        System.out.println(stats);

//        waitforenter();
        CompletableFuture.runAsync(() -> {
            System.out.println("WAIT THREAD STARTED");
            while(true) {
                try{
                    int i =System.in.read();
                    if (i > 0){
                        break;
                    }
                    Thread.sleep(250);
                }catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("WAIT THREAD ENDED");
        }).join();

        // Un-subscribe
        final UnsubscribeResponse unsubscribed = this.unsubscribe(sub);
        check(unsubscribed);

        // Close stream
        this.closestream();
    }

    private void onmessage(final DataMessage m) {
        if (m instanceof Quote) {
            System.out.println(m);
            // Handle quote
        } else if (m instanceof PriceData) {
            System.out.println(m);
            // Handle price data
        } else if (m instanceof Trade) {
            System.out.println(m);
            // Handle price data
        }
    }

    private void onerror(final StreamException e) {
        // Handle error
        qm_logger.info(e.toString());
    }

    /**
     * Subscribes for streaming market data.<br>
     * This call waits for the response to succeed or fail before returning.
     *
     * @param s the subscribe request
     * @return the subscribe response
     * @throws Exception if request failed
     */
    private SubscribeResponse subscribe(final Subscription s) throws Exception {
        final FutureResponse<SubscribeResponse> response = new FutureResponse<>();
        this.stream.subscribe(s, new OnResponse<SubscribeResponse>() {
            @Override
            public void response(final SubscribeResponse r) {
                // This code is called asynchronously when response arrives
                response.response(r);
            }

            @Override
            public void error(final Exception e) {
                // This code is called asynchronously if request fails
                response.exception(e);
            }
        });
        return response.get();
    }

    /**
     * Retrieves stats for session.<br>
     * This call waits for the response to succeed or fail before returning.
     *
     * @return the stats response
     * @throws Exception if request failed
     */
    private StatsResponse getSessionStats() throws Exception {
        final FutureResponse<StatsResponse> response = new FutureResponse<>();
        this.stream.getSessionStats(new OnResponse<StatsResponse>() {
            @Override
            public void response(final StatsResponse r) {
                // This code is called asynchronously when response arrives
                response.response(r);
            }

            @Override
            public void error(final Exception e) {
                // This code is called asynchronously if request fails
                response.exception(e);
            }
        });
        return response.get();
    }

    /**
     * Un-subscribes from streaming market data.<br>
     * This call waits for the response to succeed or fail before returning.
     *
     * @param s the un-subscribe request
     * @return the un-subscribe response
     * @throws Exception if request failed
     */
    private UnsubscribeResponse unsubscribe(final Subscription s) throws Exception {
        final FutureResponse<UnsubscribeResponse> response = new FutureResponse<>();
        this.stream.unsubscribe(s, new OnResponse<UnsubscribeResponse>() {
            @Override
            public void response(final UnsubscribeResponse r) {
                response.response(r);
            }

            @Override
            public void error(final Exception e) {
                response.exception(e);
            }
        });
        return response.get();
    }

    /**
     * Closes the stream.<br>
     * This method waits for the stream to be closed before returning.
     *
     * @throws Exception if there is a problem closing the stream
     */
    private void closestream() throws Exception {
        if (this.stream == null) {
            return;
        } else if (this.stream.state() == Stream.STATE.CLOSED || this.stream.state() == Stream.STATE.CLOSING) {
            return;
        }

        final FutureResponse closed = new FutureResponse();
        this.stream.close(new OnClose() {
            @Override
            public void closed() {
                closed.response(null);
            }
        });
        closed.get();
    }

    /**
     * Closes all resources, allowing the application to terminate properly.
     */
    public void close() {
        try {
            this.closestream();
        } catch (final Exception e) {
        }
        this.factory.close();
    }

    /**
     * Utility function throwing exception if response code isn't OK.
     *
     * @param r the response to check
     * @throws Exception if response code isn't OK
     */
    private static void check(final BaseResponse r) throws Exception {
        if (r.getCode() != ResponseCodes.OK_CODE) {
            throw new Exception(String.format(
                    "Request failed. Response %d: %s",
                    r.getCode(),
                    r.getReason()));
        }
    }

    /**
     * Utility function that waits for the user to hit ENTER.
     */
    private static void waitforenter() {
        System.out.print("Hit ENTER to exit.");
        try {
            System.in.read();
        } catch (final IOException e) {
        }
    }

    /**
     * Main method to run example
     */
    public static final void main(final String[] args) {
        final EnduserCredentialExample example = new EnduserCredentialExample();
        try {
            example.run();
        } catch (final Throwable t) {
            t.printStackTrace();
        } finally {
            // Properly release all resources
            example.close();
        }
    }

    /**
     * Utility class for programmatic configuration.
     */
    private static class Cfg {
        public static final String AUTH_SERVICE_HOST = "stream.quotemedia.com";

        public static final int STREAM_OPENTIMEOUT_S = 5;
        public static final String STREAMING_SERVICE_URI = "https://app.quotemedia.com/cache/stream/v1";

        public static AuthCfg AuthCfg() {
            return new AuthCfgBuilder().host(AUTH_SERVICE_HOST).build();
        }

        public static Config Config(final AuthCfg auth) {
            return new ConfigProgrammatic()
                    .auth(auth)
                    .build();
        }

        public static StreamCfg.Auth AuthCredentials(
                final String wmid,
                final String username,
                final String password
        ) {
            return new StreamCfg.AuthCredentials(
                    wmid,
                    username,
                    password
            );
        }

        public static StreamCfg StreamCfg(final StreamCfg.Auth auth) {
            return new StreamCfg.Builder()
                    .auth(auth)
                    .opentimeout_s(STREAM_OPENTIMEOUT_S)
                    // Set format value to MimeTypes.QITCH to use this binary protocol.
                    // Please note that although QITCH protocol uses less bandwidth it can cause performance degradation.
                    // In case if no format was specified QMCI will be used by default.
                    .format(MimeTypes.QMCI)
                    // Set rejectExcessiveConnection(true) to reject new connections when limit is reached. If not specified or value is false,
                    // first open connection will be closed instead.
                    .rejectExcessiveConnection(false)
                    // Set isSynchronousMessageProcessing(true) to process data messages synchronously. Default value is false.
                    .isSynchronousMessageProcessing(false)
                    .transport(Stream.TRANSPORT.WEBSOCKET)
                    //set whether to check server version
                    .checkVersion(true)
                    .uri(STREAMING_SERVICE_URI)
                    .conflation_ms(Stream.CONFLATION.DEFAULT)
                    .build();
        }
    }

    /**
     * Utility class allowing to wait for asynchronous response to succeed or fail.
     */
    private static class FutureResponse<R> {
        private final CountDownLatch done = new CountDownLatch(1);

        volatile private Object response;

        public void response(final R val) {
            this.response = val;
            this.done.countDown();
        }

        public void exception(final Exception val) {
            this.response = val;
            this.done.countDown();
        }

        public R get() throws Exception {
            this.done.await();
            if (this.response instanceof Exception) {
                throw (Exception) this.response;
            }
            return (R) this.response;
        }
    }
}

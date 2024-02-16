import java.io.File;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class OracleDDLExtractor {

    private static String JDBC_URL;
    private static String USER;
    private static String PASSWORD;
    private static String OUTPUT_DIR;

    private static String InitWITHSequence;
    private static String InitMINVALUESequence;
    private static String RemoveCharType;
    private static String IncSEQUENCE;
    private static String IncTABLE;
    private static String IncINDEX;
    private static String IncCONSTRAINT;
    private static String IncTRIGGER;
    private static String IncREF_CONSTRAINT;
    private static String IncTYPE;
    private static String IncVIEW;
    private static String IncFUNCTION;
    private static String IncPROCEDURE;
    private static String IncPACKAGE;
    private static String IncPACKAGE_BODY;

    private static List<PartitionedTableInfo> patitionedTablesInfo;

    public static void main(String[] args) {
        loadConfig();

        Connection connection = null;
        try {
            // Connect to the Oracle database
            connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
            System.out.println(getFormattedDateTime() + "Connected to the Oracle database.");

            // Set session changes
            System.out.println(getFormattedDateTime() + "Setting session changes...");
            setSessionChanges(connection);
            System.out.println(getFormattedDateTime() + "Session changes set.");

            // Generate output directory name
            String outputDirName = generateOutputDirName();
            System.out.println(getFormattedDateTime() + "Output directory name generated: " + outputDirName);

            // Extract DDL for each object type
            if (IncSEQUENCE.equals("Y")) {

                extractObjectDDL(connection, "SEQUENCE", "SEQUENCE", "000_Sequences",
                        outputDirName);

            }

            if (IncTABLE.equals("Y")) {

                patitionedTablesInfo = extractPartitionedTableData(connection);
                extractObjectDDL(connection, "TABLE", "TABLE", "002_Tables", outputDirName);

            }

            if (IncINDEX.equals("Y")) {

                extractObjectDDL(connection, "INDEX", "INDEX", "003_Indexes", outputDirName);

            }

            if (IncCONSTRAINT.equals("Y")) {

                extractObjectDDL(connection, "CONSTRAINT", "CONSTRAINT",
                        "004_Constraints_NonFK",
                        outputDirName);

            }
            if (IncTRIGGER.equals("Y")) {

                extractObjectDDL(connection, "TRIGGER", "TRIGGER", "005_Triggers",
                        outputDirName);

            }
            if (IncREF_CONSTRAINT.equals("Y")) {

                extractObjectDDL(connection, "REF_CONSTRAINT", "REF_CONSTRAINT",
                        "007_Constraints_FK", outputDirName);

            }
            if (IncTYPE.equals("Y")) {

                extractObjectDDL(connection, "TYPE", "TYPE", "009_Types", outputDirName);

            }
            if (IncVIEW.equals("Y")) {

                extractObjectDDL(connection, "VIEW", "VIEW", "010_Views", outputDirName);

            }
            if (IncFUNCTION.equals("Y")) {

                extractObjectDDL(connection, "FUNCTION", "FUNCTION", "011_Functions",
                        outputDirName);

            }
            if (IncPROCEDURE.equals("Y")) {

                extractObjectDDL(connection, "PROCEDURE", "PROCEDURE", "012_Procedures",
                        outputDirName);

            }
            if (IncPACKAGE.equals("Y")) {

                extractObjectDDL(connection, "PACKAGE", "PACKAGE", "013_Packages",
                        outputDirName);

            }
            if (IncPACKAGE_BODY.equals("Y")) {

                extractObjectDDL(connection, "PACKAGE BODY", "PACKAGE_BODY", "013_Packages",
                        outputDirName);

            }

            System.out.println(getFormattedDateTime() + "DDL extraction completed successfully.");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            // Close the database connection
            if (connection != null) {
                try {
                    connection.close();
                    System.out.println(getFormattedDateTime() + "Closed the database connection.");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void setSessionChanges(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "BEGIN " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE); " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PARTITIONING', FALSE); " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE); " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE); " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE); "
                        +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'EMIT_SCHEMA', FALSE); " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE); " +
                        "DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE); "
                        +
                        "END;")) {
            statement.execute();
        }
    }

    // Method to check if a string is one of the specified values
    private static boolean isOneOf(String input, String... values) {
        for (String value : values) {
            if (input.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    private static void extractObjectDDL(Connection connection, String objectType, String ddlType, String outputSubDir,
            String outputDirName)
            throws SQLException, IOException {
        String extension;
        String objectDDL;
        if (objectType.equals("PACKAGE BODY")) {
            extension = ".PBK";
        } else if (objectType.equals("PACKAGE")) {
            extension = ".PSK";
        } else {
            extension = ".sql";
        }
        System.out.println(getFormattedDateTime() + "DDL extraction for " + objectType + " started.");
        String query = "SELECT distinct OBJECT_NAME " +
                "FROM USER_OBJECTS " +
                "WHERE OBJECT_TYPE = ?" +
                " ORDER BY OBJECT_NAME";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, objectType);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String objectName = resultSet.getString("OBJECT_NAME");

                    try {
                        if (isOneOf(objectType, "TABLE")) {
                            objectDDL = getTableDDL(connection, ddlType, objectType, objectName);
                        } else {
                            objectDDL = getObjectDDL(connection, ddlType, objectType, objectName);
                        }
                        if (isOneOf(objectType, "TYPE", "VIEW", "FUNCTION", "PROCEDURE", "PACKAGE", "PACKAGE BODY")) {
                            objectDDL = objectDDL.replace(" EDITIONABLE ", " ");
                        }
                        if (isOneOf(objectType, "SEQUENCE") || InitWITHSequence.equals("Y")) {
                            Pattern pattern = Pattern.compile("(START WITH\\s+)(\\d+)");
                            Matcher matcher = pattern.matcher(objectDDL);
                            objectDDL = matcher.replaceAll("$1" + "1");
                        }

                        if (isOneOf(objectType, "TABLE") || RemoveCharType.equals("Y")) {
                            Pattern pattern = Pattern.compile("( CHAR\\))");
                            Matcher matcher = pattern.matcher(objectDDL);
                            objectDDL = matcher.replaceAll(")");
                        }

                        // Write DDL to file
                        File outputFolder = new File(outputDirName + File.separator + outputSubDir);
                        if (!outputFolder.exists()) {
                            outputFolder.mkdirs();
                        }

                        File outputFile = new File(outputFolder.getPath() + File.separator + objectName + extension);
                        try (FileWriter writer = new FileWriter(outputFile)) {
                            writer.write(objectDDL);
                        }

                    } catch (SQLException e) {
                        int errorCode = e.getErrorCode();
                        // Check for the specific Oracle error codes
                        if (errorCode == 31603 || errorCode == 1002) {
                            // Log the exception and continue the loop
                            System.err.println("Error while processing object " + objectName + ": " + e.getMessage());
                            continue;
                        } else {
                            // Re-throw the exception if it's not the specific error codes you want to
                            // handle
                            throw e;
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("[" + query + "] [" + e.getMessage() + "]");
        } catch (SQLException e1) {
            System.out.println("[" + query + "] [" + e1.getMessage() + "]");
        }
        System.out.println(getFormattedDateTime() + "DDL extraction for " + objectType + " completed.");
    }

    private static void extractObjectDDLParallel(Connection connection, String objectType, String ddlType,
            String outputSubDir,
            String outputDirName) throws SQLException, IOException {

        // Create a pool data source
        PoolDataSource dataSource = PoolDataSourceFactory.getPoolDataSource();
        dataSource.setURL(JDBC_URL);
        dataSource.setUser(USER);
        dataSource.setPassword(PASSWORD);

        // Set other connection pool properties (adjust as needed)
        dataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
        dataSource.setInitialPoolSize(1);
        dataSource.setMinPoolSize(1);
        dataSource.setMaxPoolSize(10);

        String extension;
        if (objectType.equals("PACKAGE BODY")) {
            extension = ".PBK";
        } else if (objectType.equals("PACKAGE")) {
            extension = ".PSK";
        } else {
            extension = ".sql";
        }

        System.out.println(getFormattedDateTime() + "DDL extraction for " + objectType + " started.");

        String query = "SELECT distinct OBJECT_NAME " +
                "FROM USER_OBJECTS " +
                "WHERE OBJECT_TYPE = ?" +
                " ORDER BY OBJECT_NAME";

        try (PreparedStatement statement = connection.prepareStatement(query);
                Connection poolConnection = dataSource.getConnection()) {

            statement.setString(1, objectType);

            try (ResultSet resultSet = statement.executeQuery()) {
                List<Callable<Void>> tasks = new ArrayList<>();

                while (resultSet.next()) {
                    String objectName = resultSet.getString("OBJECT_NAME");

                    tasks.add(() -> {
                        try {
                            // Use the connection from the pool
                            String objectDDL = getObjectDDL(poolConnection, ddlType, objectType, objectName);

                            if (isOneOf(objectType, "TYPE", "VIEW", "FUNCTION", "PROCEDURE", "PACKAGE",
                                    "PACKAGE BODY")) {
                                objectDDL = objectDDL.replace(" EDITIONABLE ", " ");
                            }
                            if (isOneOf(objectType, "SEQUENCE") || InitWITHSequence.equals("Y")) {
                                Pattern pattern = Pattern.compile("(START WITH\\s+)(\\d+)");
                                Matcher matcher = pattern.matcher(objectDDL);
                                objectDDL = matcher.replaceAll("$1" + "1");
                            }

                            if (isOneOf(objectType, "TABLE") || RemoveCharType.equals("Y")) {
                                Pattern pattern = Pattern.compile("( CHAR\\))");
                                Matcher matcher = pattern.matcher(objectDDL);
                                objectDDL = matcher.replaceAll(")");
                            }

                            // Write DDL to file
                            File outputFolder = new File(outputDirName + File.separator + outputSubDir);
                            if (!outputFolder.exists()) {
                                outputFolder.mkdirs();
                            }

                            File outputFile = new File(
                                    outputFolder.getPath() + File.separator + objectName + extension);
                            try (FileWriter writer = new FileWriter(outputFile)) {
                                writer.write(objectDDL);
                            }
                        } catch (SQLException | IOException e) {
                            int errorCode = e instanceof SQLException ? ((SQLException) e).getErrorCode() : 0;
                            if (errorCode == 31603 || errorCode == 1002) {
                                System.err
                                        .println("Error while processing object " + objectName + ": " + e.getMessage());
                            } else {
                                throw new RuntimeException(e);
                            }
                        }
                        return null;
                    });
                }

                // Use Executors.newFixedThreadPool or another executor type
                ExecutorService executorService = Executors.newFixedThreadPool(10);

                try {
                    executorService.invokeAll(tasks);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    executorService.shutdown();
                }
            }
        } catch (SQLException e) {
            System.out.println("[" + query + "] [" + e.getMessage() + "]");
        }

        System.out.println(getFormattedDateTime() + "DDL extraction for " + objectType + " completed.");
    }

    private static String getObjectDDL(Connection connection, String ddlType, String objectType, String objectName)
            throws SQLException {
        String query = "SELECT DBMS_METADATA.GET_DDL(?, ?) AS OBJECT_DDL FROM DUAL";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, ddlType);
            statement.setString(2, objectName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("OBJECT_DDL");
                } else {
                    throw new SQLException("No DDL found for object: " + objectName);
                }
            }
        }
    }

    private static String getTableDDL(Connection connection, String ddlType, String objectType, String objectName)
            throws SQLException {
        String query = "SELECT DBMS_METADATA.GET_DDL(?, ?) AS OBJECT_DDL FROM DUAL";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, ddlType);
            statement.setString(2, objectName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString("OBJECT_DDL");
                } else {
                    throw new SQLException("No DDL found for object: " + objectName);
                }
            }
        }
    }

    public static String getTableDDL(String table, String tableSpace, String PartitionType, String interval,
            String partitionedColumn,
            Connection con)
            throws SQLException {

        String tableScript = "";
        PreparedStatement preparedStatement = null;
        final ArrayList<String> ClumnsName = new ArrayList<String>();
        final ArrayList<String> ClumnsType = new ArrayList<String>();
        final ArrayList<String> ClumnsNullable = new ArrayList<String>();
        final String selectSQL = "SELECT column_name , DECODE(nullable, 'Y', ' ', 'NOT NULL') nullable , DECODE(data_type, 'RAW', data_type || '(' ||  data_length || ')', 'CHAR', data_type || '(' ||  char_length || ' CHAR)', 'VARCHAR',  data_type || '(' ||  char_length || ' CHAR)', 'VARCHAR2', data_type || '(' ||  char_length || ' CHAR)', 'NUMBER', NVL2(   data_precision, DECODE( data_scale, 0, data_type || '(' || data_precision || ')' , data_type || '(' || data_precision || ',' || data_scale || ')'), data_type), data_type ) data_type  FROM user_tab_columns WHERE table_name = '"
                + table + "' ORDER BY column_id";

        try {

            preparedStatement = con.prepareStatement(selectSQL);
            final ResultSet rs = preparedStatement.executeQuery();
            while (rs.next()) {
                ClumnsName.add(rs.getString(1).toLowerCase());
                ClumnsType.add(rs.getString(3));
                ClumnsNullable.add(rs.getString(2));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        try {

            tableScript = "CREATE TABLE " + table + "\n(";
            for (int i = 0; i < ClumnsName.size(); ++i) {
                tableScript = tableScript + String.format("  %-45s %s,\n", ClumnsName.get(i), ClumnsType.get(i));
            }
            tableScript = tableScript
                    + String.format(")\nPCTFREE     10\nINITRANS    20\nMAXTRANS    255\nTABLESPACE " + tableSpace);

            if (PartitionType.equals("RANGE")) {
                tableScript = tableScript + "\n"
                        + "  PARTITION BY RANGE " + "\n"
                        + "  (" + partitionedColumn + ")" + "\n"
                        + "  INTERVAL ( " + interval + " )" + "\n"
                        + "  (" + "\n"
                        + "      PARTITION initial_partition VALUES LESS THAN (TO_DATE('20000101', 'YYYYMMDD'))" + "\n"
                        + "          PCTFREE 10" + "\n"
                        + "          INITRANS 20" + "\n"
                        + "          MAXTRANS 255)" + "\n";
            }

            tableScript = tableScript + "\n/\n";
            return tableScript;
        } catch (SecurityException e3) {
            e3.printStackTrace();
        }
        return tableScript;

    }

    private static void loadConfig() {
        Properties properties = new Properties();
        try (FileReader reader = new FileReader("config.properties")) {
            properties.load(reader);
            JDBC_URL = properties.getProperty("jdbc.url");
            USER = properties.getProperty("jdbc.user");
            PASSWORD = properties.getProperty("jdbc.password");
            OUTPUT_DIR = properties.getProperty("output.dir");
            InitWITHSequence = properties.getProperty("InitWITHSequence");
            InitMINVALUESequence = properties.getProperty("InitMINVALUESequence");
            RemoveCharType = properties.getProperty("RemoveCharType");
            IncSEQUENCE = properties.getProperty("IncSEQUENCE");
            IncTABLE = properties.getProperty("IncTABLE");
            IncINDEX = properties.getProperty("IncINDEX");
            IncCONSTRAINT = properties.getProperty("IncCONSTRAINT");
            IncTRIGGER = properties.getProperty("IncTRIGGER");
            IncREF_CONSTRAINT = properties.getProperty("IncREF_CONSTRAINT");
            IncTYPE = properties.getProperty("IncTYPE");
            IncVIEW = properties.getProperty("IncVIEW");
            IncFUNCTION = properties.getProperty("IncFUNCTION");
            IncPROCEDURE = properties.getProperty("IncPROCEDURE");
            IncPACKAGE = properties.getProperty("IncPACKAGE");
            IncPACKAGE_BODY = properties.getProperty("IncPACKAGE_BODY");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String getFormattedDateTime() {
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return "[" + dateTime.format(formatter) + "] ";
    }

    private static String generateOutputDirName() {
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return OUTPUT_DIR + "_" + USER + "_" + dateTime.format(formatter);
    }

    private static List<PartitionedTableInfo> extractPartitionedTableData(Connection connection) throws SQLException {
        String query = "SELECT A.TABLE_NAME,A.PARTITIONING_TYPE,A.DEF_TABLESPACE_NAME,A.INTERVAL, B.COLUMN_NAME " +
                "FROM USER_PART_TABLES A , USER_PART_KEY_COLUMNS B " +
                "WHERE A.table_name = B.name";

        List<PartitionedTableInfo> partitionedTables = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String tableNameResult = resultSet.getString("TABLE_NAME");
                    String partitioningType = resultSet.getString("PARTITIONING_TYPE");
                    String defTablespaceName = resultSet.getString("DEF_TABLESPACE_NAME");
                    String interval = resultSet.getString("INTERVAL");
                    String column_name = resultSet.getString("COLUMN_NAME");

                    // Create PartitionedTableInfo object and add to the list
                    PartitionedTableInfo tableInfo = new PartitionedTableInfo(tableNameResult, partitioningType,
                            defTablespaceName, interval, column_name);
                    partitionedTables.add(tableInfo);
                }
            }
        }

        return partitionedTables;
    }

    private static class PartitionedTableInfo {
        private String tableName;
        private String partitioningType;
        private String defTablespaceName;
        private String interval;
        private String partitionedColumn;

        public PartitionedTableInfo(String tableName, String partitioningType, String defTablespaceName,
                String interval, String columnName) {
            this.tableName = tableName;
            this.partitioningType = partitioningType;
            this.defTablespaceName = defTablespaceName;
            this.interval = interval;
            this.partitionedColumn = columnName;
        }

        @Override
        public String toString() {
            return "Table Name: " + tableName +
                    ", Partitioning Type: " + partitioningType +
                    ", Default Tablespace: " + defTablespaceName +
                    ", Interval: " + interval +
                    ", partitionedColumn: " + partitionedColumn;

        }
    }
}

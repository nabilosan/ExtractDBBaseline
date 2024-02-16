import java.io.*;
import java.text.SimpleDateFormat;

public class LogTools {
    private static String LOG_FILE;

    static void log(String message) {
        String formattedMessage = getFormattedLogMessage(message);

        // Print to console
        System.out.println(formattedMessage);

        // Write to log file
        writeToFile(formattedMessage, LOG_FILE);
    }

    private static String getFormattedLogMessage(String message) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
        String timestamp = dateFormat.format(new java.util.Date());

        return timestamp + " " + message;
    }

    static void setLogFile(String logFileName) {
        LogTools.LOG_FILE = logFileName;
    }

    private static void writeToFile(String message, String logFileName) {
        try {
            // Check if the log file exists
            File logFile = new File(logFileName);

            // If the log file doesn't exist, create it
            if (!logFile.exists()) {
                if (logFile.createNewFile()) {
                    System.out.println("Log file created: " + logFileName);
                } else {
                    System.out.println("Failed to create log file: " + logFileName);
                    return;
                }
            }

            // Append the log message to the log file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
                writer.write(message);
                writer.newLine();
            } catch (IOException e) {
                System.out.println("Error writing to log file: " + e.getMessage());
            }
        } catch (IOException e) {
            System.out.println("Error checking log file existence: " + e.getMessage());
        }
    }

}

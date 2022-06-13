package com.alibaba.maxgraph.dataload;

import org.apache.commons.cli.*;

import java.io.IOException;

public class LoadTool {

    public static void ingest(String path, boolean isFromOSS) throws IOException {
        new IngestDataCommand(path, isFromOSS).run();
    }

    public static void commit(String path, boolean isFromOSS) throws IOException {
        new CommitDataCommand(path, isFromOSS).run();
    }

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption(
                Option.builder("c")
                        .longOpt("command")
                        .hasArg()
                        .argName("COMMAND")
                        .desc("supported COMMAND: ingest / commit")
                        .build());
        options.addOption(
                Option.builder("d")
                        .longOpt("dir")
                        .hasArg()
                        .argName("HDFS_PATH")
                        .desc("data directory of HDFS. e.g., hdfs://1.2.3.4:9000/build_output")
                        .build());
        options.addOption(
                Option.builder("oss")
                        .longOpt("ossconfigfile")
                        .hasArg()
                        .argName("OSS_CONFIG_FILE")
                        .desc("OSS Config File. e.g., config.init")
                        .build());
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String command = commandLine.getOptionValue("command");
        String path = null;
        boolean isFromOSS = false;
        if (commandLine.hasOption("oss")) {
            isFromOSS = true;
            path = commandLine.getOptionValue("oss");
        } else {
            path = commandLine.getOptionValue("dir");
        }

        if (commandLine.hasOption("help") || command == null) {
            printHelp(options);
        } else if (command.equalsIgnoreCase("ingest")) {
            ingest(path, isFromOSS);
        } else if (command.equalsIgnoreCase("commit")) {
            commit(path, isFromOSS);
        } else {
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("load_tool", options, true);
    }
}

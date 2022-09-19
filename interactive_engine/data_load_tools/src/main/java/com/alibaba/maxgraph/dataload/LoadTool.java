package com.alibaba.maxgraph.dataload;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

public class LoadTool {

    public static void ingest(String configPath, boolean isFromOSS, String uniquePath)
            throws IOException {
        new IngestDataCommand(configPath, isFromOSS, uniquePath).run();
    }

    public static void commit(String configPath, boolean isFromOSS, String uniquePath)
            throws IOException {
        new CommitDataCommand(configPath, isFromOSS, uniquePath).run();
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
        options.addOption(
                Option.builder("u")
                        .longOpt("uniquepath")
                        .hasArg()
                        .argName("UNIQUE_PATH")
                        .desc("unique path from uuid. e.g., unique_path")
                        .build());
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String command = commandLine.getOptionValue("command");
        String configPath = null;
        String uniquePath = null;
        boolean isFromOSS = false;
        if (commandLine.hasOption("oss")) {
            isFromOSS = true;
            configPath = commandLine.getOptionValue("oss");
            uniquePath = commandLine.getOptionValue("u");
        } else {
            configPath = commandLine.getOptionValue("dir");
        }

        if (commandLine.hasOption("help") || command == null) {
            printHelp(options);
        } else if (command.equalsIgnoreCase("ingest")) {
            ingest(configPath, isFromOSS, uniquePath);
        } else if (command.equalsIgnoreCase("commit")) {
            commit(configPath, isFromOSS, uniquePath);
        } else if (command.equalsIgnoreCase("ingestAndCommit")) {
            ingest(configPath, isFromOSS, uniquePath);
            commit(configPath, isFromOSS, uniquePath);
        }
        else {
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("load_tool", options, true);
    }
}

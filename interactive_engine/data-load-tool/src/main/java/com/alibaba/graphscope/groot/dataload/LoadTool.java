package com.alibaba.graphscope.groot.dataload;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

public class LoadTool {

    public static void ingest(String configPath) throws IOException {
        new IngestDataCommand(configPath).run();
    }

    public static void commit(String configPath) throws IOException {
        new CommitDataCommand(configPath).run();
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
                Option.builder("f")
                        .longOpt("config")
                        .hasArg()
                        .argName("CONFIG")
                        .desc("path to configuration file")
                        .build());
        options.addOption(Option.builder("h").longOpt("help").desc("print this message").build());
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String command = commandLine.getOptionValue("command");
        String configPath = commandLine.getOptionValue("config");

        if (commandLine.hasOption("help") || command == null) {
            printHelp(options);
        } else if (command.equalsIgnoreCase("ingest")) {
            ingest(configPath);
        } else if (command.equalsIgnoreCase("commit")) {
            commit(configPath);
        } else if (command.equalsIgnoreCase("ingestAndCommit")) {
            ingest(configPath);
            commit(configPath);
        } else {
            printHelp(options);
        }
    }

    private static void printHelp(Options options) {
        new HelpFormatter().printHelp("load_tool", options, true);
    }
}

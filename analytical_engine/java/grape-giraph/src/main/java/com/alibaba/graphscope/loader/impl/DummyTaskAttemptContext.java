package com.alibaba.graphscope.loader.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;

import java.io.IOException;
import java.net.URI;

public class DummyTaskAttemptContext implements TaskAttemptContext {
    private String status;
    public DummyTaskAttemptContext() {
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
        return null;
    }

    @Override
    public void setStatus(String s) {
        this.status = s;
    }

    @Override
    public String getStatus() {
        return this.status;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public Counter getCounter(Enum<?> anEnum) {
        return null;
    }

    @Override
    public Counter getCounter(String s, String s1) {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public Credentials getCredentials() {
        return null;
    }

    @Override
    public JobID getJobID() {
        return null;
    }

    @Override
    public int getNumReduceTasks() {
        return 0;
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        return null;
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return null;
    }

    @Override
    public Class<?> getOutputValueClass() {
        return null;
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        return null;
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        return null;
    }

    @Override
    public String getJobName() {
        return null;
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        return null;
    }

    @Override
    public RawComparator<?> getSortComparator() {
        return null;
    }

    @Override
    public String getJar() {
        return null;
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
        return null;
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        return null;
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        return false;
    }

    @Override
    public boolean getTaskCleanupNeeded() {
        return false;
    }

    @Override
    public boolean getProfileEnabled() {
        return false;
    }

    @Override
    public String getProfileParams() {
        return null;
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
        return null;
    }

    @Override
    public String getUser() {
        return null;
    }

    /**
     * @deprecated
     */
    @Override
    public boolean getSymlink() {
        return false;
    }

    @Override
    public Path[] getArchiveClassPaths() {
        return new Path[0];
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        return new URI[0];
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        return new URI[0];
    }

    /**
     * @deprecated
     */
    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        return new Path[0];
    }

    /**
     * @deprecated
     */
    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        return new Path[0];
    }

    @Override
    public Path[] getFileClassPaths() {
        return new Path[0];
    }

    @Override
    public String[] getArchiveTimestamps() {
        return new String[0];
    }

    @Override
    public String[] getFileTimestamps() {
        return new String[0];
    }

    @Override
    public int getMaxMapAttempts() {
        return 0;
    }

    @Override
    public int getMaxReduceAttempts() {
        return 0;
    }

    @Override
    public void progress() {

    }
}

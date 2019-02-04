package datawave.mr.bulk;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.common.util.ArgumentChecker;
import datawave.mr.bulk.split.DefaultLocationStrategy;
import datawave.mr.bulk.split.DefaultSplitStrategy;
import datawave.mr.bulk.split.LocationStrategy;
import datawave.mr.bulk.split.RangeSplit;
import datawave.mr.bulk.split.SplitStrategy;

public class BulkInputFormat extends AccumuloInputFormat {
    
    protected static final Logger log = Logger.getLogger(AccumuloInputFormat.class);
    
    protected static final String PREFIX = BulkInputFormat.class.getSimpleName();
    protected static final String INPUT_INFO_HAS_BEEN_SET = PREFIX + ".configured";
    protected static final String INSTANCE_HAS_BEEN_SET = PREFIX + ".instanceConfigured";
    protected static final String USERNAME = PREFIX + ".username";
    protected static final String PASSWORD_PATH = PREFIX + ".password";
    
    protected static final String PASSWORD = PREFIX + ".password.hardcode";
    protected static final String TABLE_NAME = PREFIX + ".tablename";
    protected static final String AUTHORIZATIONS = PREFIX + ".authorizations";
    
    protected static final String INSTANCE_NAME = PREFIX + ".instanceName";
    protected static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
    protected static final String RACKSTRATEGY = PREFIX + ".rack.strategy.class";
    protected static final String RANGESPLITSTRATEGY = PREFIX + ".split.strategy.class";
    protected static final String MOCK = ".useInMemoryInstance";
    
    protected static final String RANGES = PREFIX + ".ranges";
    protected static final String AUTO_ADJUST_RANGES = PREFIX + ".ranges.autoAdjust";
    
    protected static final String ROW_REGEX = PREFIX + ".regex.row";
    protected static final String COLUMN_FAMILY_REGEX = PREFIX + ".regex.cf";
    protected static final String COLUMN_QUALIFIER_REGEX = PREFIX + ".regex.cq";
    
    protected static final String COLUMNS = PREFIX + ".columns";
    protected static final String LOGLEVEL = PREFIX + ".loglevel";
    
    protected static final String ISOLATED = PREFIX + ".isolated";
    
    protected static final String LOCAL_ITERATORS = PREFIX + ".localiters";
    
    // Used to specify the maximum # of versions of an Accumulo cell value to return
    protected static final String MAX_VERSIONS = PREFIX + ".maxVersions";
    
    // Used for specifying the iterators to be applied
    protected static final String ITERATORS = PREFIX + ".iterators";
    protected static final String ITERATORS_OPTIONS = PREFIX + ".iterators.options";
    protected static final String ITERATORS_DELIM = ",";
    
    protected static final String READ_OFFLINE = PREFIX + ".read.offline";
    protected static final String WORKING_DIRECTORY = PREFIX + ".working.dir";
    
    /**
     * Enable or disable use of the {@link IsolatedScanner} in this configuration object. By default it is not enabled.
     * 
     * @param conf
     *            The Hadoop configuration object
     * @param enable
     *            if true, enable usage of the IsolatedScanner. Otherwise, disable.
     */
    public static void setIsolated(Configuration conf, boolean enable) {
        conf.setBoolean(ISOLATED, enable);
    }
    
    public static void setRackStrategy(Configuration conf, Class<? extends LocationStrategy> rackClass) {
        conf.set(RACKSTRATEGY, rackClass.getCanonicalName());
    }
    
    /**
     * Enable or disable use of the {@link ClientSideIteratorScanner} in this Configuration object. By default it is not enabled.
     * 
     * @param conf
     *            The Hadoop configuration object
     * @param enable
     *            if true, enable usage of the ClientSideInteratorScanner. Otherwise, disable.
     */
    public static void setLocalIterators(Configuration conf, boolean enable) {
        conf.setBoolean(LOCAL_ITERATORS, enable);
    }
    
    /**
     * Initialize the user, table, and authorization information for the configuration object that will be used with an Accumulo InputFormat.
     * 
     * @param conf
     *            the Hadoop Configuration object
     * @param user
     *            a valid accumulo user
     * @param passwd
     *            the user's password
     * @param table
     *            the table to read
     * @param auths
     *            the authorizations used to restrict data read
     */
    public static void setMemoryInput(Configuration conf, String user, byte[] passwd, String table, Authorizations auths) {
        
        conf.setBoolean(INPUT_INFO_HAS_BEEN_SET, true);
        
        ArgumentChecker.notNull(user, passwd, table);
        conf.set(USERNAME, user);
        conf.set("accumulo.username", user);
        conf.set(TABLE_NAME, table);
        conf.set(PASSWORD, new String(passwd));
        if (auths != null && !auths.isEmpty())
            conf.set(AUTHORIZATIONS, auths.toString());
    }
    
    /**
     * Initialize the user, table, and authorization information for the configuration object that will be used with an Accumulo InputFormat.
     * 
     * @param job
     *            the Hadoop Job object
     * @param user
     *            a valid accumulo user
     * @param passwd
     *            the user's password
     * @param table
     *            the table to read
     * @param auths
     *            the authorizations used to restrict data read
     */
    public static void setInputInfo(Job job, String user, byte[] passwd, String table, Authorizations auths) {
        Configuration conf = job.getConfiguration();
        if (conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
            throw new IllegalStateException("Input info can only be set once per job");
        conf.setBoolean(INPUT_INFO_HAS_BEEN_SET, true);
        
        ArgumentChecker.notNull(user, passwd, table);
        conf.set(USERNAME, user);
        conf.set("accumulo.username", user);
        conf.set(TABLE_NAME, table);
        if (auths != null && !auths.isEmpty())
            conf.set(AUTHORIZATIONS, auths.toString());
        
        try {
            FileSystem fs = FileSystem.get(conf);
            String workingDirectory = conf.get(WORKING_DIRECTORY, fs.getWorkingDirectory().toString());
            Path work = new Path(workingDirectory);
            Path file = new Path(work, conf.get("mapreduce.job.name") + System.currentTimeMillis() + ".pw");
            conf.set(PASSWORD_PATH, file.toString());
            FSDataOutputStream fos = fs.create(file, false);
            fs.setPermission(file, new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
            fs.deleteOnExit(file);
            
            byte[] encodedPw = Base64.encodeBase64(passwd);
            fos.writeInt(encodedPw.length);
            fos.write(encodedPw);
            fos.close();
            
            conf.set("accumulo.password", new String(encodedPw));
            
            job.addCacheFile(file.toUri());
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        
    }
    
    /**
     * Configure a {@link ZooKeeperInstance} for this configuration object.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @param instanceName
     *            the accumulo instance name
     * @param zooKeepers
     *            a comma-separated list of zookeeper servers
     */
    public static void setZooKeeperInstance(Configuration conf, String instanceName, String zooKeepers) {
        if (conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
            throw new IllegalStateException("Instance info can only be set once per job");
        conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
        
        ArgumentChecker.notNull(instanceName, zooKeepers);
        conf.set(INSTANCE_NAME, instanceName);
        conf.set(ZOOKEEPERS, zooKeepers);
        conf.set("accumulo.instance.name", instanceName);
        conf.set("accumulo.zookeepers", zooKeepers);
    }
    
    /**
     * Configure a {@link InMemoryInstance} for this configuration object.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @param instanceName
     *            the accumulo instance name
     */
    public static void setInMemoryInstance(Configuration conf, String instanceName) {
        conf.setBoolean(INSTANCE_HAS_BEEN_SET, true);
        conf.setBoolean(MOCK, true);
        conf.set(INSTANCE_NAME, instanceName);
    }
    
    /**
     * Set the ranges to map over for this configuration object.
     * 
     * @param job
     *            the Hadoop job
     * @param ranges
     *            the ranges that will be mapped over
     */
    public static void setRanges(Job job, Collection<Range> ranges) {
        ArgumentChecker.notNull(job);
        ArgumentChecker.notNull(ranges);
        
        Configuration conf = job.getConfiguration();
        
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            String workingDirectory = conf.get(WORKING_DIRECTORY, fs.getWorkingDirectory().toString());
            Path work = new Path(workingDirectory);
            Path tempPath = new Path(work, UUID.randomUUID() + ".cache");
            FSDataOutputStream outStream = fs.create(tempPath);
            try {
                
                outStream.writeInt(ranges.size());
                for (Range r : ranges) {
                    r.write(outStream);
                }
                outStream.close();
                job.addCacheFile(tempPath.toUri());
            } catch (IOException ex) {
                throw new IllegalArgumentException("Unable to encode ranges to Base64", ex);
            }
            conf.set(RANGES, tempPath.toString());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    /**
     * Specify the working directory, fs.getWorkingDirectory() doesn't work with ViewFS.
     * 
     * @param conf
     * @param path
     */
    public static void setWorkingDirectory(Configuration conf, String path) {
        conf.set(WORKING_DIRECTORY, path);
    }
    
    /**
     * Disables the adjustment of ranges for this configuration object. By default, overlapping ranges will be merged and ranges will be fit to existing tablet
     * boundaries. Disabling this adjustment will cause there to be exactly one mapper per range set using {@link #setRanges(Job, Collection)}.
     * 
     * @param conf
     *            the Hadoop configuration object
     */
    public static void disableAutoAdjustRanges(Configuration conf) {
        conf.setBoolean(AUTO_ADJUST_RANGES, false);
    }
    
    /**
     * Sets the max # of values that may be returned for an individual Accumulo cell. By default, applied before all other Accumulo iterators (highest priority)
     * leveraged in the scan by the record reader. To adjust priority use setIterator() &amp; setIteratorOptions() w/ the VersioningIterator type explicitly.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @param maxVersions
     *            the max number of versions per accumulo cell
     * @throws IOException
     *             if maxVersions is &lt; 1
     */
    public static void setMaxVersions(Configuration conf, int maxVersions) throws IOException {
        if (maxVersions < 1)
            throw new IOException("Invalid maxVersions: " + maxVersions + ".  Must be >= 1");
        conf.setInt(MAX_VERSIONS, maxVersions);
    }
    
    /**
     * <p>
     * Enable reading offline tables. This will make the map reduce job directly read the tables files. If the table is not offline, then the job will fail. If
     * the table comes online during the map reduce job, its likely that the job will fail.
     * 
     * <p>
     * To use this option, the map reduce user will need access to read the accumulo directory in HDFS.
     * 
     * <p>
     * Reading the offline table will create the scan time iterator stack in the map process. So any iterators that are configured for the table will need to be
     * on the mappers classpath. The accumulo-site.xml may need to be on the mappers classpath if HDFS or the accumlo directory in HDFS are non-standard.
     * 
     * <p>
     * One way to use this feature is to clone a table, take the clone offline, and use the clone as the input table for a map reduce job. If you plan to map
     * reduce over the data many times, it may be better to the compact the table, clone it, take it offline, and use the clone for all map reduce jobs. The
     * reason to do this is that compaction will reduce each tablet in the table to one file, and its faster to read from one file.
     * 
     * <p>
     * There are two possible advantages to reading a tables file directly out of HDFS. First, you may see better read performance. Second, it will support
     * speculative execution better. When reading an online table speculative execution can put more load on an already slow tablet server.
     * 
     * @param conf
     *            the job
     * @param scanOff
     *            pass true to read offline tables
     */
    
    public static void setScanOffline(Configuration conf, boolean scanOff) {
        conf.setBoolean(READ_OFFLINE, scanOff);
    }
    
    /**
     * Restricts the columns that will be mapped over for this configuration object.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @param columnFamilyColumnQualifierPairs
     *            A pair of {@link Text} objects corresponding to column family and column qualifier. If the column qualifier is null, the entire column family
     *            is selected. An empty set is the default and is equivalent to scanning the all columns.
     */
    public static void fetchColumns(Configuration conf, Collection<Pair<Text,Text>> columnFamilyColumnQualifierPairs) {
        ArgumentChecker.notNull(columnFamilyColumnQualifierPairs);
        ArrayList<String> columnStrings = new ArrayList<>(columnFamilyColumnQualifierPairs.size());
        for (Pair<Text,Text> column : columnFamilyColumnQualifierPairs) {
            if (column.getFirst() == null)
                throw new IllegalArgumentException("Column family can not be null");
            
            String col = new String(Base64.encodeBase64(TextUtil.getBytes(column.getFirst())));
            if (column.getSecond() != null)
                col += ":" + new String(Base64.encodeBase64(TextUtil.getBytes(column.getSecond())));
            columnStrings.add(col);
        }
        conf.setStrings(COLUMNS, columnStrings.toArray(new String[columnStrings.size()]));
    }
    
    /**
     * Sets the log level for this configuration object.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @param level
     *            the logging level
     */
    public static void setLogLevel(Configuration conf, Level level) {
        ArgumentChecker.notNull(level);
        log.setLevel(level);
        conf.setInt(LOGLEVEL, level.toInt());
    }
    
    /**
     * Encode an iterator on the input for this configuration object.
     * 
     * @param conf
     *            The Hadoop configuration in which to save the iterator configuration
     * @param cfg
     *            The configuration of the iterator
     */
    public static void addIterator(Configuration conf, IteratorSetting cfg) {
        // First check to see if anything has been set already
        String iterators = conf.get(ITERATORS);
        
        // No iterators specified yet, create a new string
        if (iterators == null || iterators.isEmpty()) {
            iterators = new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()).toString();
        } else {
            // append the next iterator & reset
            iterators = iterators.concat(ITERATORS_DELIM + new AccumuloIterator(cfg.getPriority(), cfg.getIteratorClass(), cfg.getName()));
        }
        // Store the iterators w/ the job
        conf.set(ITERATORS, iterators);
        for (Entry<String,String> entry : cfg.getOptions().entrySet()) {
            if (entry.getValue() == null)
                continue;
            
            String iteratorOptions = conf.get(ITERATORS_OPTIONS);
            
            // No options specified yet, create a new string
            if (iteratorOptions == null || iteratorOptions.isEmpty()) {
                iteratorOptions = new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()).toString();
            } else {
                // append the next option & reset
                iteratorOptions = iteratorOptions.concat(ITERATORS_DELIM + new AccumuloIteratorOption(cfg.getName(), entry.getKey(), entry.getValue()));
            }
            
            // Store the options w/ the job
            conf.set(ITERATORS_OPTIONS, iteratorOptions);
        }
    }
    
    /**
     * Determines whether a configuration has isolation enabled.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return true if isolation is enabled, false otherwise
     * @see #setIsolated(Configuration, boolean)
     */
    protected static boolean isIsolated(Configuration conf) {
        return conf.getBoolean(ISOLATED, false);
    }
    
    /**
     * Determines whether a configuration uses local iterators.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return true if uses local iterators, false otherwise
     * @see #setLocalIterators(Configuration, boolean)
     */
    protected static boolean usesLocalIterators(Configuration conf) {
        return conf.getBoolean(LOCAL_ITERATORS, false);
    }
    
    /**
     * Gets the table name from the configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return the table name
     * @see #setInputInfo(Job, String, byte[], String, Authorizations)
     */
    protected static String getTablename(Configuration conf) {
        return conf.get(TABLE_NAME);
    }
    
    /**
     * Gets the authorizations to set for the scans from the configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return the accumulo scan authorizations
     * @see #setInputInfo(Job, String, byte[], String, Authorizations)
     */
    protected static Authorizations getAuthorizations(Configuration conf) {
        String authString = conf.get(AUTHORIZATIONS);
        return authString == null ? Authorizations.EMPTY : new Authorizations(authString.split(","));
    }
    
    /**
     * Initializes an Accumulo {@link Instance} based on the configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return an accumulo instance
     * @see #setZooKeeperInstance(Configuration, String, String)
     * @see #setInMemoryInstance(Configuration, String)
     */
    protected static Instance getInstance(Configuration conf) {
        if (conf.getBoolean(MOCK, false))
            return new InMemoryInstance(conf.get(INSTANCE_NAME));
        return new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(conf.get(INSTANCE_NAME)).withZkHosts(conf.get(ZOOKEEPERS)));
    }
    
    /**
     * Gets the ranges to scan over from a configuration object.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return the ranges
     * @throws IOException
     *             if the ranges have been encoded improperly
     * @see #setRanges(Job, Collection)
     */
    protected static List<Range> getRanges(Configuration conf) throws IOException {
        String rangesPath = conf.get(RANGES);
        FileSystem fs = FileSystem.get(conf);
        
        FSDataInputStream inputStream = fs.open(new Path(rangesPath));
        int size = inputStream.readInt();
        ArrayList<Range> ranges = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Range range = new Range();
            range.readFields(inputStream);
            ranges.add(range);
        }
        inputStream.close();
        
        return ranges;
    }
    
    /**
     * Gets the columns to be mapped over from this configuration object.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return a set of columns
     * @see #fetchColumns(Configuration, Collection)
     */
    protected static Set<Pair<Text,Text>> getFetchedColumns(Configuration conf) {
        Set<Pair<Text,Text>> columns = new HashSet<>();
        for (String col : conf.getStringCollection(COLUMNS)) {
            int idx = col.indexOf(":");
            Text cf = new Text(idx < 0 ? Base64.decodeBase64(col.getBytes()) : Base64.decodeBase64(col.substring(0, idx).getBytes()));
            Text cq = idx < 0 ? null : new Text(Base64.decodeBase64(col.substring(idx + 1).getBytes()));
            columns.add(new Pair<>(cf, cq));
        }
        return columns;
    }
    
    /**
     * Determines whether a configuration has auto-adjust ranges enabled.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return true if auto-adjust is enabled, false otherwise
     * @see #disableAutoAdjustRanges(Configuration)
     */
    protected static boolean getAutoAdjustRanges(Configuration conf) {
        return conf.getBoolean(AUTO_ADJUST_RANGES, true);
    }
    
    /**
     * Gets the log level from this configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return the log level
     * @see #setLogLevel(Configuration, Level)
     */
    protected static Level getLogLevel(Configuration conf) {
        return Level.toLevel(conf.getInt(LOGLEVEL, Level.INFO.toInt()));
    }
    
    // InputFormat doesn't have the equivalent of OutputFormat's
    // checkOutputSpecs(JobContext job)
    /**
     * Check whether a configuration is fully configured to be used with an Accumulo {@link org.apache.hadoop.mapreduce.InputFormat}.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @throws IOException
     *             if the configuration is improperly configured
     */
    protected static void validateOptions(Configuration conf) throws IOException {
        if (!conf.getBoolean(INPUT_INFO_HAS_BEEN_SET, false))
            throw new IOException("Input info has not been set.");
        if (!conf.getBoolean(INSTANCE_HAS_BEEN_SET, false))
            throw new IOException("Instance info has not been set.");
        
        /*
         * if (conf.get(RACKSTRATEGY) == null) { throw new IOException("Rack strategy must be set."); }
         */
        // validate that we can connect as configured
        try {
            Connector c = getInstance(conf).getConnector(getUsername(conf), new PasswordToken(getPassword(conf)));
            if (!c.securityOperations().authenticateUser(getUsername(conf), new PasswordToken(getPassword(conf))))
                throw new IOException("Unable to authenticate user");
            if (!c.securityOperations().hasTablePermission(getUsername(conf), getTablename(conf), TablePermission.READ))
                throw new IOException("Unable to access table");
            
            if (!usesLocalIterators(conf)) {
                // validate that any scan-time iterators can be loaded by the the tablet servers
                for (AccumuloIterator iter : getIterators(conf)) {
                    if (!c.tableOperations().testClassLoad(getTablename(conf), iter.getIteratorClass(), SortedKeyValueIterator.class.getName())
                                    && !c.instanceOperations().testClassLoad(iter.getIteratorClass(), SortedKeyValueIterator.class.getName()))
                        throw new AccumuloException("Servers are unable to load " + iter.getIteratorClass() + " as a " + SortedKeyValueIterator.class.getName());
                }
            }
            
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new IOException(e);
        }
    }
    
    /**
     * Gets the user name from the configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return the user name
     * @see #setInputInfo(Job, String, byte[], String, Authorizations)
     */
    protected static String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }
    
    /**
     * @param conf
     *            the Hadoop configuration object
     * @return the BASE64-encoded password
     * @throws IOException
     * @see #setInputInfo(Job, String, byte[], String, Authorizations)
     */
    protected static byte[] getPassword(Configuration conf) throws IOException {
        if (null != conf.get(PASSWORD_PATH)) {
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path(conf.get(PASSWORD_PATH));
            
            FSDataInputStream fdis = fs.open(file);
            int length = fdis.readInt();
            byte[] encodedPassword = new byte[length];
            fdis.read(encodedPassword);
            fdis.close();
            
            return Base64.decodeBase64(encodedPassword);
        } else {
            String passwd = conf.get(PASSWORD);
            if (null != passwd)
                return passwd.getBytes();
            else
                return null;
        }
    }
    
    /**
     * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return a list of iterators
     * @see #addIterator(Configuration, IteratorSetting)
     */
    protected static List<AccumuloIterator> getIterators(Configuration conf) {
        
        String iterators = conf.get(ITERATORS);
        
        // If no iterators are present, return an empty list
        if (iterators == null || iterators.isEmpty())
            return new ArrayList<>();
        
        // Compose the set of iterators encoded in the job configuration
        StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS), ITERATORS_DELIM);
        List<AccumuloIterator> list = new ArrayList<>();
        while (tokens.hasMoreTokens()) {
            String itstring = tokens.nextToken();
            list.add(new AccumuloIterator(itstring));
        }
        return list;
    }
    
    /**
     * Clears the list of the iterator settings (for iterators to apply to a scanner) from this configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @see #addIterator(Configuration, IteratorSetting)
     */
    public static void clearIterators(Configuration conf) {
        
        conf.set(ITERATORS, "");
        conf.set(ITERATORS_OPTIONS, "");
        
    }
    
    public static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf) {
        String iteratorOptions = conf.get(ITERATORS_OPTIONS);
        
        // If no options are present, return an empty list
        if (iteratorOptions == null || iteratorOptions.isEmpty())
            return new ArrayList<>();
        
        // Compose the set of options encoded in the job configuration
        StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS_OPTIONS), ITERATORS_DELIM);
        List<AccumuloIteratorOption> list = new ArrayList<>();
        while (tokens.hasMoreTokens()) {
            String optionString = tokens.nextToken();
            list.add(new AccumuloIteratorOption(optionString));
        }
        return list;
    }
    
    /**
     * Gets the maxVersions to use for the {@link VersioningIterator} from this configuration.
     * 
     * @param conf
     *            the Hadoop configuration object
     * @return the max versions, -1 if not configured
     * @see #setMaxVersions(Configuration, int)
     */
    protected static int getMaxVersions(Configuration conf) {
        return conf.getInt(MAX_VERSIONS, -1);
    }
    
    protected static boolean isOfflineScan(Configuration conf) {
        return conf.getBoolean(READ_OFFLINE, false);
    }
    
    protected abstract static class RecordReaderBase<K,V> extends RecordReader<K,V> {
        protected long numKeysRead;
        protected Iterator<Entry<Key,Value>> scannerIterator;
        protected RangeSplit split;
        private BatchScanner scanner = null;
        protected float progress = 0.0f;
        
        /**
         * Gets a list of the iterator options specified on this configuration.
         * 
         * @param conf
         *            the Hadoop configuration object
         * @return a list of iterator options
         * @see #addIterator(Configuration, IteratorSetting)
         */
        public static List<AccumuloIteratorOption> getIteratorOptions(Configuration conf) {
            String iteratorOptions = conf.get(ITERATORS_OPTIONS);
            
            // If no options are present, return an empty list
            if (iteratorOptions == null || iteratorOptions.isEmpty())
                return new ArrayList<>();
            
            // Compose the set of options encoded in the job configuration
            StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS_OPTIONS), ITERATORS_DELIM);
            List<AccumuloIteratorOption> list = new ArrayList<>();
            while (tokens.hasMoreTokens()) {
                String optionString = tokens.nextToken();
                list.add(new AccumuloIteratorOption(optionString));
            }
            return list;
        }
        
        /**
         * Apply the configured iterators from the configuration to the scanner.
         * 
         * @param conf
         *            the Hadoop configuration object
         * @param scanner
         *            the scanner to configure
         * @throws AccumuloException
         */
        protected void setupIterators(Configuration conf, BatchScanner scanner) throws AccumuloException {
            List<AccumuloIterator> iterators = getIterators(conf);
            List<AccumuloIteratorOption> options = getIteratorOptions(conf);
            
            Map<String,IteratorSetting> scanIterators = new HashMap<>();
            for (AccumuloIterator iterator : iterators) {
                scanIterators.put(iterator.getIteratorName(),
                                new IteratorSetting(iterator.getPriority(), iterator.getIteratorName(), iterator.getIteratorClass()));
            }
            for (AccumuloIteratorOption option : options) {
                scanIterators.get(option.iteratorName).addOption(option.getKey(), option.getValue());
            }
            for (AccumuloIterator iterator : iterators) {
                scanner.addScanIterator(scanIterators.get(iterator.getIteratorName()));
            }
        }
        
        /**
         * Gets a list of the iterator settings (for iterators to apply to a scanner) from this configuration.
         * 
         * @param conf
         *            the Hadoop configuration object
         * @return a list of iterators
         * @see #addIterator(Configuration, IteratorSetting)
         */
        protected static List<AccumuloIterator> getIterators(Configuration conf) {
            
            String iterators = conf.get(ITERATORS);
            
            // If no iterators are present, return an empty list
            if (iterators == null || iterators.isEmpty())
                return new ArrayList<>();
            
            // Compose the set of iterators encoded in the job configuration
            StringTokenizer tokens = new StringTokenizer(conf.get(ITERATORS), ITERATORS_DELIM);
            List<AccumuloIterator> list = new ArrayList<>();
            while (tokens.hasMoreTokens()) {
                String itstring = tokens.nextToken();
                list.add(new AccumuloIterator(itstring));
            }
            return list;
        }
        
        /**
         * If maxVersions has been set, configure a {@link VersioningIterator} at priority 0 for this scanner.
         * 
         * @param conf
         *            the Hadoop configuration object
         * @param scanner
         *            the scanner to configure
         */
        protected void setupMaxVersions(Configuration conf, BatchScanner scanner) {
            int maxVersions = getMaxVersions(conf);
            // Check to make sure its a legit value
            if (maxVersions >= 1) {
                IteratorSetting vers = new IteratorSetting(0, "vers", VersioningIterator.class);
                VersioningIterator.setMaxVersions(vers, maxVersions);
                scanner.addScanIterator(vers);
            }
        }
        
        /**
         * Initialize a scanner over the given input split using this task attempt configuration.
         */
        public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
            if (null != scanner) {
                scanner.close();
            }
            split = (RangeSplit) inSplit;
            if (log.isDebugEnabled())
                log.debug("Initializing input split: " + split.getRanges());
            Configuration conf = attempt.getConfiguration();
            Instance instance = getInstance(conf);
            String user = getUsername(conf);
            byte[] password = getPassword(conf);
            Authorizations authorizations = getAuthorizations(conf);
            
            try {
                log.debug("Creating connector with user: " + user);
                Connector conn = instance.getConnector(user, new PasswordToken(password));
                log.debug("Creating scanner for table: " + getTablename(conf));
                log.debug("Authorizations are: " + authorizations);
                
                scanner = conn.createBatchScanner(getTablename(conf), authorizations, 2);
                
                setupMaxVersions(conf, scanner);
                IteratorSetting is = new IteratorSetting(50, RegExFilter.class);
                RegExFilter.setRegexs(is, conf.get(ROW_REGEX), conf.get(COLUMN_FAMILY_REGEX), conf.get(COLUMN_QUALIFIER_REGEX), null, false);
                scanner.addScanIterator(is);
                setupIterators(conf, scanner);
            } catch (Exception e) {
                throw new IOException(e);
            }
            
            // setup a scanner within the bounds of this split
            for (Pair<Text,Text> c : getFetchedColumns(conf)) {
                if (c.getSecond() != null) {
                    log.debug("Fetching column " + c.getFirst() + ":" + c.getSecond());
                    scanner.fetchColumn(c.getFirst(), c.getSecond());
                } else {
                    log.debug("Fetching column family " + c.getFirst());
                    scanner.fetchColumnFamily(c.getFirst());
                }
            }
            
            scanner.setRanges(split.getRanges());
            
            numKeysRead = -1;
            
            // do this last after setting all scanner options
            scannerIterator = scanner.iterator();
        }
        
        public void close() {
            scanner.close();
        }
        
        @Override
        public float getProgress() throws IOException {
            if (numKeysRead < 0) {
                progress = 0.0f;
            } else if (currentKey == null) {
                progress = 1.0f;
            } else {
                float newProgress = split.getProgress(currentKey);
                if (newProgress > progress) {
                    progress = newProgress;
                }
            }
            return progress;
        }
        
        protected K currentK = null;
        protected V currentV = null;
        protected Key currentKey = null;
        protected Value currentValue = null;
        
        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return currentK;
        }
        
        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return currentV;
        }
    }
    
    protected static SplitStrategy getSplitStrategy(Configuration conf) {
        
        try {
            Class<? extends SplitStrategy> clazz = Class.forName(conf.get(RANGESPLITSTRATEGY, DefaultSplitStrategy.class.getCanonicalName())).asSubclass(
                            SplitStrategy.class);
            return clazz.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            log.error(e);
        }
        return new DefaultSplitStrategy();
    }
    
    protected static LocationStrategy getLocationStrategy(Configuration conf) {
        try {
            Class<? extends LocationStrategy> clazz = Class.forName(conf.get(RACKSTRATEGY, DefaultLocationStrategy.class.getCanonicalName())).asSubclass(
                            LocationStrategy.class);
            return clazz.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            log.error(e);
        }
        return new DefaultLocationStrategy();
    }
    
    /**
     * The Class IteratorSetting. Encapsulates specifics for an Accumulo iterator's name &amp; priority.
     */
    static class AccumuloIterator {
        
        private static final String FIELD_SEP = ":";
        
        private int priority;
        private String iteratorClass;
        private String iteratorName;
        
        public AccumuloIterator(int priority, String iteratorClass, String iteratorName) {
            this.priority = priority;
            this.iteratorClass = iteratorClass;
            this.iteratorName = iteratorName;
        }
        
        // Parses out a setting given an string supplied from an earlier toString() call
        public AccumuloIterator(String iteratorSetting) {
            // Parse the string to expand the iterator
            StringTokenizer tokenizer = new StringTokenizer(iteratorSetting, FIELD_SEP);
            priority = Integer.parseInt(tokenizer.nextToken());
            iteratorClass = tokenizer.nextToken();
            iteratorName = tokenizer.nextToken();
        }
        
        public int getPriority() {
            return priority;
        }
        
        public String getIteratorClass() {
            return iteratorClass;
        }
        
        public String getIteratorName() {
            return iteratorName;
        }
        
        @Override
        public String toString() {
            return priority + FIELD_SEP + iteratorClass + FIELD_SEP + iteratorName;
        }
        
    }
    
    /**
     * The Class AccumuloIteratorOption. Encapsulates specifics for an Accumulo iterator's optional configuration details - associated via the iteratorName.
     */
    static class AccumuloIteratorOption {
        private static final String FIELD_SEP = ":";
        
        private String iteratorName;
        private String key;
        private String value;
        
        public AccumuloIteratorOption(String iteratorName, String key, String value) {
            this.iteratorName = iteratorName;
            this.key = key;
            this.value = value;
        }
        
        // Parses out an option given a string supplied from an earlier toString() call
        public AccumuloIteratorOption(String iteratorOption) {
            StringTokenizer tokenizer = new StringTokenizer(iteratorOption, FIELD_SEP);
            this.iteratorName = tokenizer.nextToken();
            try {
                this.key = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
                this.value = URLDecoder.decode(tokenizer.nextToken(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        
        public String getIteratorName() {
            return iteratorName;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getValue() {
            return value;
        }
        
        @Override
        public String toString() {
            try {
                return iteratorName + FIELD_SEP + URLEncoder.encode(key, "UTF-8") + FIELD_SEP + URLEncoder.encode(value, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) {
        
        return new RecordReaderBase<Key,Value>() {
            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (scannerIterator.hasNext()) {
                    ++numKeysRead;
                    Entry<Key,Value> entry = scannerIterator.next();
                    currentK = currentKey = entry.getKey();
                    currentV = currentValue = entry.getValue();
                    if (log.isTraceEnabled())
                        log.trace("Processing key/value pair: " + DefaultFormatter.formatEntry(entry, true));
                    return true;
                } else if (numKeysRead < 0) {
                    numKeysRead = 0;
                }
                return false;
            }
        };
    }
    
}

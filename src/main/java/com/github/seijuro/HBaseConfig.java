package com.github.seijuro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;


/**
 * Created by seijuro
 */
public class HBaseConfig {
    static final int WELKNOWN_PORT_MAX = 1024;
    static final int PORT_MAX = 65536;

    public static class Property {
        public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
        public static final String ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
        public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.znode.parent";

        public static final String CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";
    }

    /**
     * Builder pattern class
     */
    public static class Builder {
        /**
         * laoding configuration from file at filepath.
         *
         * @param filepath
         * @return
         * @throws FileNotFoundException
         * @throws IOException
         */
        public static HBaseConfig from(String filepath) throws IOException {
            FileInputStream fis = new FileInputStream(filepath);
            Properties prop = new Properties();

            prop.load(fis);

            fis.close();

            return new HBaseConfig(prop);
        }

        /**
         * Instance Properties
         */
        private Properties props;

        /**
         * C'tor
         */
        public Builder() {
            this.props = new Properties();
        }

        public Builder setProperty(String property, String value) {
            assert (value != null && value.length() > 0);

            this.props.setProperty(property, value);
            return this;
        }

        public Builder setQuorum(String quorum) {
            this.props.put(Property.ZOOKEEPER_QUORUM, quorum);
            return this;
        }

        public Builder setClientPort(int port) {
            assert (port > WELKNOWN_PORT_MAX && port <= PORT_MAX);

            this.props.setProperty(Property.ZOOKEEPER_CLIENTPORT, Integer.toString(port));
            return this;
        }

        public Builder setZNodeParent(String parent) {
            assert (parent != null && parent.length() > 0);

            this.props.setProperty(Property.ZOOKEEPER_ZNODE_PARENT, parent);
            return this;
        }

        public Builder setDistributedCluster(boolean flag) {
            this.props.setProperty(Property.CLUSTER_DISTRIBUTED, Boolean.toString(flag));
            return this;
        }

        /**
         * Builder pattern method
         *
         * @return
         */
        public HBaseConfig build() {
            return new HBaseConfig(this);
        }
    }

    /**
     * Instance Properties
     */
    private final Properties props;

    /**
     * C'tor
     *
     * @param properties
     */
    HBaseConfig(Properties properties) {
        this.props = properties;
    }

    /**
     * C'tor
     * @param builder
     */
    HBaseConfig(Builder builder) {
        this.props = builder.props;
    }

    public String getProperty(String property) {
        assert (property != null);

        return this.props.getProperty(property);
    }

    public String getZKQuorum() {
        return this.props.getProperty(Property.ZOOKEEPER_QUORUM);
    }

    public int getClientPort() throws NullPointerException, NumberFormatException {
        String port = this.props.getProperty(Property.ZOOKEEPER_CLIENTPORT);

        if (port == null) {
            throw new NullPointerException(String.format("property, %s, isn't set.", Property.ZOOKEEPER_CLIENTPORT));
        }

        return Integer.parseInt(port);
    }

    public boolean isClusterDisributed() {
        String flag = this.props.getProperty(Property.CLUSTER_DISTRIBUTED);

        if (flag == null) {
            throw new NullPointerException(String.format("Property, %s, isn't set", Property.CLUSTER_DISTRIBUTED));
        }

        return Boolean.parseBoolean(flag);
    }

    public Configuration create() {
        Configuration configuration = HBaseConfiguration.create();
        for (Map.Entry<Object, Object> entry : this.props.entrySet()) {
            configuration.set(String.class.cast(entry.getKey()), String.class.cast(entry.getValue()));
        }

        return configuration;
    }
}
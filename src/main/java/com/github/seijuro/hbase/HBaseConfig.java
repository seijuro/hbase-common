package com.github.seijuro.hbase;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;


/**
 * Created by seijuro
 */
public class HBaseConfig {
    /**
     * Class Properties
     */
    private static final Logger LOG = LoggerFactory.getLogger(HBaseConfig.class);

    private static final int WELKNOWN_PORT_MAX = 1024;
    private static final int PORT_MAX = 65536;

    /**
     * designed for hbase configuration properties.
     */
    public enum Property {
        ZOOKEEPER_QUORUM("hbase.zookeeper.quorum"),
        ZOOKEEPER_CLIENTPORT("hbase.zookeeper.property.clientPort"),
        ZOOKEEPER_ZNODE_PARENT("zookeeper.znode.parent"),
        CLUSTER_DISTRIBUTED("hbase.cluster.distributed");

        /**
         * Instance properties
         */
        @Getter
        private final String property;

        /**
         * C'tor
         *
         * @param $property
         */
        Property(String $property) {
            this.property = $property;
        }
    }

    /**
     * Instance Properties
     */
    private final Properties props;

    /**
     * C'tor
     * @param builder
     */
    HBaseConfig(Builder builder) {
        this.props = builder.props;
    }

    /**
     * get value for the given property whose key is equal to.
     *
     * @param property
     * @return
     */
    public Object getProperty(Property property) {
        if (Objects.nonNull(property)) {
            return this.props.getProperty(property.getProperty());
        }

        return null;
    }

    public String getZKQuorum() {
        return (String)getProperty(Property.ZOOKEEPER_QUORUM);
    }

    public Integer getClientPort() {
        return (Integer)getProperty(Property.ZOOKEEPER_CLIENTPORT);
    }

    public Boolean isClusterDisributed() {
        return (Boolean)getProperty(Property.CLUSTER_DISTRIBUTED);
    }

    public Configuration create() {
        Configuration configuration = HBaseConfiguration.create();
        for (Map.Entry<Object, Object> entry : this.props.entrySet()) {
            configuration.set(String.class.cast(entry.getKey()), entry.getValue().toString());
        }

        return configuration;
    }

    /**
     * Builder pattern class
     */
    public static class Builder {
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

        public Builder setProperty(Property property, String value) {
            assert Objects.nonNull(value);

            if (Objects.isNull(value)) {
                this.props.remove(property.getProperty());
            }
            else {
                this.props.setProperty(property.getProperty(), value);
            }

            return this;
        }

        /**
         * set property, quorum.
         *
         * @param quorum
         * @return
         */
        public Builder setQuorum(String quorum) {
            return setProperty(Property.ZOOKEEPER_QUORUM, quorum);
        }

        /**
         * set property, client port.
         *
         * @param port
         * @return
         */
        public Builder setClientPort(int port) throws IllegalArgumentException {
            //  checking argument(s)
            if (port <= WELKNOWN_PORT_MAX ||
                    port > PORT_MAX) {
                String msg = String.format("Parameter, port (%d), must be a value within a range (%d, %d].", port, WELKNOWN_PORT_MAX, PORT_MAX);

                LOG.warn("Checking argument failed ... (reason : {})", msg);

                throw new IllegalArgumentException(msg);
            }

            return setProperty(Property.ZOOKEEPER_CLIENTPORT, Integer.toString(port));
        }

        /**
         * set property, znode parent.
         *
         * @param parent
         * @return
         */
        public Builder setZNodeParent(String parent) {
            if (StringUtils.isEmpty(parent)) {
                return setProperty(Property.ZOOKEEPER_ZNODE_PARENT, null);
            }

            return setProperty(Property.ZOOKEEPER_ZNODE_PARENT, parent);
        }

        /**
         * set property, distributed cluster.
         *
         * @param flag
         * @return
         */
        public Builder setDistributedCluster(boolean flag) {
            return setProperty(Property.CLUSTER_DISTRIBUTED, Boolean.toString(flag));
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
}
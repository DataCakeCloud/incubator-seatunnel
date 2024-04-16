package org.apache.seatunnel.connectors.seatunnel.file.hdfs.serialization;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SerializableConfiguration implements Serializable {

    private transient Configuration hadoopConf;

    public SerializableConfiguration(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        hadoopConf.write(out);
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        hadoopConf = new Configuration(false);
        hadoopConf.readFields(in);
    }

    public Configuration get() {
        return hadoopConf;
    }
}
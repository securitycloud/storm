
package cz.muni.fi.storm.spouts;



import java.io.Serializable;

public class KafkaMessageId implements Comparable<KafkaMessageId>, Serializable {
    private final int _partition;
    private final long _offset;

    public KafkaMessageId(int partition, long offset) {
        _partition = partition;
        _offset = offset;
    }


    public int getPartition() {
        return _partition;
    }

    public long getOffset() {
        return _offset;
    }

    
    @Override
    public boolean equals(final Object o) {
        if (o instanceof KafkaMessageId) {
            final KafkaMessageId other = (KafkaMessageId) o;
            return other.getPartition() == _partition && other.getOffset() == _offset;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // create a hash code using all bits of both identifying members
        return (31 + _partition) * (int) (_offset ^ (_offset >>> 32));
    }

    
    @Override
    public int compareTo(final KafkaMessageId id) {
        // instance is always > null
        if (id == null) {
            return 1;
        }
        // use signum to perform the comparison, mark _partition more significant than _offset
        return 2 * Integer.signum(_partition - id.getPartition()) + Long.signum(_offset - id.getOffset());
    }

    @Override
    public String toString() {
        return "(" + _partition + "," + _offset + ")";
    }
}
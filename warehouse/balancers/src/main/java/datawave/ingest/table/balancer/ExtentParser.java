package datawave.ingest.table.balancer;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;

public interface ExtentParser {
    String getDate(KeyExtent extent);
    
    String getDate(TKeyExtent extent);
}

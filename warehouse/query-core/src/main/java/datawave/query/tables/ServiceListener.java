package datawave.query.tables;

public interface ServiceListener {
    
    default void starting() {}
    
    void stopping();
    
    void failed(String from, Throwable failure);
}

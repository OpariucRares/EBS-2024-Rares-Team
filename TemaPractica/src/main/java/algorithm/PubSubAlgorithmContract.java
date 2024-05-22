package algorithm;

public interface PubSubAlgorithmContract {
    void init();
    void generateSubscriptions();
    void generatePublications();
    void createPubAndSubFiles();
}

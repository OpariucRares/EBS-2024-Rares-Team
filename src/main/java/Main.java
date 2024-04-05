import algorithm.PubSubAlgorithm;

public class Main {
    public static void main(String[] args) {
        //creating an object with default values
        PubSubAlgorithm pubSubAlgorithm = new PubSubAlgorithm();

        //reading from the input file and getting the values
        pubSubAlgorithm.init();
        pubSubAlgorithm.printInputValues();

        //algorithm of generating
        pubSubAlgorithm.generatePublications();
        pubSubAlgorithm.generateSubscriptions();

        //the result written in files
        pubSubAlgorithm.createPubAndSubFiles();
    }

}
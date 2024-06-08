package workers.publish;

import models.PublicationDEPRECATED;

import java.util.ArrayList;
import java.util.List;

public class PublisherGeneratorThread extends Thread {
    private final int noOfPublishers;
    private List<PublicationDEPRECATED> publicationDEPRECATEDS;
    public PublisherGeneratorThread(int noOfPublishers){
        this.noOfPublishers = noOfPublishers;
        publicationDEPRECATEDS = new ArrayList<>();
    }
    public List<PublicationDEPRECATED> getPublications(){
        return publicationDEPRECATEDS;
    }
    @Override
    public void run() {
        for(int i = 0; i < noOfPublishers; i++){
            PublicationDEPRECATED pub = new PublicationDEPRECATED();
            publicationDEPRECATEDS.add(pub);
        }
    }
}
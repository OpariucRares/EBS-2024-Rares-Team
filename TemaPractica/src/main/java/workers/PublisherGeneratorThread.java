package workers;

import models.Publication;
import models.Subscription;

import java.util.ArrayList;
import java.util.List;

public class PublisherGeneratorThread extends Thread {
    private final int noOfPublishers;
    private List<Publication> publications;
    public PublisherGeneratorThread(int noOfPublishers){
        this.noOfPublishers = noOfPublishers;
        publications = new ArrayList<>();
    }
    public List<Publication> getPublications(){
        return publications;
    }
    @Override
    public void run() {
        for(int i = 0; i < noOfPublishers; i++){
            Publication pub = new Publication();
            publications.add(pub);
        }
    }
}

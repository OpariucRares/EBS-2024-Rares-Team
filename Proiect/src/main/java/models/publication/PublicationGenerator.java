package models.publication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import models.publication.PublicationOuterClass.Publication;

public class PublicationGenerator {
    public List<Publication> generatePublications(int count, Map<String, Double> fieldFreq) {

        List<Publication.Builder> publicationBuilders = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            publicationBuilders.add(Publication.newBuilder());
        }

        for (Map.Entry<String, Double> entry : fieldFreq.entrySet()) {
            String fieldName = entry.getKey();
            Double freq = entry.getValue();
            int fieldCount = (int) (count * freq);

            new PublicationsWorker(publicationBuilders, fieldName, fieldCount).run();
        }

        List<Publication> publications = new ArrayList<>();
        for (Publication.Builder builder : publicationBuilders) {
            publications.add(builder.build());
        }

        return publications;
    }
}

package models.publication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class PublicationGenerator {
    public List<Publication> generatePublications(
            int count, Map<String, Double> fieldFreq){

        List<Publication> publications = new ArrayList<>();

        int publicationLine = 0;

        for (int i = 0; i < count; i++) {
            publications.add(new Publication());
        }

        for (var fieldFreqEntry: fieldFreq.entrySet()) {
            String fieldName = fieldFreqEntry.getKey();
            Double freq = fieldFreqEntry.getValue();
            int fieldCount = (int) (count * freq);

            new PublicationsWorker(
                    count, publications, fieldName, publicationLine, fieldCount
            ).run();

            publicationLine += fieldCount;
        }

        return publications;
    }
}

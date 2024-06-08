package models.publication;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PublicationsWorker implements Runnable {
    private static final String[] COMPANIES = {"Google", "Microsoft", "Apple", "Amazon"};
    private static final double PRECISION = 100.0;

    private final Integer publicationCount;
    private final List<Publication> publicationList;
    private Integer publicationLine;
    private final String fieldName;
    private final Integer fieldCount;

    public PublicationsWorker(Integer publicationCount, List<Publication> publicationList,
                              String fieldName, Integer publicationLine, Integer fieldCount) {
        this.publicationCount = publicationCount;
        this.publicationList = publicationList;
        this.fieldName = fieldName;
        this.publicationLine = publicationLine;
        this.fieldCount = fieldCount;
    }

    @Override
    public void run() {
        for (int i = 0; i < fieldCount; i++) {
            Object fieldValue;

            if (fieldName.equals("company")){
                fieldValue = COMPANIES[ThreadLocalRandom.current().nextInt(0, COMPANIES.length)];
            } else if(fieldName.equals("date")){
                fieldValue = getRandomDate();
            }
            else{
                fieldValue = Math.round(ThreadLocalRandom.current().nextDouble() * PRECISION) / PRECISION;
            }

            publicationList.get(publicationLine % publicationCount).addField(
                    new PublicationField(
                            fieldName,
                            fieldValue
                    )
            );
            this.publicationLine++;
        }
    }

    private static Date getRandomDate() {
        int year = ThreadLocalRandom.current().nextInt(2000, 2023) - 1900;
        int month = ThreadLocalRandom.current().nextInt(0, 12);
        int day = ThreadLocalRandom.current().nextInt(1, 32);
        return new Date(year, month, day);
    }
}

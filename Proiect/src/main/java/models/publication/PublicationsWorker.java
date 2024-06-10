package models.publication;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import models.publication.PublicationOuterClass.Publication;
import models.publication.PublicationOuterClass.PublicationField;

public class PublicationsWorker implements Runnable {
    private static final String[] COMPANIES = {"Google", "Microsoft", "Apple", "Amazon"};
    private static final double PRECISION = 100.0;

    private final List<Publication.Builder> publicationBuilders;
    private final String fieldName;
    private final int fieldCount;

    public PublicationsWorker(List<Publication.Builder> publicationBuilders, String fieldName, int fieldCount) {
        this.publicationBuilders = publicationBuilders;
        this.fieldName = fieldName;
        this.fieldCount = fieldCount;
    }

    @Override
    public void run() {
        for (int i = 0; i < fieldCount; i++) {
            Object fieldValue;

            if (fieldName.equals("company")) {
                fieldValue = COMPANIES[ThreadLocalRandom.current().nextInt(0, COMPANIES.length)];
            } else if (fieldName.equals("date")) {
                fieldValue = getRandomDate();
            } else if (fieldName.equals("value")) {
                fieldValue = Math.round(ThreadLocalRandom.current().nextDouble() * PRECISION) / PRECISION;
            } else if (fieldName.equals("drop"))
            {
                fieldValue = Math.round(ThreadLocalRandom.current().nextDouble() * PRECISION) / PRECISION;
            } else
            {
                fieldValue = Math.round(ThreadLocalRandom.current().nextDouble() * PRECISION) / PRECISION;
            }

            PublicationField.Builder fieldBuilder = PublicationField.newBuilder()
                    .setFieldName(fieldName);

            if (fieldName.equals("company")) {
                fieldBuilder.setCompanyField((String) fieldValue);
            } else if (fieldName.equals("date")) {
                fieldBuilder.setDateField((String) fieldValue);
            } else if (fieldName.equals("value"))  {
                fieldBuilder.setValueField((double) fieldValue);
            } else if (fieldName.equals("drop")) {
                fieldBuilder.setDropField((double) fieldValue);
            } else if (fieldName.equals("variation")) {
                fieldBuilder.setVariationField((double) fieldValue);
            }

            publicationBuilders.get(i % publicationBuilders.size()).addFields(fieldBuilder);
        }
    }

    private static String getRandomDate() {
        int year = ThreadLocalRandom.current().nextInt(2000, 2023);
        int month = ThreadLocalRandom.current().nextInt(1, 13);
        int day = ThreadLocalRandom.current().nextInt(1, 32);
        return String.format("%04d-%02d-%02d", year, month, day);
    }
}

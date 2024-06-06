package models;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class Publication {
    private final double MIN_VALUE = 0.0d;
    private final double MAX_VALUE = 100.0d;
    private final double MIN_VARIATION = 0.0d;
    private final double MAX_VARIATION = 5.0d;
    private final double MIN_DROP = 0.0d;
    private final double MAX_DROP = 100.0d;

    private final List<String> companies = Arrays.asList("Facebook", "Amazon", "Netflix", "Google");
    private final String company;
    private final double value;
    private final double drop;
    private final double variation;
    private final Date date;
    public Publication(){
        Random random = new Random();

        int indexCompany = random.nextInt(companies.size());
        this.company = companies.get(indexCompany);

        this.value = Math.round((MIN_VALUE + (MAX_VALUE - MIN_VALUE) * random.nextDouble()) * 100.0) / 100.0;

        this.variation = Math.round((MIN_VARIATION + (MAX_VARIATION - MIN_VARIATION) * random.nextDouble()) * 100.0) / 100.0;

        this.drop = Math.round((MIN_DROP + (MAX_DROP - MIN_DROP) * random.nextDouble()) * 100.0) / 100.0;

        LocalDate start = LocalDate.of(2023, 1, 1);
        LocalDate end = LocalDate.of(2024, 3, 31);
        long startDateEpochDay = start.toEpochDay();
        long endDateEpochDay = end.toEpochDay();
        long randomDateEpochDay = startDateEpochDay + random.nextInt((int) (endDateEpochDay - startDateEpochDay));
        LocalDate randomDate = LocalDate.ofEpochDay(randomDateEpochDay);
        this.date = java.sql.Date.valueOf(randomDate);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{(company,").append(this.company).append(");")
                .append("(value,").append(this.value).append(");")
                .append("(drop,").append(this.drop).append(");")
                .append("(variation,").append(this.variation).append(");")
                .append("(date,").append(new SimpleDateFormat("dd.MM.yyyy").format(this.date)).append(")}\n");
        return sb.toString();
    }
}
package algorithm;

import algorithm.util.Constants;
import models.Publication;
import models.Subscription;
import workers.PublisherGeneratorThread;
import workers.SubscriptionGeneratorThread;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class PubSubAlgorithm implements PubSubAlgorithmContract{
    private int noOfSubs;
    private int noOfPubs;
    private int companyRate;
    private int valueRate;
    private int dropRate;
    private int variationRate;
    private int dateRate;
    private boolean isParallel;
    private List<Subscription> generatedSubscriptions;
    private List<Publication> generatedPublications;
    public PubSubAlgorithm(){
        noOfSubs = 250;
        noOfPubs = 250;
        companyRate = 30;
        valueRate = 20;
        dropRate = 20;
        variationRate = 19;
        dateRate = 11;
        isParallel = false;
        generatedSubscriptions = new ArrayList<>();
        generatedPublications = new ArrayList<>();
    }
    @Override
    public void init() {
        // Path to the input file
        String filePath = "input.txt";

        try {
            // Read input from file
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                String key = parts[0].trim();
                String value = parts[1].trim();
                switch (key) {
                    case Constants.SUBSCRIPTIONS_KEY:
                        noOfSubs = Integer.parseInt(value);
                        break;
                    case Constants.PUBLICATIONS_KEY:
                        noOfPubs = Integer.parseInt(value);
                        break;
                    case Constants.COMPANY_KEY:
                        companyRate = Integer.parseInt(value);
                        break;
                    case Constants.PERCENTAGE_VALUE_KEY:
                        valueRate = Integer.parseInt(value);
                        break;
                    case Constants.DROP_VALUE_KEY:
                        dropRate = Integer.parseInt(value);
                        break;
                    case Constants.VARIATION_VALUE_KEY:
                        variationRate = Integer.parseInt(value);
                        break;
                    case Constants.DATE_VALUE_KEY:
                        dateRate = Integer.parseInt(value);
                        break;
                    case Constants.IS_PARALLEL_KEY:
                        isParallel = value.equals("yes");
                        break;
                    default:
                        System.out.println("Invalid key: " + key);
                }
            }
            reader.close();
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Invalid format in file: " + e.getMessage());
        }
    }
    @Override
    public void generateSubscriptions() {
        if (!isParallel){
            //here the algorithm of generating subscriptions non parallel
            return;
        }

        List<String> metadatas = Arrays.asList("Company", "Value", "Drop", "Variation", "Date");
        List<Integer> threadRates = Arrays.asList(companyRate, valueRate, dropRate, variationRate, dateRate);

        List<SubscriptionGeneratorThread> availableThreads = new ArrayList<>();
        for (int i = 0; i < metadatas.size(); i++) {
            SubscriptionGeneratorThread thread = new SubscriptionGeneratorThread(metadatas.get(i), threadRates.get(i), noOfSubs);
            availableThreads.add(thread);
            thread.start();
        }

        for (SubscriptionGeneratorThread thread : availableThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        int subsCounter = 0;

        for(SubscriptionGeneratorThread thread : availableThreads)
        {
            if(thread.getSubscriptions().size() + subsCounter <= noOfSubs )
            {
                generatedSubscriptions.addAll(thread.getSubscriptions());
                subsCounter += thread.getSubscriptions().size();
                continue;
            }
            for(Subscription sub : thread.getSubscriptions())
            {
                if(subsCounter < noOfSubs)
                {
                    generatedSubscriptions.add(sub);
                    subsCounter++;
                    continue;
                }

                Random random = new Random();
                boolean ok = false;
                while(!ok) {
                    int randomIndex = random.nextInt(generatedSubscriptions.size());
                    Subscription randomSubscription = generatedSubscriptions.get(randomIndex);

                    for (String key : sub.getInfo().keySet()) {
                        if (!randomSubscription.getInfo().containsKey(key)) {
                            randomSubscription.addInfo(key, sub.getInfo().get(key));
                            ok = true;
                        }

                    }
                }
            }
        }

        System.out.println("RESULTS:");

        System.out.println("Number of generated subscriptions: " + generatedSubscriptions.size());

        System.out.println(generatedSubscriptions);

        long companySubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey("Company"))
                .count();

        long valueSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey("Value"))
                .count();

        long dropSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey("Drop"))
                .count();

        long variationSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey("Variation"))
                .count();

        long dateSubscriptionCount = generatedSubscriptions.stream()
                .filter(subscription -> subscription.getInfo().containsKey("Date"))
                .count();

        System.out.println("Number of subscriptions containing 'Company' key: " + companySubscriptionCount);
        System.out.println("Number of subscriptions containing 'Value' key: " + valueSubscriptionCount);
        System.out.println("Number of subscriptions containing 'Drop' key: " + dropSubscriptionCount);
        System.out.println("Number of subscriptions containing 'Variation' key: " + variationSubscriptionCount);
        System.out.println("Number of subscriptions containing 'Date' key: " + dateSubscriptionCount);
    }

    @Override
    public void generatePublications() {
        if (!isParallel){
            for(int i = 0; i < noOfPubs; i++){
                Publication publication = new Publication();
                generatedPublications.add(publication);
            }
            return;
        }

        int limitPubs = 50;
        int countThreads = noOfPubs / limitPubs + 1;
        List<PublisherGeneratorThread> publisherGeneratorThreads = new ArrayList<>();
        for(int i = 0; i < countThreads; i++){
            PublisherGeneratorThread thread = new PublisherGeneratorThread(limitPubs);
            publisherGeneratorThreads.add(thread);
            thread.start();
        }
        for(PublisherGeneratorThread thread : publisherGeneratorThreads){
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (PublisherGeneratorThread thread : publisherGeneratorThreads){
            generatedPublications.addAll(thread.getPublications());
        }
    }
    @Override
    public void createPubAndSubFiles() {
        String directoryPath = "result";

        File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        String pubFilePath = directoryPath + File.separator + "publications.txt";
        String subFilePath = directoryPath + File.separator + "subscriptions.txt";

        writeDataToFile(pubFilePath, generatedPublications);

        writeDataToFile(subFilePath, generatedSubscriptions);

        System.out.println("Both files created successfully.");
    }
    private void writeDataToFile(String filePath, List<?> dataList) {
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            for (Object data : dataList) {
                bw.write(data.toString());
            }
            bw.close();
            fw.close();

            System.out.println("File created successfully: " + filePath);
        } catch (IOException e) {
            System.err.println("Error creating file: " + e.getMessage());
        }
    }


    public void printInputValues(){
        StringBuilder sb = new StringBuilder();
        sb.append("Number of subscriptions: ").append(noOfSubs)
                        .append("\nNumber of publications: ").append(noOfPubs)
                        .append("\nCompany percentage: ").append(companyRate)
                        .append("\nValue percentage: ").append(valueRate)
                        .append("\nDrop percentage: ").append(dropRate)
                        .append("\nVariation percentage: ").append(variationRate)
                        .append("\nDate percentage: ").append(dateRate)
                        .append("\nIs Parallel: ").append(isParallel);
        System.out.println(sb);
    }

}

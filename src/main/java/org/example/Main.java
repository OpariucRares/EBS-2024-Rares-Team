package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Main {

    private static int noOfSubs;
    private static int companyRate;
    private static int valueRate;
    private static int dropRate;
    private static int variationRate;
    private static int dateRate;


    public static void getInputData() {
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
                    case "number of subscriptions":
                        noOfSubs = Integer.parseInt(value);
                        break;
                    case "company percentage":
                        companyRate = Integer.parseInt(value);
                        break;
                    case "value percentage":
                        valueRate = Integer.parseInt(value);
                        break;
                    case "drop percentage":
                        dropRate = Integer.parseInt(value);
                        break;
                    case "variation percentage":
                        variationRate = Integer.parseInt(value);
                        break;
                    case "date percentage":
                        dateRate = Integer.parseInt(value);
                        break;
                    default:
                        System.out.println("Invalid key: " + key);
                }
            }
            reader.close();

            System.out.println("Number of subscriptions: " + noOfSubs);
            System.out.println("Company percentage: " + companyRate);
            System.out.println("Value percentage: " + valueRate);
            System.out.println("Drop percentage: " + dropRate);
            System.out.println("Variation percentage: " + variationRate);
            System.out.println("Date percentage: " + dateRate);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        } catch (NumberFormatException e) {
            System.err.println("Invalid format in file: " + e.getMessage());
        }
    }

    public static void main(String[] args) {

        getInputData();

        ArrayList<Subscription> generatedSubscriptions = new ArrayList();

        SubscriptionGeneratorThread companyThread = new SubscriptionGeneratorThread("Company", companyRate, noOfSubs);
        SubscriptionGeneratorThread valueThread = new SubscriptionGeneratorThread("Value", valueRate, noOfSubs);
        SubscriptionGeneratorThread dropThread = new SubscriptionGeneratorThread("Drop", dropRate, noOfSubs);
        SubscriptionGeneratorThread variationThread = new SubscriptionGeneratorThread("Variation", variationRate, noOfSubs);
        SubscriptionGeneratorThread dateThread = new SubscriptionGeneratorThread("Date", dateRate, noOfSubs);

        ArrayList<SubscriptionGeneratorThread> availableThreads = new ArrayList();

        availableThreads.add(companyThread);
        availableThreads.add(valueThread);
        availableThreads.add(dropThread);
        availableThreads.add(variationThread);
        availableThreads.add(dateThread);

        companyThread.start();
        valueThread.start();
        dropThread.start();
        variationThread.start();
        dateThread.start();

        try {
            companyThread.join();
            valueThread.join();
            dropThread.join();
            variationThread.join();
            dateThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int subsCounter = 0;

        for(SubscriptionGeneratorThread thread : availableThreads)
        {
            if(thread.getSubscriptions().size() + subsCounter <= noOfSubs )
            {
                generatedSubscriptions.addAll(thread.getSubscriptions());
                subsCounter += thread.getSubscriptions().size();
            }
            else
            {
                for(Subscription sub : thread.getSubscriptions())
                {
                    if(subsCounter < noOfSubs)
                    {
                        generatedSubscriptions.add(sub);
                        subsCounter++;
                    }

                    else {
                        Random random = new Random();
                        boolean ok = false;
                        while(!ok)
                        {
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
}
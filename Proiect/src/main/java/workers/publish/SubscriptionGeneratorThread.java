//package workers.publish;
//
//import util.Constants;
//import models.SubscriptionDEPRECATED;
//
//import java.time.LocalDate;
//import java.util.*;
//
//
//public class SubscriptionGeneratorThread extends Thread {
//    private int rate;
//    private String metaData;
//    private int noOfSubs;
//    private List<SubscriptionDEPRECATED> subscriptionDEPRECATEDS;
//
//    private final List<String> companyOperators = List.of("=", "!=");
//
//    private final List<String> otherOperators = List.of("<", "<=", "=", ">", ">=");
//
//    private final double MIN_VALUE = 0.0d;
//    private final double MAX_VALUE = 100.0d;
//    private final double MIN_VARIATION = 0.0d;
//    private final double MAX_VARIATION = 5.0d;
//    private final double MIN_DROP = 0.0d;
//    private final double MAX_DROP = 100.0d;
//
//
//    public SubscriptionGeneratorThread(String metaData, int rate, int noOfSubs) {
//        this.metaData = metaData;
//        this.rate = rate;
//        this.noOfSubs = noOfSubs;
//        this.subscriptionDEPRECATEDS = new ArrayList<>();
//    }
//
//    @Override
//    public void run() {
//        List<String> metadatas = Constants.metadataKeys;
//
//        int actualItems = (rate*noOfSubs)/100;
//        Random random = new Random();
//
//        for (int i = 0; i < actualItems; i++) {
//            SubscriptionDEPRECATED subscriptionDEPRECATED = new SubscriptionDEPRECATED();
//            if(Objects.equals(metaData, metadatas.get(Constants.getInstance().COMPANY_INDEX)))
//            {
//                final List<String> companies = Arrays.asList("Facebook", "Amazon", "Netflix", "Google");
//
//                int indexCompany = random.nextInt(companies.size());
//                int localOperator;
//                if (random.nextInt(100) > 10) {
//                    localOperator = random.nextInt(companyOperators.size());
//                }
//                else
//                {
//                    localOperator = 0;
//                }
//
//                subscriptionDEPRECATED.addOperator(companyOperators.get(localOperator));
//                subscriptionDEPRECATED.addInfo(metaData, companies.get(indexCompany));
//                subscriptionDEPRECATEDS.add(subscriptionDEPRECATED);
//                continue;
//            }
//            if(Objects.equals(metaData, metadatas.get(Constants.getInstance().DATE_INDEX)))
//            {
//                LocalDate start = LocalDate.of(2023, 1, 1);
//                LocalDate end = LocalDate.of(2024, 3, 31);
//                long startDateEpochDay = start.toEpochDay();
//                long endDateEpochDay = end.toEpochDay();
//                long randomDateEpochDay = startDateEpochDay + random.nextInt((int) (endDateEpochDay - startDateEpochDay));
//                LocalDate randomDate = LocalDate.ofEpochDay(randomDateEpochDay);
//
//                int localOperator = random.nextInt(otherOperators.size());
//
//                subscriptionDEPRECATED.addOperator(otherOperators.get(localOperator));
//                subscriptionDEPRECATED.addInfo(metaData, java.sql.Date.valueOf(randomDate).toString());
//                subscriptionDEPRECATEDS.add(subscriptionDEPRECATED);
//                continue;
//            }
//            double dummyResult = 0.0d;
//            if(Objects.equals(metaData, metadatas.get(Constants.getInstance().VARIATION_INDEX))){
//                dummyResult = Math.round((MIN_VARIATION + (MAX_VARIATION - MIN_VARIATION) * random.nextDouble()) * 100.0) / 100.0;
//            }
//            if(Objects.equals(metaData, metadatas.get(Constants.getInstance().VALUE_INDEX))){
//                dummyResult = Math.round((MIN_VALUE + (MAX_VALUE - MIN_VALUE) * random.nextDouble()) * 100.0) / 100.0;
//            }
//            if(Objects.equals(metaData, metadatas.get(Constants.getInstance().DROP_INDEX))){
//                dummyResult = Math.round((MIN_DROP + (MAX_DROP - MIN_DROP) * random.nextDouble()) * 100.0) / 100.0;
//            }
//
//            int localOperator = random.nextInt(otherOperators.size());
//
//            subscriptionDEPRECATED.addOperator(otherOperators.get(localOperator));
//            subscriptionDEPRECATED.addInfo(metaData, String.valueOf(dummyResult));
//            subscriptionDEPRECATEDS.add(subscriptionDEPRECATED);
//        }
//    }
//
//    public List<SubscriptionDEPRECATED> getSubscriptions() {
//        return subscriptionDEPRECATEDS;
//    }
//}
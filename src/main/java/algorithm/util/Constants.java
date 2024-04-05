package algorithm.util;

public class Constants {
    private static Constants instance;
    public static final String SUBSCRIPTIONS_KEY = "number of subscriptions";
    public static final String PUBLICATIONS_KEY = "number of publications";
    public static final String COMPANY_KEY = "company percentage";
    public static final String PERCENTAGE_VALUE_KEY = "value percentage";
    public static final String DROP_VALUE_KEY = "drop percentage";
    public static final String VARIATION_VALUE_KEY = "variation percentage";
    public static final String DATE_VALUE_KEY = "date percentage";
    public static final String IS_PARALLEL_KEY = "is parallel";
    private Constants(){}
    public static Constants getInstance() {
        if (instance == null) {
            instance = new Constants();
        }
        return instance;
    }
}

package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public static final List<String> metadataKeys = new ArrayList<>(Arrays.asList("Company", "Value", "Drop", "Variation", "Date"));

    public final int COMPANY_INDEX = 0;
    public final int VALUE_INDEX = 1;
    public final int DROP_INDEX = 2;
    public final int VARIATION_INDEX = 3;
    public final int DATE_INDEX = 4;
    public static final String MINIMUM_COMPANY = "minimum company";
    private Constants(){}
    public static Constants getInstance() {
        if (instance == null) {
            instance = new Constants();
        }
        return instance;
    }
}
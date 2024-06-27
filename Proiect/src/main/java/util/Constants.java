package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Constants {
    public Constants(){}

    public final Map<String, Double> fieldFreq = Map.of(
            "company", 0.80,
            "value", 0.80,
            "drop", 0.80,
            "variation", 0.80,
            "date", 0.80
    );

//    public final Map<String, Double> eqFreq = Map.of(
//            "company", 0.25
//    );

    public final Map<String, Double> eqFreq = Map.of(
            "company", 1.0
    );

    public final Map<String, Double> pubFieldFreq = Map.of(
            "company", 1.0,
            "value", 1.0,
            "drop", 1.0,
            "variation", 1.0,
            "date", 1.0
    );
}
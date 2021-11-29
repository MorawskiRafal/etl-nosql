package tbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.format.DateTimeFormatter;


public class TypeMapper {
    public static Object map(String type, String value) {
        type = type.trim().toLowerCase();

        if (value.equals(""))
            return null;

        switch (type) {
            case "int":
            case "integer":
                return Integer.parseInt(value);
            case "bigint":
            case "long":
                return Long.parseLong(value);
            case "string":
            case "varchar":
            case "ascii":
                return value;
            case "decimal":
                return BigDecimal.valueOf(Double.parseDouble(value));
            case "date":
                java.time.LocalDate jld = java.time.LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                return com.datastax.driver.core.LocalDate.fromYearMonthDay(
                        jld.getYear(), jld.getMonthValue(), jld.getDayOfMonth());
        }

        throw new RuntimeException("Unable to parse for type:" + type + " and value " + value);
    }
}


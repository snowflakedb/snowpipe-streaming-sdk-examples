package com.snowflake.example.monitoring;

import com.github.javafaker.Faker;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Generates fake user data rows using Java Faker.
 * Produces the same schema as the Python monitor_abort_example.
 */
public class FakeDataGenerator {
    private final Faker faker = new Faker();

    public Map<String, Object> makeRow(int userId) {
        Map<String, Object> row = new HashMap<>();
        row.put("user_id", userId);
        row.put("first_name", faker.name().firstName());
        row.put("last_name", faker.name().lastName());
        row.put("email", faker.internet().emailAddress());
        row.put("phone_number", faker.phoneNumber().phoneNumber());
        row.put("address", faker.address().fullAddress());
        row.put("date_of_birth", randomDateOfBirth().toString());
        row.put("registration_date", faker.date().past(90, TimeUnit.DAYS)
                .toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().toString());
        row.put("city", faker.address().city());
        row.put("state", faker.address().state());
        row.put("country", faker.address().country());
        return row;
    }

    private LocalDate randomDateOfBirth() {
        java.util.Date dob = faker.date().birthday(18, 80);
        return dob.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }
}

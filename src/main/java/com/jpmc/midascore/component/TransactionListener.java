package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    private static final String INCENTIVE_URL =
            "http://localhost:8080/incentive";

    public TransactionListener(DatabaseConduit databaseConduit,
                               RestTemplate restTemplate) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {

        UserRecord sender =
                databaseConduit.findUser(transaction.getSenderId());

        UserRecord recipient =
                databaseConduit.findUser(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        Incentive incentive = restTemplate.postForObject(
                INCENTIVE_URL,
                transaction,
                Incentive.class
        );

        float incentiveAmount = 0F;

        if (incentive != null) {
            incentiveAmount = incentive.getAmount();
        }

        sender.setBalance(
                sender.getBalance() - transaction.getAmount());

        recipient.setBalance(
                recipient.getBalance()
                        + transaction.getAmount()
                        + incentiveAmount);

        databaseConduit.save(sender);
        databaseConduit.save(recipient);

        TransactionRecord record =
                new TransactionRecord(
                        sender,
                        recipient,
                        transaction.getAmount()
                );

        record.setIncentive(incentiveAmount);

        databaseConduit.saveTransaction(record);

        System.out.println("Processing: " + transaction);
    }
}